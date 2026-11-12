"""领域模型：封包清单、到达信封、规范摘要与接收状态。

身份规则（需求核心）：
- 交付以「航次、站位、包代际、清单摘要」标识，文件名只是显示信息；
- 只有带封包标记的清单才确定成员集合；
- 封包前清单可修改，封包后再改必须产生新代际。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum

from ..domain import member_path

_IDENTIFIER = re.compile(r'^[A-Za-z0-9_.\-]{1,64}$')
_HEX64 = re.compile(r'^[0-9a-f]{64}$')


def _require_identifier(value, field_name):
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f'{field_name} 不是合法标识: {value!r}')
    return value


class PackageState(str, Enum):
    """接收服务必须能分辨的四种状态。"""

    AWAITING_SEAL = 'AWAITING_SEAL'          # 清单收齐但尚未封包
    SEALED_INCOMPLETE = 'SEALED_INCOMPLETE'  # 封包但缺成员
    READY = 'READY'                          # 完整待发布
    PUBLISHED = 'PUBLISHED'                  # 正式可用（终态，整包边界）

    @property
    def 中文说明(self):
        return {
            PackageState.AWAITING_SEAL: '清单未封包',
            PackageState.SEALED_INCOMPLETE: '已封包但缺成员',
            PackageState.READY: '完整待发布',
            PackageState.PUBLISHED: '已正式发布',
        }[self]


class ArrivalKind(str, Enum):
    MANIFEST = 'manifest'
    MEMBER = 'member'
    SEAL = 'seal'


class ArrivalStatus(str, Enum):
    ACCEPTED = 'ACCEPTED'        # 已确认入库（来源已可安全清理）
    DUPLICATE = 'DUPLICATE'      # 重复到件，幂等忽略
    PENDING = 'PENDING'          # 已登记，等待对应清单/封包后再判定
    QUARANTINED = 'QUARANTINED'  # 隔离：大小、摘要或归属不符


@dataclass(frozen=True)
class MemberSpec:
    """封包清单中的单个成员凭据。"""

    name: str
    size: int
    digest: str

    def to_dict(self):
        return {'name': self.name, 'size': self.size, 'digest': self.digest}


@dataclass(frozen=True)
class Manifest:
    """一份清单（未封包或已封包）。"""

    voyage: str
    station: str
    generation: int
    sealed: bool
    members: tuple[MemberSpec, ...]
    digest: str = field(compare=False)

    @property
    def package_key(self):
        return f'{self.voyage}/{self.station}/g{self.generation}'

    def member(self, name):
        for spec in self.members:
            if spec.name == name:
                return spec
        return None

    def to_dict(self):
        return {
            'voyage': self.voyage,
            'station': self.station,
            'generation': self.generation,
            'sealed': self.sealed,
            'members': [spec.to_dict() for spec in self.members],
            'manifest_digest': self.digest,
        }


def canonical_bytes(voyage, station, generation, sealed, members):
    """清单正文的规范序列化字节：键排序、无空白，摘要据此稳定计算。"""

    body = {
        'voyage': voyage,
        'station': station,
        'generation': generation,
        'sealed': sealed,
        'members': [
            {'name': spec.name, 'size': spec.size, 'digest': spec.digest}
            if isinstance(spec, MemberSpec)
            else {'name': spec['name'], 'size': spec['size'], 'digest': spec['digest']}
            for spec in members
        ],
    }
    return json.dumps(body, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(',', ':')).encode('utf-8')


def manifest_digest(voyage, station, generation, sealed, members):
    import hashlib

    return hashlib.sha256(
        canonical_bytes(voyage, station, generation, sealed, members)
    ).hexdigest()


def parse_manifest(raw):
    """解析并严格校验一个清单 JSON 对象。"""

    if not isinstance(raw, dict):
        raise ValueError('清单必须是 JSON 对象')
    voyage = _require_identifier(raw.get('voyage'), 'voyage')
    station = _require_identifier(raw.get('station'), 'station')
    generation = raw.get('generation')
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise ValueError('generation 必须是不小于 1 的整数')
    sealed = raw.get('sealed')
    if not isinstance(sealed, bool):
        raise ValueError('sealed 必须是布尔值')
    raw_members = raw.get('members')
    if not isinstance(raw_members, list) or not raw_members:
        raise ValueError('清单成员必须是非空列表')

    members = []
    seen = set()
    for item in raw_members:
        if not isinstance(item, dict):
            raise ValueError('清单成员项必须是对象')
        name = member_path(str(item['name'])) if isinstance(item.get('name'), str) else None
        if name is None:
            raise ValueError('成员缺少合法名称')
        if name in seen:
            raise ValueError(f'清单成员重复: {name}')
        seen.add(name)
        size = item.get('size')
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError(f'成员 {name} 的 size 非法')
        digest = item.get('digest')
        if not isinstance(digest, str) or not _HEX64.fullmatch(digest):
            raise ValueError(f'成员 {name} 的 digest 必须是 64 位小写十六进制 sha256')
        members.append(MemberSpec(name=name, size=size, digest=digest))

    digest = manifest_digest(voyage, station, generation, sealed, members)
    return Manifest(
        voyage=voyage,
        station=station,
        generation=generation,
        sealed=sealed,
        members=tuple(members),
        digest=digest,
    )


@dataclass(frozen=True)
class Envelope:
    """接收目录中的一个到件信封（attribution 归属信息）。"""

    arrival_id: str
    kind: ArrivalKind
    voyage: str | None
    station: str | None
    generation: int | None
    manifest: Manifest | None
    member_name: str | None

    def package_key(self):
        if self.generation is None or self.voyage is None or self.station is None:
            return None
        return f'{self.voyage}/{self.station}/g{self.generation}'


def parse_envelope(raw):
    """解析到件信封并做归属校验。

    信封示例见 synthetic.write_arrival。member 到件的信封必须声明航次/站位/
    代际，且若携带清单头则必须与声明一致——这是「归属不符即隔离」的依据。
    """

    if not isinstance(raw, dict):
        raise ValueError('信封必须是 JSON 对象')
    arrival_id = raw.get('arrival_id')
    if not isinstance(arrival_id, str) or not _IDENTIFIER.fullmatch(arrival_id):
        raise ValueError('arrival_id 非法')
    kind_value = raw.get('kind')
    try:
        kind = ArrivalKind(kind_value)
    except ValueError as exc:
        raise ValueError(f'未知到件类型: {kind_value!r}') from exc

    voyage = raw.get('voyage')
    station = raw.get('station')
    generation = raw.get('generation')
    if voyage is not None:
        _require_identifier(voyage, 'voyage')
    if station is not None:
        _require_identifier(station, 'station')
    if generation is not None and (
        not isinstance(generation, int) or isinstance(generation, bool) or generation < 1
    ):
        raise ValueError('generation 必须是不小于 1 的整数')

    manifest_raw = raw.get('manifest')
    manifest = parse_manifest(manifest_raw) if manifest_raw is not None else None
    if manifest is not None:
        if voyage is not None and voyage != manifest.voyage:
            raise ValueError('信封与清单的 voyage 归属不符')
        if station is not None and station != manifest.station:
            raise ValueError('信封与清单的 station 归属不符')
        if generation is not None and generation != manifest.generation:
            raise ValueError('信封与清单的 generation 归属不符')
        voyage, station, generation = manifest.voyage, manifest.station, manifest.generation

    member_name = None
    if kind is ArrivalKind.MEMBER:
        if voyage is None or station is None or generation is None:
            raise ValueError('member 到件必须声明完整归属（voyage/station/generation）')
        raw_name = raw.get('member_name')
        if not isinstance(raw_name, str):
            raise ValueError('member 到件缺少 member_name')
        member_name = member_path(raw_name)
    elif kind is ArrivalKind.SEAL:
        if voyage is None or station is None or generation is None:
            raise ValueError('seal 到件必须声明完整归属')
    elif kind is ArrivalKind.MANIFEST and manifest is None:
        raise ValueError('manifest 到件必须携带清单正文')

    return Envelope(
        arrival_id=arrival_id,
        kind=kind,
        voyage=voyage,
        station=station,
        generation=generation,
        manifest=manifest,
        member_name=member_name,
    )
