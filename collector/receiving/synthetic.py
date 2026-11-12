"""合成小文件与到件信封的构造工具（演示与测试专用，非生产代码路径）。

接收服务面对的是「别人拷来的目录」，本模块负责在接收目录里造出那些目录：
每个到件是 ``inbox/<arrival_id>/`` 下的一个信封 ``envelope.json``，成员到件
另外带 ``payload.bin``。信封里的归属字段是隔离判定的依据。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .fsstore import ENVELOPE_NAME, PAYLOAD_NAME
from .model import manifest_digest


def make_member(name: str, content: bytes):
    """返回 (size, sha256) 成员凭据。"""

    return len(content), hashlib.sha256(content).hexdigest()


def member_specs(files: dict[str, bytes]):
    return [
        {'name': name, 'size': len(content),
         'digest': hashlib.sha256(content).hexdigest()}
        for name, content in sorted(files.items())
    ]


def write_arrival(inbox: Path, arrival_id: str, envelope: dict,
                  payload: bytes | None = None) -> Path:
    """在接收目录写入一个到件单元，返回该单元目录。"""

    entry = Path(inbox) / arrival_id
    entry.mkdir(parents=True, exist_ok=False)
    (entry / ENVELOPE_NAME).write_text(
        json.dumps(envelope, ensure_ascii=False, indent=2, sort_keys=True),
        encoding='utf-8',
    )
    if payload is not None:
        (entry / PAYLOAD_NAME).write_bytes(payload)
    return entry


def manifest_envelope(arrival_id, voyage, station, generation, sealed,
                      members: list[dict]) -> dict:
    digest = manifest_digest(voyage, station, generation, sealed, members)
    return {
        'arrival_id': arrival_id,
        'kind': 'manifest',
        'voyage': voyage,
        'station': station,
        'generation': generation,
        'manifest': {
            'voyage': voyage,
            'station': station,
            'generation': generation,
            'sealed': sealed,
            'members': members,
        },
    }, digest


def member_envelope(arrival_id, voyage, station, generation, member_name) -> dict:
    return {
        'arrival_id': arrival_id,
        'kind': 'member',
        'voyage': voyage,
        'station': station,
        'generation': generation,
        'member_name': member_name,
    }


def seal_envelope(arrival_id, voyage, station, generation) -> dict:
    return {
        'arrival_id': arrival_id,
        'kind': 'seal',
        'voyage': voyage,
        'station': station,
        'generation': generation,
    }


def synthesize_files(seed: str, names: list[str]) -> dict[str, bytes]:
    """按固定规则生成可复现的合成小文件内容。"""

    files = {}
    for index, name in enumerate(names):
        files[name] = f'{seed}|{name}|第{index + 1}行\n'.encode('utf-8')
    return files
