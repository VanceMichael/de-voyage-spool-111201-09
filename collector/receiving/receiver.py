"""接收服务：接收目录扫描与正式目录晋升的可恢复流程。

并发与恢复要点：
- 扫描全程持有全局 fcntl 排它锁，两个接收进程串行化，不会各搬一次；
- 到件先经一次原子 ``rename`` 从 ``inbox/`` 认领到 ``inbox/.processing/``，
  认领即归属，另一个进程的扫描再也看不到它；
- 成员字节先 fsync 落进内容寻址仓库并拿到确认，台账事务提交之后才删除
  来源目录——清理只发生在可证明的接收确认之后；
- 任何一步崩溃后重跑：processing 中未入账的到件被重新处理，已入账的
  幂等跳过，隔离区与对象仓库都按摘要去重。
"""

from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path

from .fsstore import (
    ENVELOPE_NAME,
    PAYLOAD_NAME,
    DigestMismatch,
    ObjectStore,
    ensure_layout,
    fsync_directory,
    quarantine_entry,
)
from .ledger import Ledger
from .model import (
    ArrivalKind,
    ArrivalStatus,
    Manifest,
    MemberSpec,
    PackageState,
    parse_envelope,
)


class Receiver:
    def __init__(self, root: Path):
        self.root = ensure_layout(Path(root))
        self.inbox = self.root / 'inbox'
        self.processing = self.inbox / '.processing'
        self.ledger = Ledger(self.root / 'ledger.sqlite3')
        self.objects = ObjectStore(self.root)
        self.lock_file = self.root / '.locks' / 'receiver.lock'

    def close(self):
        self.ledger.close()

    @contextmanager
    def _exclusive_lock(self):
        with self.lock_file.open('a+') as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    # ------------------------------------------------------------------ 扫描

    def scan_once(self):
        """执行一轮扫描，返回本轮处理动作的结构化报告。

        报告按到件在本轮结束时的最终状态归类（同一轮内成员先于清单暂存、
        随后被封包追认的情况，最终计入 accepted 而非 pending）。
        """

        touched = []
        report = {'accepted': [], 'duplicate': [], 'pending': [], 'quarantined': []}
        with self._exclusive_lock():
            claimed = self._claim_entries()
            for entry_dir, loose_file in claimed:
                try:
                    self._process_entry(entry_dir, loose_file, report)
                    touched.append(entry_dir.name)
                except Exception:  # 基础设施错误：保留现场，下轮重试
                    report.setdefault('errors', []).append(
                        {'entry': entry_dir.name, 'dir': str(entry_dir)}
                    )
                    raise
        self._reclassify_report(touched, report)
        return report

    def _reclassify_report(self, touched_names, report):
        """处理顺序可能让某到件在一轮内经历暂存→追认，按台账最终状态归桶。"""

        if not touched_names:
            return
        placeholders = ','.join('?' for _ in touched_names)
        rows = self.ledger.connection.execute(
            f'SELECT arrival_id, package_key, kind, member_name, status, detail '
            f'FROM arrivals WHERE arrival_id IN ({placeholders})',
            touched_names,
        ).fetchall()
        final = {row['arrival_id']: row for row in rows}
        duplicates = {
            row['arrival_id']: row
            for row in self.ledger.connection.execute(
                f'SELECT arrival_id, package_key, detail FROM duplicate_arrivals '
                f'WHERE arrival_id IN ({placeholders})', touched_names,
            ).fetchall()
        }
        for bucket in ('accepted', 'duplicate', 'pending', 'quarantined'):
            report[bucket] = []
        for name in touched_names:
            if name in duplicates:
                dup = duplicates[name]
                report['duplicate'].append(
                    {'arrival_id': name, 'package': dup['package_key'],
                     'reason': dup['detail']})
                continue
            row = final.get(name)
            if row is None:
                continue
            item = {'arrival_id': name, 'package': row['package_key']}
            if row['member_name']:
                item['member'] = row['member_name']
            if row['detail']:
                item['reason'] = row['detail']
            bucket = {
                ArrivalStatus.ACCEPTED.value: 'accepted',
                ArrivalStatus.DUPLICATE.value: 'duplicate',
                ArrivalStatus.PENDING.value: 'pending',
                ArrivalStatus.QUARANTINED.value: 'quarantined',
            }.get(row['status'])
            if bucket:
                report[bucket].append(item)
        # 隔离区中由本轮产生、但 arrivals 表无对应行的条目仍需可见
        for q in self.ledger.list_quarantine():
            if q['arrival_id'] in touched_names and not any(
                    item['arrival_id'] == q['arrival_id']
                    for item in report['quarantined']):
                report['quarantined'].append(
                    {'arrival_id': q['arrival_id'],
                     'package': q['package_key'], 'reason': q['reason']})

    def _claim_entries(self):
        """把 inbox 顶层所有到件原子移入 processing，返回 (目录, 松散文件名)。"""

        claimed = []
        claimed_names = set()
        for child in sorted(self.inbox.iterdir(), key=lambda p: p.name):
            if child.name == '.processing':
                continue
            target = self.processing / child.name
            if target.exists():
                # 同名冲突（极少见）：追加 pid 后缀，绝不合并/覆盖
                target = self.processing / f'{child.name}.{os.getpid()}'
            os.rename(child, target)
            if target.is_file():
                # 松散文件没有信封：包一层目录并以原文件名作为到件标识
                loose_name = target.name
                wrapped = self.processing / loose_name
                if wrapped.exists():
                    wrapped = self.processing / f'{loose_name}.{os.getpid()}'
                wrapped.mkdir()
                os.rename(target, wrapped / loose_name)
                target = wrapped
                claimed.append((wrapped, loose_name))
            else:
                claimed.append((target, None))
            claimed_names.add(target.name)
        # 崩溃恢复：上一轮遗留在 processing 的到件本轮继续处理
        for child in sorted(self.processing.iterdir(), key=lambda p: p.name):
            if child.is_dir() and child.name not in claimed_names:
                claimed.append((child, None))
                claimed_names.add(child.name)
        return claimed

    # ------------------------------------------------------------- 单到件处理

    def _process_entry(self, entry_dir: Path, loose_file, report):
        envelope_path = entry_dir / ENVELOPE_NAME

        if loose_file is not None or not envelope_path.is_file():
            reason = '接收目录中的松散文件或缺少信封，无法确认归属'
            path = quarantine_entry(self.root, entry_dir.name, entry_dir, reason)
            with self.ledger.transaction():
                self.ledger.record_arrival(
                    arrival_id=entry_dir.name, package_key=None,
                    kind=ArrivalKind.MEMBER, status=ArrivalStatus.QUARANTINED,
                    quarantine_path=str(path), detail=reason,
                )
                self.ledger.record_quarantine(
                    arrival_id=entry_dir.name, package_key=None,
                    kind=ArrivalKind.MEMBER, reason=reason, path=str(path),
                )
            report['quarantined'].append({'arrival_id': entry_dir.name, 'reason': reason})
            return

        raw_envelope = json.loads(envelope_path.read_text(encoding='utf-8'))
        try:
            envelope = parse_envelope(raw_envelope)
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            reason = f'信封校验失败: {exc}'
            path = quarantine_entry(self.root, entry_dir.name, entry_dir, reason)
            with self.ledger.transaction():
                self.ledger.record_arrival(
                    arrival_id=entry_dir.name, package_key=None,
                    kind=ArrivalKind.MEMBER, status=ArrivalStatus.QUARANTINED,
                    quarantine_path=str(path), detail=reason,
                )
                self.ledger.record_quarantine(
                    arrival_id=entry_dir.name, package_key=None,
                    kind=ArrivalKind.MEMBER, reason=reason, path=str(path),
                )
            report['quarantined'].append({'arrival_id': entry_dir.name, 'reason': reason})
            return

        if envelope.kind is ArrivalKind.MANIFEST:
            # 相同摘要清单重发走清单内部的幂等逻辑，不能在入口直接丢弃
            self._handle_manifest(envelope, entry_dir, report)
        elif envelope.kind is ArrivalKind.SEAL:
            self._handle_seal(envelope, entry_dir, report)
        else:
            known = self.ledger.get_arrival(envelope.arrival_id)
            if known is not None:
                # 成员重发：到件身份已确认过，登记一条 DUPLICATE 凭据后清理来源
                with self.ledger.transaction():
                    self.ledger.record_duplicate(
                        arrival_id=envelope.arrival_id,
                        package_key=envelope.package_key(),
                        kind=ArrivalKind.MEMBER,
                        detail='到件身份已登记，重复投递幂等忽略',
                    )
                self._remove_source(entry_dir)
                report['duplicate'].append(
                    {'arrival_id': envelope.arrival_id,
                     'reason': '到件身份已登记，重复投递幂等忽略'})
                return
            self._handle_member(envelope, entry_dir, report)

    # ------------------------------------------------------------- 清单到件

    def _handle_manifest(self, envelope, entry_dir, report):
        manifest: Manifest = envelope.manifest
        package_key = manifest.package_key
        package = self.ledger.get_package(package_key)
        sealed_after_seal = (
            package is not None and package['sealed_manifest_digest'] is not None
            and package['sealed_manifest_digest'] != manifest.digest
        )
        if sealed_after_seal:
            reason = (
                f'包 {package_key} 已封包（{package["sealed_manifest_digest"][:12]}…），'
                f'封包后修改必须产生新代际，不能改写代际 {manifest.generation}'
            )
            self._quarantine(envelope, entry_dir, reason, report)
            return

        # 同摘要清单重发：幂等确认
        if self.ledger.get_manifest(manifest.digest) is not None:
            with self.ledger.transaction():
                self.ledger.record_arrival(
                    arrival_id=envelope.arrival_id, package_key=package_key,
                    kind=ArrivalKind.MANIFEST, status=ArrivalStatus.DUPLICATE,
                    manifest_digest=manifest.digest, detail='相同清单摘要重复到件',
                )
            self._remove_source(entry_dir)
            report['duplicate'].append({'arrival_id': envelope.arrival_id})
            return

        self.ledger.ensure_package(manifest)
        with self.ledger.transaction():
            self.ledger._insert_manifest_locked(manifest, envelope.arrival_id)
            self.ledger.record_arrival(
                arrival_id=envelope.arrival_id, package_key=package_key,
                kind=ArrivalKind.MANIFEST, status=ArrivalStatus.ACCEPTED,
                manifest_digest=manifest.digest,
            )
            if manifest.sealed:
                # 成员可能在同轮更早（或此前）已暂存，封包清单到达即追认/隔离
                self._reconcile_pending_locked(package_key, manifest)
            else:
                self._adopt_pending_locked(package_key)
            # 追认可能改变成员状态，必须在同一事务内再重算一次包状态
            self.ledger._recompute_locked(package_key)
        self._remove_source(entry_dir)
        report['accepted'].append(
            {'arrival_id': envelope.arrival_id, 'package': package_key,
             'manifest_digest': manifest.digest, 'sealed': manifest.sealed}
        )

    def _reconcile_pending_locked(self, package_key, sealed_manifest: Manifest):
        """封包瞬间把之前暂存的成员按确定成员集合重新判定。"""

        for pending in self.ledger.list_pending_members(package_key):
            spec = sealed_manifest.member(pending['member_name'])
            if spec is not None and pending['object_digest'] == spec.digest \
                    and pending['object_size'] == spec.size:
                self.ledger.update_arrival(
                    pending['arrival_id'], status=ArrivalStatus.ACCEPTED,
                )
            else:
                reason = (
                    f'成员 {pending["member_name"]} 不属于封包清单或凭据不符，'
                    '封包后判定为隔离'
                )
                self.ledger.update_arrival(
                    pending['arrival_id'], status=ArrivalStatus.QUARANTINED,
                    detail=reason,
                )
                self.ledger.record_quarantine(
                    arrival_id=pending['arrival_id'], package_key=package_key,
                    kind=ArrivalKind.MEMBER, reason=reason,
                )

    def _adopt_pending_locked(self, package_key):
        """未封包草稿到达：先到的暂存成员继续保持 PENDING，但刷新归属关联。

        封包前成员集合尚未确定，不能做凭据判定；这里只保证它们挂在正确的包上，
        状态重算仍给出 AWAITING_SEAL。
        """

        self.ledger.connection.execute(
            'UPDATE arrivals SET package_key=? WHERE package_key IS NULL '
            "AND kind='member' AND status=?",
            (package_key, ArrivalStatus.PENDING.value),
        )
        self.ledger._recompute_locked(package_key)

    # ------------------------------------------------------------- 封包到件

    def _handle_seal(self, envelope, entry_dir, report):
        package_key = envelope.package_key()
        package = self.ledger.get_package(package_key)
        if package is None or package['sealed_manifest_digest'] is not None:
            reason = (
                f'seal 到件无效：包 {package_key} 不存在或已封包'
                if package is None else f'包 {package_key} 已封包，重复 seal 忽略'
            )
            status = ArrivalStatus.QUARANTINED if package is None else ArrivalStatus.DUPLICATE
            with self.ledger.transaction():
                self.ledger.record_arrival(
                    arrival_id=envelope.arrival_id, package_key=package_key,
                    kind=ArrivalKind.SEAL, status=status, detail=reason,
                )
                if package is None:
                    self.ledger.record_quarantine(
                        arrival_id=envelope.arrival_id, package_key=package_key,
                        kind=ArrivalKind.SEAL, reason=reason,
                    )
            self._remove_source(entry_dir)
            (report['quarantined'] if package is None else report['duplicate']).append(
                {'arrival_id': envelope.arrival_id, 'reason': reason}
            )
            return

        # 封包：冻结该包当前最新一版草稿清单的成员集合
        drafts = [row for row in self.ledger.list_manifests(package_key)
                  if not row['sealed']]
        if not drafts:
            reason = f'包 {package_key} 没有可封包的草稿清单'
            with self.ledger.transaction():
                self.ledger.record_arrival(
                    arrival_id=envelope.arrival_id, package_key=package_key,
                    kind=ArrivalKind.SEAL, status=ArrivalStatus.QUARANTINED,
                    detail=reason,
                )
                self.ledger.record_quarantine(
                    arrival_id=envelope.arrival_id, package_key=package_key,
                    kind=ArrivalKind.SEAL, reason=reason,
                )
            self._remove_source(entry_dir)
            report['quarantined'].append(
                {'arrival_id': envelope.arrival_id, 'reason': reason})
            return
        sealed = self._seal_latest_manifest(package_key, drafts[-1], envelope.arrival_id)
        self._remove_source(entry_dir)
        report['accepted'].append(
            {'arrival_id': envelope.arrival_id, 'package': package_key,
             'manifest_digest': sealed.digest, 'sealed': True}
        )

    def _seal_latest_manifest(self, package_key, latest_row, seal_arrival_id):
        raw_body = json.loads(latest_row['body'])
        sealed_manifest = Manifest(
            voyage=raw_body['voyage'], station=raw_body['station'],
            generation=raw_body['generation'], sealed=True,
            members=tuple(
                MemberSpec(name=m['name'], size=m['size'], digest=m['digest'])
                for m in raw_body['members']
            ),
            digest='',
        )
        from .model import manifest_digest

        digest = manifest_digest(
            sealed_manifest.voyage, sealed_manifest.station,
            sealed_manifest.generation, True, sealed_manifest.members,
        )
        object.__setattr__(sealed_manifest, 'digest', digest)
        with self.ledger.transaction():
            self.ledger._insert_manifest_locked(sealed_manifest, seal_arrival_id)
            self.ledger.record_arrival(
                arrival_id=seal_arrival_id, package_key=package_key,
                kind=ArrivalKind.SEAL, status=ArrivalStatus.ACCEPTED,
                manifest_digest=digest,
            )
            self._reconcile_pending_locked(package_key, sealed_manifest)
            # 追认/隔离完成后再重算，确保 READY 与成员确认在同一事务内可见
            self.ledger._recompute_locked(package_key)
        return sealed_manifest

    # ------------------------------------------------------------- 成员到件

    def _handle_member(self, envelope, entry_dir, report):
        package_key = envelope.package_key()
        payload = entry_dir / PAYLOAD_NAME
        if not payload.is_file():
            self._quarantine(envelope, entry_dir, '成员到件缺少 payload.bin', report)
            return
        try:
            digest, size = self.objects.add_path(payload)
        except DigestMismatch as exc:
            self._quarantine(envelope, entry_dir, str(exc), report)
            return

        package = self.ledger.get_package(package_key)
        if package is None:
            return self._accept_or_pending(
                envelope, entry_dir, report, digest, size,
                status=ArrivalStatus.PENDING,
                detail='清单尚未到达，成员先暂存，身份以信封归属为准',
            )

        sealed = self.ledger.sealed_manifest(package_key)
        if sealed is not None:
            spec = next(
                (m for m in self.ledger.get_manifest_members(sealed['manifest_digest'])
                 if m['name'] == envelope.member_name),
                None,
            )
            if spec is None or spec['digest'] != digest or spec['size'] != size:
                reason = (
                    f'成员 {envelope.member_name} 与封包清单凭据不符或不在清单内'
                    f'（清单 {sealed["manifest_digest"][:12]}…）：实际 {digest[:12]}…/{size}B'
                )
                self._quarantine(envelope, entry_dir, reason, report,
                                 object_digest=digest, object_size=size)
                return
        else:
            spec = None

        # 只有与已确认内容字节一致的重发才算重复；同名异内容在上面已被隔离，
        # 绝不会因为文件名相同而覆盖旧件
        existing = self.ledger.accepted_member(package_key, envelope.member_name)
        if existing is not None:
            if existing['object_digest'] != digest:
                reason = (
                    f'同名成员 {envelope.member_name} 内容与已确认件不同'
                    f'（已收 {existing["object_digest"][:12]}…，'
                    f'新到 {digest[:12]}…），拒绝按名字覆盖'
                )
                self._quarantine(envelope, entry_dir, reason, report,
                                 object_digest=digest, object_size=size)
                return
            with self.ledger.transaction():
                self.ledger.record_arrival(
                    arrival_id=envelope.arrival_id, package_key=package_key,
                    kind=ArrivalKind.MEMBER, status=ArrivalStatus.DUPLICATE,
                    member_name=envelope.member_name, object_digest=digest,
                    object_size=size, detail='成员已收齐且字节一致，重复到件幂等忽略',
                )
            self._remove_source(entry_dir)
            report['duplicate'].append({'arrival_id': envelope.arrival_id})
            return

        # 未封包：先登记为 PENDING，封包时按冻结的成员集合再判定；
        # 已封包且凭据一致（走到这里即已通过上面的封包凭据核对）：直接 ACCEPTED
        status = ArrivalStatus.ACCEPTED if sealed is not None else ArrivalStatus.PENDING
        return self._accept_or_pending(
            envelope, entry_dir, report, digest, size,
            status=status,
            detail='与封包清单凭据一致' if sealed is not None
            else '清单未封包，暂存待封包判定',
        )

    def _accept_or_pending(self, envelope, entry_dir, report, digest, size, *,
                           status, detail):
        package_key = envelope.package_key()
        with self.ledger.transaction():
            self.ledger.record_arrival(
                arrival_id=envelope.arrival_id, package_key=package_key,
                kind=ArrivalKind.MEMBER, status=status,
                member_name=envelope.member_name, object_digest=digest,
                object_size=size, detail=detail,
            )
            if status is ArrivalStatus.ACCEPTED and package_key:
                # 状态在同一事务内重算，保证 READY 与成员确认同时可见
                self.ledger._recompute_locked(package_key)
        self._remove_source(entry_dir)
        bucket = 'pending' if status is ArrivalStatus.PENDING else 'accepted'
        report[bucket].append(
            {'arrival_id': envelope.arrival_id, 'package': package_key,
             'member': envelope.member_name, 'digest': digest, 'size': size}
        )

    # ------------------------------------------------------------- 隔离/清理

    def _quarantine(self, envelope, entry_dir, reason, report, *,
                    object_digest=None, object_size=None):
        path = quarantine_entry(self.root, envelope.arrival_id, entry_dir, reason)
        package_key = envelope.package_key()
        with self.ledger.transaction():
            self.ledger.record_arrival(
                arrival_id=envelope.arrival_id, package_key=package_key,
                kind=envelope.kind, status=ArrivalStatus.QUARANTINED,
                member_name=envelope.member_name, object_digest=object_digest,
                object_size=object_size, quarantine_path=str(path), detail=reason,
            )
            self.ledger.record_quarantine(
                arrival_id=envelope.arrival_id, package_key=package_key,
                kind=envelope.kind, reason=reason, path=str(path),
            )
        report['quarantined'].append(
            {'arrival_id': envelope.arrival_id, 'package': package_key, 'reason': reason}
        )

    def _remove_source(self, entry_dir: Path):
        """可证明的接收确认（对象 fsync + 台账提交）之后才清理来源。"""

        if not entry_dir.exists():
            return
        for child in sorted(entry_dir.rglob('*'), reverse=True):
            if child.is_file() or child.is_symlink():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
        entry_dir.rmdir()
        fsync_directory(self.processing)

    # ------------------------------------------------------------------ 概览

    def status(self):
        packages = []
        for package in self.ledger.list_packages():
            key = package['package_key']
            state = PackageState(package['state'])
            sealed = self.ledger.sealed_manifest(key)
            members = []
            missing = []
            if sealed:
                for spec in self.ledger.get_manifest_members(sealed['manifest_digest']):
                    accepted = self.ledger.accepted_member(key, spec['name'])
                    members.append({
                        'name': spec['name'],
                        'size': spec['size'],
                        'digest': spec['digest'],
                        'present': accepted is not None,
                        'stored_digest': accepted['object_digest'] if accepted else None,
                    })
                    if accepted is None:
                        missing.append(spec['name'])
            packages.append({
                'package': key,
                'voyage': package['voyage'],
                'station': package['station'],
                'generation': package['generation'],
                'state': state.value,
                'state_text': state.中文说明,
                'sealed_manifest_digest': package['sealed_manifest_digest'],
                'supersedes': package['supersedes'],
                'manifest_revisions': len(self.ledger.list_manifests(key)),
                'members': members,
                'missing': missing,
                'pending_members': [
                    row['arrival_id'] for row in self.ledger.list_pending_members(key)
                ],
            })
        return {
            'root': str(self.root),
            'packages': packages,
            'quarantine': self.ledger.list_quarantine(),
            'counts': self.ledger.counts(),
        }
