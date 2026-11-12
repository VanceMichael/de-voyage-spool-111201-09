"""整包发布：分析组只能看到完整包，看不到半包。

发布流程（每个阶段都有崩溃后的对账点）：
1. 仅 READY 包可发布；在 ``staging/<package>-g<n>/`` 构建整包，成员用硬链接
   指向内容寻址仓库，并逐个复核字节数与摘要；
2. 构建完成、目录 fsync 后，台账记 STAGED；
3. 同文件系统内一次 ``os.rename`` 把整包原子晋升到 ``published/<pkg>/g<n>``，
   再在事务内记 PUBLISHED 并翻转包状态；
4. 磁盘满或中途停止只会留下 staging 残片，published/ 下永远没有半包。
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path

from .fsstore import ObjectStore, fsync_directory, hardlink_or_copy
from .ledger import Ledger
from .model import PackageState

PACKAGE_JSON = 'PACKAGE.json'


class PublishError(RuntimeError):
    pass


class _Publisher:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.ledger = Ledger(self.root / 'ledger.sqlite3')
        self.objects = ObjectStore(self.root)
        self.staging = self.root / 'staging'
        self.published = self.root / 'published'
        self.staging.mkdir(parents=True, exist_ok=True)
        self.published.mkdir(parents=True, exist_ok=True)
        self.lock_file = self.root / '.locks' / 'publisher.lock'

    def close(self):
        self.ledger.close()

    @contextmanager
    def _lock(self):
        with self.lock_file.open('a+') as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _staging_dir(self, package_key: str) -> Path:
        safe = package_key.replace('/', '__')
        return self.staging / safe

    def _final_dir(self, package_key: str) -> Path:
        voyage, station, generation = package_key.split('/')
        return self.published / voyage / station / generation

    def publish(self, package_key: str, *, crash: str | None = None):
        """发布单个包。crash 仅用于演示：before_promote 在晋升前模拟进程被杀。"""

        with self._lock():
            package = self.ledger.get_package(package_key)
            if package is None:
                raise PublishError(f'未知包: {package_key}')
            state = package['state']
            if state == PackageState.PUBLISHED.value:
                return self.get_publish_event_safe(package_key)
            if state != PackageState.READY.value:
                raise PublishError(
                    f'包 {package_key} 当前状态 {state}，只有 READY 才能发布'
                )

            sealed = self.ledger.sealed_manifest(package_key)
            members = self.ledger.get_manifest_members(sealed['manifest_digest'])
            staging_dir = self._staging_dir(package_key)
            if staging_dir.exists():
                # 上一轮崩溃残片：整包重建，不做增量修补
                self._rmtree(staging_dir)
            staging_dir.mkdir(parents=True)

            manifest_body = json.loads(sealed['body'])
            for spec in members:
                source = self.objects.object_path(spec['digest'])
                if not source.is_file():
                    raise PublishError(
                        f'成员 {spec["name"]} 的对象仓库文件缺失: {spec["digest"]}'
                    )
                target = staging_dir / spec['name']
                hardlink_or_copy(source, target)
                self._verify_file(target, spec['size'], spec['digest'])

            descriptor = {
                'package': package_key,
                'voyage': package['voyage'],
                'station': package['station'],
                'generation': package['generation'],
                'manifest_digest': sealed['manifest_digest'],
                'supersedes': package['supersedes'],
                'members': members,
            }
            descriptor_path = staging_dir / PACKAGE_JSON
            descriptor_path.write_text(
                json.dumps(descriptor, ensure_ascii=False, indent=2, sort_keys=True),
                encoding='utf-8',
            )
            fsync_directory(staging_dir)

            with self.ledger.transaction():
                self.ledger.stage_publish_event(
                    package_key, sealed['manifest_digest'], staging_dir
                )

            if crash == 'before_promote':
                # 模拟磁盘满/断电/SIGKILL：台账已记 STAGED，published/ 下什么都没有
                raise _CrashBeforePromote(package_key, staging_dir)

            final_dir = self._final_dir(package_key)
            if final_dir.exists():
                raise PublishError(f'正式目录已存在，拒绝覆盖: {final_dir}')
            final_dir.parent.mkdir(parents=True, exist_ok=True)
            os.rename(staging_dir, final_dir)
            fsync_directory(final_dir.parent)
            with self.ledger.transaction():
                self.ledger.mark_published(package_key, final_dir)
            return self.ledger.get_publish_event(package_key)

    def publish_all_ready(self, **kwargs):
        results = []
        for package in self.ledger.list_packages():
            if package['state'] == PackageState.READY.value:
                results.append(self.publish(package['package_key'], **kwargs))
        return results

    def recover(self):
        """对账所有 STAGED 残片并完成或重建晋升；published 与台账不一致时修复。"""

        actions = []
        known_staging = set()
        with self._lock():
            for package in self.ledger.list_packages():
                key = package['package_key']
                event = self.ledger.get_publish_event(key)
                final_dir = self._final_dir(key)
                if event is None:
                    continue
                known_staging.add(self._staging_dir(key))
                if event['state'] == 'PUBLISHED':
                    if not final_dir.exists():
                        # 极端情况：rename 已完成但事务未提交——重建并补记
                        self._rebuild_staging(key)
                        os.rename(self._staging_dir(key), final_dir)
                        fsync_directory(final_dir.parent)
                        with self.ledger.transaction():
                            self.ledger.mark_published(key, final_dir)
                        actions.append(
                            {'package': key, 'action': 'rebuilt_after_lost_rename'})
                    continue

                # STAGED：上一轮在晋升前/中崩溃
                if final_dir.exists():
                    # rename 已落地但事务未提交：校验正式目录完整后只补记台账
                    self._verify_published_tree(key, final_dir)
                    staging_dir = self._staging_dir(key)
                    if staging_dir.exists():
                        self._rmtree(staging_dir)
                    with self.ledger.transaction():
                        self.ledger.mark_published(key, final_dir)
                    actions.append(
                        {'package': key, 'action': 'rebuilt_after_lost_rename'})
                    continue
                # 正式目录尚不存在：整包重新构建后晋升
                self._rebuild_staging(key)
                final_dir.parent.mkdir(parents=True, exist_ok=True)
                os.rename(self._staging_dir(key), final_dir)
                fsync_directory(final_dir.parent)
                with self.ledger.transaction():
                    self.ledger.mark_published(key, final_dir)
                actions.append(
                    {'package': key, 'action': 'promoted_after_recovery'})

            # 没有任何发布事件对应的 staging 残片：中断/垃圾残留，安全清除
            for leftover in sorted(self.staging.iterdir()):
                if leftover.is_dir() and leftover not in known_staging:
                    self._rmtree(leftover)
                    actions.append({
                        'package': leftover.name.replace('__', '/'),
                        'action': 'discarded_orphan_staging',
                    })
        return actions

    def _rebuild_staging(self, package_key):
        """按封包清单从对象仓库重建整包（复用 publish 的校验路径）。"""

        package = self.ledger.get_package(package_key)
        sealed = self.ledger.sealed_manifest(package_key)
        members = self.ledger.get_manifest_members(sealed['manifest_digest'])
        staging_dir = self._staging_dir(package_key)
        if staging_dir.exists():
            self._rmtree(staging_dir)
        staging_dir.mkdir(parents=True)
        for spec in members:
            hardlink_or_copy(self.objects.object_path(spec['digest']),
                             staging_dir / spec['name'])
            self._verify_file(staging_dir / spec['name'], spec['size'], spec['digest'])
        descriptor = {
            'package': package_key,
            'voyage': package['voyage'],
            'station': package['station'],
            'generation': package['generation'],
            'manifest_digest': sealed['manifest_digest'],
            'supersedes': package['supersedes'],
            'members': members,
        }
        (staging_dir / PACKAGE_JSON).write_text(
            json.dumps(descriptor, ensure_ascii=False, indent=2, sort_keys=True),
            encoding='utf-8',
        )
        fsync_directory(staging_dir)

    @staticmethod
    def _verify_file(path: Path, expected_size, expected_digest):
        hasher = hashlib.sha256()
        size = 0
        with path.open('rb') as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
                size += len(chunk)
        if size != expected_size or hasher.hexdigest() != expected_digest:
            raise PublishError(f'发布校验失败: {path}')

    def _verify_published_tree(self, package_key: str, final_dir: Path):
        """对已落地的正式目录按封包清单逐个复核，防止半截 rename 被误认完整。"""

        if not (final_dir / PACKAGE_JSON).is_file():
            raise PublishError(f'正式目录缺少 {PACKAGE_JSON}: {final_dir}')
        sealed = self.ledger.sealed_manifest(package_key)
        for spec in self.ledger.get_manifest_members(sealed['manifest_digest']):
            self._verify_file(final_dir / spec['name'], spec['size'], spec['digest'])

    @staticmethod
    def _rmtree(path: Path):
        import shutil

        shutil.rmtree(path)

    def get_publish_event_safe(self, key):
        return self.ledger.get_publish_event(key)


class _CrashBeforePromote(RuntimeError):
    def __init__(self, package_key, staging_dir):
        super().__init__(f'模拟崩溃：{package_key} 已构建于 {staging_dir}，尚未晋升')
        self.package_key = package_key
        self.staging_dir = staging_dir


# 对外保持无状态函数风格，内部复用同一个实现
def publish(root: Path, package_key: str, *, crash: str | None = None):
    publisher = _Publisher(root)
    try:
        return publisher.publish(package_key, crash=crash)
    finally:
        publisher.close()


def publish_all_ready(root: Path, **kwargs):
    publisher = _Publisher(root)
    try:
        return publisher.publish_all_ready(**kwargs)
    finally:
        publisher.close()


def recover(root: Path):
    publisher = _Publisher(root)
    try:
        return publisher.recover()
    finally:
        publisher.close()
