"""文件系统布局与内容寻址对象仓库。

目录布局（spool 根目录下）：

    inbox/                 接收目录：船方拷入的到件单元，每个一个子目录
      .processing/         已被某进程原子认领、正在处理的到件
    objects/ab/<digest>    内容寻址仓库：按 sha256 存放，永不按名字覆盖
    quarantine/<id>/       隔离区：保留原信封、载荷与隔离原因
    staging/               发布前的整包临时构建区（半包永不进入 published）
    published/<pkg>/       正式可用目录，仅由整包 rename 晋升产生
      <generation>/
    ledger.sqlite3         台账（Ledger）
    .locks/                fcntl 进程锁
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

PAYLOAD_NAME = 'payload.bin'
ENVELOPE_NAME = 'envelope.json'
REASON_NAME = 'reason.txt'
_CHUNK = 1024 * 1024


class ObjectStore:
    """内容寻址仓库：同名不同内容的成员各自独立存放。"""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.objects = self.root / 'objects'
        self.tmp = self.root / 'tmp'
        self.objects.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(parents=True, exist_ok=True)

    def object_path(self, digest: str) -> Path:
        return self.objects / digest[:2] / digest

    def has(self, digest: str) -> bool:
        return self.object_path(digest).is_file()

    def add_path(self, source: Path, expected_digest: str | None = None):
        """把一个已落盘文件流式摘要后原子放入仓库，返回 (digest, size)。

        先写 tmp/ 并 fsync，再 rename 到最终分片路径；同摘要对象已存在则直接复用，
        绝不因为文件名相同而覆盖任何旧件。
        """

        hasher = hashlib.sha256()
        size = 0
        with source.open('rb') as handle:
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
                size += len(chunk)
        digest = hasher.hexdigest()
        if expected_digest is not None and digest != expected_digest:
            raise DigestMismatch(digest, expected_digest)

        target = self.object_path(digest)
        if target.is_file():
            return digest, size
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = self.tmp / f'obj-{os.getpid()}-{digest}'
        try:
            with tmp_file.open('wb') as out, source.open('rb') as handle:
                while True:
                    chunk = handle.read(_CHUNK)
                    if not chunk:
                        break
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_file, target)
            self._fsync_dir(target.parent)
        finally:
            tmp_file.unlink(missing_ok=True)
        return digest, size

    def add_bytes(self, data: bytes, expected_digest: str | None = None):
        import tempfile

        fd, name = tempfile.mkstemp(dir=self.tmp, prefix='obj-')
        path = Path(name)
        try:
            with os.fdopen(fd, 'wb') as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            return self.add_path(path, expected_digest)
        finally:
            path.unlink(missing_ok=True)

    @staticmethod
    def _fsync_dir(directory: Path):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class DigestMismatch(Exception):
    def __init__(self, actual, expected):
        super().__init__(f'摘要不符: 实际 {actual}，期望 {expected}')
        self.actual = actual
        self.expected = expected


def ensure_layout(root: Path) -> Path:
    """创建并返回标准化的 spool 根目录。"""

    root = Path(root)
    for name in ('inbox', 'inbox/.processing', 'objects', 'quarantine',
                 'staging', 'published', 'tmp', '.locks'):
        (root / name).mkdir(parents=True, exist_ok=True)
    return root


def quarantine_entry(root: Path, arrival_id: str, processing_dir: Path | None,
                     reason: str) -> Path:
    """把到件整体移入隔离区并写入原因，目标冲突时追加序号。"""

    target = root / 'quarantine' / arrival_id
    counter = 1
    while target.exists():
        target = root / 'quarantine' / f'{arrival_id}-{counter}'
        counter += 1
    target.parent.mkdir(parents=True, exist_ok=True)
    if processing_dir is not None and processing_dir.exists():
        os.rename(processing_dir, target)
    else:
        target.mkdir(parents=True, exist_ok=True)
    reason_path = target / REASON_NAME
    reason_path.write_text(reason + '\n', encoding='utf-8')
    return target


def hardlink_or_copy(source: Path, target: Path):
    """优先硬链接（零拷贝且 inode 相同），跨设备时退回复制并 fsync。"""

    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, target)
    except OSError:
        shutil.copyfile(source, target)
        with target.open('rb') as handle:
            os.fsync(handle.fileno())


def fsync_directory(path: Path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
