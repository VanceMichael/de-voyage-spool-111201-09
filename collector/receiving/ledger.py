"""SQLite 台账：接收事实的持久凭据。

所有状态推进都在 ``BEGIN IMMEDIATE`` 事务内完成，配合文件系统 fsync 保证
「先落盘确认、后清理来源」。台账不依赖文件名判断任何业务事实。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .model import ArrivalKind, ArrivalStatus, Manifest, PackageState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS packages(
    package_key TEXT PRIMARY KEY,
    voyage TEXT NOT NULL,
    station TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    sealed_manifest_digest TEXT,
    supersedes TEXT REFERENCES packages(package_key),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests(
    manifest_digest TEXT PRIMARY KEY,
    package_key TEXT NOT NULL REFERENCES packages(package_key),
    voyage TEXT NOT NULL,
    station TEXT NOT NULL,
    generation INTEGER NOT NULL,
    sealed INTEGER NOT NULL,
    body TEXT NOT NULL,
    received_arrival_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifest_members(
    manifest_digest TEXT NOT NULL REFERENCES manifests(manifest_digest),
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (manifest_digest, name)
);
CREATE TABLE IF NOT EXISTS arrivals(
    arrival_id TEXT PRIMARY KEY,
    package_key TEXT,
    kind TEXT NOT NULL,
    member_name TEXT,
    status TEXT NOT NULL,
    object_digest TEXT,
    object_size INTEGER,
    manifest_digest TEXT,
    quarantine_path TEXT,
    detail TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_arrivals_package ON arrivals(package_key, status);
CREATE TABLE IF NOT EXISTS duplicate_arrivals(
    arrival_id TEXT NOT NULL,
    package_key TEXT,
    kind TEXT NOT NULL,
    detail TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (arrival_id, created_at)
);
CREATE TABLE IF NOT EXISTS quarantine(
    arrival_id TEXT PRIMARY KEY,
    package_key TEXT,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    path TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS publish_events(
    package_key TEXT PRIMARY KEY REFERENCES packages(package_key),
    manifest_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    path TEXT,
    staged_at TEXT NOT NULL,
    published_at TEXT
);
"""


def _now():
    return datetime.now(timezone.utc).isoformat()


class Ledger:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.execute('PRAGMA synchronous=FULL')
        self.connection.executescript(_SCHEMA)

    def close(self):
        self.connection.close()

    @contextmanager
    def transaction(self):
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    # ------------------------------------------------------------------ 包

    def get_package(self, package_key):
        row = self.connection.execute(
            'SELECT * FROM packages WHERE package_key=?', (package_key,)
        ).fetchone()
        return dict(row) if row else None

    def list_packages(self):
        rows = self.connection.execute(
            'SELECT * FROM packages ORDER BY voyage, station, generation'
        ).fetchall()
        return [dict(row) for row in rows]

    def ensure_package(self, manifest: Manifest):
        key = manifest.package_key
        with self.transaction():
            if self.get_package(key) is None:
                self._insert_package_locked(manifest)
                if manifest.sealed:
                    self._recompute_locked(key)
        return self.get_package(key)

    def _insert_package_locked(self, manifest: Manifest):
        """调用方必须已持有事务。"""

        now = _now()
        supersedes = None
        if manifest.sealed:
            prior = self.connection.execute(
                'SELECT package_key FROM packages WHERE voyage=? AND station=? '
                'AND generation < ? ORDER BY generation DESC LIMIT 1',
                (manifest.voyage, manifest.station, manifest.generation),
            ).fetchone()
            supersedes = prior['package_key'] if prior else None
        self.connection.execute(
            'INSERT INTO packages(package_key, voyage, station, generation, '
            'state, sealed_manifest_digest, supersedes, created_at, updated_at) '
            'VALUES (?,?,?,?,?,?,?,?,?)',
            (manifest.package_key, manifest.voyage, manifest.station,
             manifest.generation, PackageState.AWAITING_SEAL.value,
             manifest.digest if manifest.sealed else None,
             supersedes, now, now),
        )

    def _set_state_locked(self, package_key, state: PackageState):
        self.connection.execute(
            'UPDATE packages SET state=?, updated_at=? WHERE package_key=?',
            (state.value, _now(), package_key),
        )

    def recompute_state(self, package_key):
        with self.transaction():
            self._recompute_locked(package_key)
        return self.get_package(package_key)

    def _recompute_locked(self, package_key):
        """根据封包清单成员与 ACCEPTED 到件重算状态，PUBLISHED 为终态。"""

        package = self.get_package(package_key)
        if package is None or package['state'] == PackageState.PUBLISHED.value:
            return
        sealed_digest = package['sealed_manifest_digest']
        if not sealed_digest:
            self._set_state_locked(package_key, PackageState.AWAITING_SEAL)
            return
        members = self.connection.execute(
            'SELECT name, digest FROM manifest_members WHERE manifest_digest=?',
            (sealed_digest,),
        ).fetchall()
        complete = True
        for member in members:
            hit = self.connection.execute(
                "SELECT 1 FROM arrivals WHERE package_key=? AND member_name=? "
                "AND status=? AND object_digest=? LIMIT 1",
                (package_key, member['name'], ArrivalStatus.ACCEPTED.value,
                 member['digest']),
            ).fetchone()
            if hit is None:
                complete = False
                break
        self._set_state_locked(
            package_key,
            PackageState.READY if complete else PackageState.SEALED_INCOMPLETE,
        )

    # --------------------------------------------------------------- 清单

    def get_manifest(self, manifest_digest):
        row = self.connection.execute(
            'SELECT * FROM manifests WHERE manifest_digest=?', (manifest_digest,)
        ).fetchone()
        return dict(row) if row else None

    def insert_manifest(self, manifest: Manifest, arrival_id: str):
        with self.transaction():
            self._insert_manifest_locked(manifest, arrival_id)

    def _insert_manifest_locked(self, manifest: Manifest, arrival_id: str):
        """调用方必须已持有事务。"""

        now = _now()
        if self.get_manifest(manifest.digest) is not None:
            return
        self.connection.execute(
            'INSERT INTO manifests(manifest_digest, package_key, voyage, station, '
            'generation, sealed, body, received_arrival_id, created_at) '
            'VALUES (?,?,?,?,?,?,?,?,?)',
            (manifest.digest, manifest.package_key, manifest.voyage,
             manifest.station, manifest.generation, int(manifest.sealed),
             json.dumps(manifest.to_dict(), ensure_ascii=False, sort_keys=True),
             arrival_id, now),
        )
        self.connection.executemany(
            'INSERT OR IGNORE INTO manifest_members(manifest_digest, name, size, digest) '
            'VALUES (?,?,?,?)',
            [(manifest.digest, m.name, m.size, m.digest) for m in manifest.members],
        )
        package = self.get_package(manifest.package_key)
        if manifest.sealed and package['sealed_manifest_digest'] is None:
            self.connection.execute(
                'UPDATE packages SET sealed_manifest_digest=?, updated_at=? '
                'WHERE package_key=?',
                (manifest.digest, now, manifest.package_key),
            )
        self._recompute_locked(manifest.package_key)

    def list_manifests(self, package_key):
        rows = self.connection.execute(
            'SELECT * FROM manifests WHERE package_key=? ORDER BY created_at, rowid',
            (package_key,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_manifest_members(self, manifest_digest):
        rows = self.connection.execute(
            'SELECT * FROM manifest_members WHERE manifest_digest=? ORDER BY name',
            (manifest_digest,),
        ).fetchall()
        return [dict(row) for row in rows]

    def sealed_manifest(self, package_key):
        package = self.get_package(package_key)
        if not package or not package['sealed_manifest_digest']:
            return None
        return self.get_manifest(package['sealed_manifest_digest'])

    # --------------------------------------------------------------- 到件

    def get_arrival(self, arrival_id):
        row = self.connection.execute(
            'SELECT * FROM arrivals WHERE arrival_id=?', (arrival_id,)
        ).fetchone()
        return dict(row) if row else None

    def record_arrival(self, *, arrival_id, package_key, kind: ArrivalKind,
                       status: ArrivalStatus, member_name=None, object_digest=None,
                       object_size=None, manifest_digest=None, quarantine_path=None,
                       detail=None):
        self.connection.execute(
            'INSERT INTO arrivals(arrival_id, package_key, kind, member_name, status, '
            'object_digest, object_size, manifest_digest, quarantine_path, detail, updated_at) '
            'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (arrival_id, package_key, kind.value, member_name, status.value,
             object_digest, object_size, manifest_digest, quarantine_path, detail, _now()),
        )

    def record_duplicate(self, *, arrival_id, package_key, kind: ArrivalKind,
                         detail=None):
        """登记一次重复投递；原始 ACCEPTED 凭据行保持不变。"""

        self.connection.execute(
            'INSERT INTO duplicate_arrivals(arrival_id, package_key, kind, detail, '
            'created_at) VALUES (?,?,?,?,?)',
            (arrival_id, package_key, kind.value, detail, _now()),
        )

    def count_duplicates(self):
        return self.connection.execute(
            'SELECT count(*) FROM duplicate_arrivals'
        ).fetchone()[0]

    def update_arrival(self, arrival_id, *, status: ArrivalStatus, object_digest=None,
                       object_size=None, quarantine_path=None, detail=None):
        self.connection.execute(
            'UPDATE arrivals SET status=?, object_digest=COALESCE(?, object_digest), '
            'object_size=COALESCE(?, object_size), quarantine_path=COALESCE(?, quarantine_path), '
            'detail=COALESCE(?, detail), updated_at=? WHERE arrival_id=?',
            (status.value, object_digest, object_size, quarantine_path, detail,
             _now(), arrival_id),
        )

    def list_pending_members(self, package_key):
        rows = self.connection.execute(
            "SELECT * FROM arrivals WHERE package_key=? AND kind=? AND status=? "
            "ORDER BY rowid",
            (package_key, ArrivalKind.MEMBER.value, ArrivalStatus.PENDING.value),
        ).fetchall()
        return [dict(row) for row in rows]

    def accepted_member(self, package_key, member_name):
        row = self.connection.execute(
            "SELECT * FROM arrivals WHERE package_key=? AND member_name=? AND status=? "
            "LIMIT 1",
            (package_key, member_name, ArrivalStatus.ACCEPTED.value),
        ).fetchone()
        return dict(row) if row else None

    # --------------------------------------------------------------- 隔离

    def record_quarantine(self, *, arrival_id, package_key, kind: ArrivalKind,
                          reason, path=None):
        self.connection.execute(
            'INSERT OR IGNORE INTO quarantine(arrival_id, package_key, kind, reason, path, '
            'created_at) VALUES (?,?,?,?,?,?)',
            (arrival_id, package_key, kind.value, reason, path, _now()),
        )

    def list_quarantine(self):
        rows = self.connection.execute(
            'SELECT * FROM quarantine ORDER BY rowid'
        ).fetchall()
        return [dict(row) for row in rows]

    # --------------------------------------------------------------- 发布

    def get_publish_event(self, package_key):
        row = self.connection.execute(
            'SELECT * FROM publish_events WHERE package_key=?', (package_key,)
        ).fetchone()
        return dict(row) if row else None

    def stage_publish_event(self, package_key, manifest_digest, staging_path):
        self.connection.execute(
            'INSERT INTO publish_events(package_key, manifest_digest, state, path, '
            'staged_at, published_at) VALUES (?,?,?,?,?,NULL) '
            'ON CONFLICT(package_key) DO UPDATE SET manifest_digest=excluded.manifest_digest, '
            'state=excluded.state, path=excluded.path, staged_at=excluded.staged_at, '
            'published_at=NULL',
            (package_key, manifest_digest, 'STAGED', str(staging_path), _now()),
        )

    def mark_published(self, package_key, final_path):
        self.connection.execute(
            "UPDATE publish_events SET state='PUBLISHED', path=?, published_at=? "
            'WHERE package_key=?',
            (str(final_path), _now(), package_key),
        )
        self._set_state_locked(package_key, PackageState.PUBLISHED)

    # --------------------------------------------------------------- 概览

    def counts(self):
        result = {}
        for table in ('packages', 'manifests', 'arrivals', 'quarantine',
                      'publish_events'):
            result[table] = self.connection.execute(
                f'SELECT count(*) FROM {table}'
            ).fetchone()[0]
        return result
