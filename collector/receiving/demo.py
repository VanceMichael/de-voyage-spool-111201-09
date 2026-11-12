"""端到端演示：失序到件、封包后更正、发布前崩溃恢复。

时间线来自 fixtures/deliveries.json（路径由命令行参数传入，不依赖工作目录），
额外补充三个压力情景（重名异内容、重复到件、封包后试图改写同代际）以展示
隔离与幂等。崩溃环节在独立子进程中触发真实进程退出，再用恢复命令对账。
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from .publisher import publish, recover
from .receiver import Receiver
from .synthetic import (
    make_member,
    manifest_envelope,
    member_envelope,
    seal_envelope,
    synthesize_files,
    write_arrival,
)


class Demo:
    def __init__(self, root: Path, fixture_path: Path):
        self.root = Path(root)
        self.fixture_path = Path(fixture_path)
        self.events = json.loads(self.fixture_path.read_text(encoding='utf-8'))['events']
        self.receiver = Receiver(self.root)
        # 每个航次/站位的合成文件内容表：name -> bytes
        self.files: dict[str, bytes] = {}
        self.trace = []

    def close(self):
        self.receiver.close()

    # --------------------------------------------------------------- 工具

    def _file_content(self, name: str) -> bytes:
        if name not in self.files:
            self.files.update(
                synthesize_files(f'{name}-{len(self.files)}', [name])
            )
        return self.files[name]

    def _scan(self, label):
        report = self.receiver.scan_once()
        self.trace.append({'step': label, 'report': report})
        return report

    def _states(self):
        return {
            p['package']: p['state']
            for p in self.receiver.status()['packages']
        }

    # --------------------------------------------------------------- 事件

    def deliver_manifest(self, event: dict, arrival_id: str):
        names = event['members']
        files = {name: self._file_content(name) for name in names}
        members = [
            dict(zip(('name', 'size', 'digest'), (name, *make_member(name, data))))
            for name, data in sorted(files.items())
        ]
        envelope, digest = manifest_envelope(
            arrival_id, event.get('voyage', 'V1'), event.get('station', 'S1'),
            event['generation'], event['sealed'], members,
        )
        write_arrival(self.receiver.inbox, arrival_id, envelope)
        return digest, members

    def deliver_member(self, event: dict, arrival_id: str, *,
                       content: bytes | None = None,
                       voyage='V1', station='S1',
                       generation: int | None = None,
                       name: str | None = None):
        generation = event.get('generation', 1) if generation is None else generation
        member_name = name or event['name']
        payload = content if content is not None else self._file_content(member_name)
        envelope = member_envelope(
            arrival_id, voyage, station, generation, member_name,
        )
        write_arrival(self.receiver.inbox, arrival_id, envelope, payload)
        return member_name, payload

    def deliver_seal(self, event: dict, arrival_id: str):
        envelope = seal_envelope(
            arrival_id, event.get('voyage', 'V1'), event.get('station', 'S1'),
            event['generation'],
        )
        write_arrival(self.receiver.inbox, arrival_id, envelope)

    # --------------------------------------------------------------- 主流程

    def run(self):
        result = {'fixture': str(self.fixture_path), 'root': str(self.root),
                  'phases': [], 'fixture_trace': self.trace}

        # ===== 第一部分：严格回放 fixtures/deliveries.json 的非崩溃事件 =====
        manifest_counter = member_counter = seal_counter = 0
        crash_event = None
        for index, event in enumerate(self.events, start=1):
            kind = event['type']
            if kind == 'crash':
                crash_event = (index, event)
                continue
            manifest_counter, member_counter, seal_counter = self._replay_event(
                index, event, manifest_counter, member_counter, seal_counter, result)

        # 失序到件的收尾：fixture 时间线结束时 g2 仍缺 a.csv，先补齐到 READY，
        # 这样随后的「发布前崩溃」才发生在真正的晋升边界上
        self._deliver_late_g2_member(result)

        # ===== 第二部分：在独立子进程中真实触发发布前崩溃 =====
        if crash_event is not None:
            result['phases'].append(self._crash_phase(crash_event[1], crash_event[0]))

        # ===== 第三部分：补充压力情景 =====
        result['extra'] = self._extra_scenarios()
        # ===== 第四部分：对账恢复与最终核对 =====
        result['recovery'] = recover(self.root)
        result['final_status'] = self.receiver.status()
        result['published_tree'] = self._published_tree()
        return result

    def _replay_event(self, index, event, manifest_counter, member_counter,
                      seal_counter, result):
        kind = event['type']
        if kind == 'manifest':
            manifest_counter += 1
            arrival_id = f'm{manifest_counter}'
            digest, members = self.deliver_manifest(event, arrival_id)
            report = self._scan(f'事件{index}: 清单 g{event["generation"]} '
                                f'sealed={event["sealed"]}')
            result['phases'].append({
                'event': index, 'type': 'manifest',
                'generation': event['generation'], 'sealed': event['sealed'],
                'manifest_digest': digest,
                'members': [m['name'] for m in members],
                'report': report, 'states': self._states(),
            })
        elif kind == 'member':
            member_counter += 1
            arrival_id = f'f{member_counter}'
            member_name, _payload = self.deliver_member(event, arrival_id)
            report = self._scan(f'事件{index}: 成员 {member_name} 到件')
            result['phases'].append({
                'event': index, 'type': 'member', 'name': member_name,
                'arrival_id': arrival_id, 'report': report, 'states': self._states(),
            })
        elif kind == 'seal':
            seal_counter += 1
            arrival_id = f's{seal_counter}'
            self.deliver_seal(event, arrival_id)
            report = self._scan(f'事件{index}: 封包 g{event["generation"]}')
            result['phases'].append({
                'event': index, 'type': 'seal',
                'generation': event['generation'],
                'report': report, 'states': self._states(),
            })
        else:
            raise ValueError(f'未知事件类型: {kind}')
        return manifest_counter, member_counter, seal_counter

    def _deliver_late_g2_member(self, result):
        """fixture 中 g2 封包后缺的 a.csv 迟到两天后到达。"""

        g2 = next(p for p in self.receiver.status()['packages']
                  if p['package'] == 'V1/S1/g2')
        if not g2['missing']:
            return
        name = g2['missing'][0]
        payload = self._file_content(name)
        self.deliver_member({'generation': 2}, 'f-late-a',
                            content=payload, generation=2, name=name)
        report = self.receiver.scan_once()
        result['phases'].append({
            'event': None, 'type': 'member', 'name': name,
            'arrival_id': 'f-late-a',
            'note': '迟到两天的 g2 成员，补齐后整包进入 READY',
            'report': report, 'states': self._states(),
        })

    # ---------------------------------------------------------- 崩溃与恢复

    def _crash_phase(self, event, index):
        package_key = 'V1/S1/g2'
        before = self._snapshot_published()
        # 在子进程中触发真实崩溃：staging 已构建、台账 STAGED，但尚未晋升
        code = '\n'.join([
            'import sys',
            'sys.path.insert(0, sys.argv[2])',
            'from pathlib import Path',
            'from collector.receiving.publisher import _Publisher, _CrashBeforePromote',
            'p = _Publisher(Path(sys.argv[1]))',
            'try:',
            f"    p.publish({package_key!r}, crash={event['point']!r})",
            'except _CrashBeforePromote as exc:',
            '    print("CRASHED:", exc)',
            'finally:',
            '    p.close()',
        ])
        completed = subprocess.run(
            [sys.executable, '-I', '-c', code, str(self.root), str(_project_root())],
            cwd=_project_root(), capture_output=True, text=True, check=False,
        )
        crashed = completed.returncode == 0 and 'CRASHED:' in completed.stdout
        after = self._snapshot_published()
        # 崩溃后重新打开 receiver（模拟新进程接班），published 不应出现半包
        self.receiver.close()
        self.receiver = Receiver(self.root)
        status = self.receiver.status()
        g2 = next(p for p in status['packages'] if p['package'] == package_key)
        staging_left = sorted(
            str(p.relative_to(self.root)) for p in (self.root / 'staging').rglob('*')
            if p.is_file()
        )
        return {
            'event': index, 'type': 'crash', 'point': event['point'],
            'subprocess_stdout': completed.stdout.strip(),
            'subprocess_returncode': completed.returncode,
            'crash_observed': crashed,
            'published_before': before,
            'published_after_crash': after,
            'half_package_visible': after != before,
            'g2_state_after_crash': g2['state'],
            'staging_leftovers': staging_left,
        }

    def _snapshot_published(self):
        published = self.root / 'published'
        return sorted(
            str(p.relative_to(self.root))
            for p in published.rglob('*') if p.is_file()
        ) if published.exists() else []

    # ---------------------------------------------------------- 压力情景

    def _extra_scenarios(self):
        extra = []

        # 情景 A：摘要不符——同名 a.csv 但内容被篡改，必须隔离且不覆盖旧件
        good_digest = next(
            m['digest'] for p in self.receiver.status()['packages']
            if p['package'] == 'V1/S1/g2' for m in p['members'] if m['name'] == 'a.csv'
        )
        tampered = b'CORRUPTED-bytes-not-in-any-manifest\n'
        self.deliver_member({'generation': 2}, 'f-bad-digest',
                            content=tampered, generation=2, name='a.csv')
        report_bad = self.receiver.scan_once()
        extra.append({'scenario': '摘要不符的同名成员（隔离，不覆盖）',
                      'good_digest': good_digest,
                      'tampered_digest': __import__('hashlib').sha256(tampered).hexdigest(),
                      'report': report_bad,
                      'quarantine': self.receiver.ledger.list_quarantine()})

        # 情景 C：重复到件——同一 arrival_id 重发，幂等且来源被清理
        payload = self._file_content('a.csv')
        self.deliver_member({'generation': 2}, 'f-late-a',
                            content=payload, generation=2, name='a.csv')
        report_dup = self.receiver.scan_once()
        extra.append({'scenario': '重复到件幂等处理', 'report': report_dup})

        # 情景 D：封包后试图改写同一代际——必须隔离，要求其改用新代际
        envelope, _ = manifest_envelope(
            'm-illegal', 'V1', 'S1', 1, True,
            [{'name': 'a.csv', 'size': 999,
              'digest': '0' * 64}],
        )
        write_arrival(self.receiver.inbox, 'm-illegal', envelope)
        report_sealed_change = self.receiver.scan_once()
        extra.append({'scenario': '封包后改写同代际被拒',
                      'report': report_sealed_change,
                      'quarantine': self.receiver.ledger.list_quarantine()})

        # 情景 E：两个接收进程同时扫描——文件锁串行化，同一份成员不会被搬两次
        concurrency = self._two_processes_concurrency()
        extra.append({'scenario': '两个接收进程并发扫描', **concurrency})

        # 先把已经完整的 g1 / g2 正常发布（g2 的正式发布会在崩溃恢复后完成）
        ready = [p['package'] for p in self.receiver.status()['packages']
                 if p['state'] == 'READY']
        published_now = []
        for key in ready:
            if key != 'V1/S1/g2':
                published_now.append(publish(self.root, key))
        extra.append({'scenario': '完整包先发布', 'published': ready,
                      'published_now': [e['path'] for e in published_now if e]})
        return extra

    def _two_processes_concurrency(self):
        """造一个新包 g3 与一个成员，两个进程同时跑 scan。"""

        files = synthesize_files('g3', ['x.csv'])
        members = [dict(zip(('name', 'size', 'digest'),
                            ('x.csv', *make_member('x.csv', files['x.csv']))))]
        envelope, _ = manifest_envelope('m-g3', 'V1', 'S1', 3, True, members)
        write_arrival(self.receiver.inbox, 'm-g3', envelope)
        write_arrival(
            self.receiver.inbox, 'f-g3-x',
            member_envelope('f-g3-x', 'V1', 'S1', 3, 'x.csv'),
            files['x.csv'],
        )
        code = (
            'import json, sys; sys.path.insert(0, sys.argv[2]); '
            'from pathlib import Path; '
            'from collector.receiving.receiver import Receiver; '
            'r=Receiver(Path(sys.argv[1])); '
            'print(json.dumps(r.scan_once(), ensure_ascii=False)); '
            'r.close()'
        )
        procs = [
            subprocess.Popen(
                [sys.executable, '-I', '-c', code, str(self.root),
                 str(_project_root())],
                cwd=_project_root(), stdout=subprocess.PIPE, text=True,
            )
            for _ in range(2)
        ]
        outputs = [p.communicate()[0].strip() for p in procs]
        parsed = [json.loads(out) for out in outputs if out]
        accepted_ids = sorted(
            item['arrival_id'] for rep in parsed for item in rep.get('accepted', [])
        )
        return {
            'processes': 2,
            'accepted_per_process': accepted_ids,
            'double_processing': len(accepted_ids) != len(set(accepted_ids)),
            'g3_state': next(
                p['state'] for p in self.receiver.status()['packages']
                if p['package'] == 'V1/S1/g3'
            ),
        }

    def _published_tree(self):
        published = self.root / 'published'
        if not published.exists():
            return {}
        tree = {}
        for package_dir in sorted(published.rglob('PACKAGE.json')):
            base = package_dir.parent
            tree[str(base.relative_to(self.root))] = sorted(
                str(p.relative_to(base)) for p in base.rglob('*') if p.is_file()
            )
        return tree


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def run(fixture_path: Path, keep_root: Path | None = None):
    if keep_root is None:
        temp = tempfile.TemporaryDirectory(prefix='voyage-spool-demo-')
        root = Path(temp.name)
        try:
            return _run_with_root(root, fixture_path)
        finally:
            temp.cleanup()
    return _run_with_root(Path(keep_root), fixture_path)


def _run_with_root(root: Path, fixture_path: Path):
    demo = Demo(root, fixture_path)
    try:
        return demo.run()
    finally:
        demo.close()
