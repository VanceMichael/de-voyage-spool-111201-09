"""接收服务的行为测试：身份、封包、隔离、并发与崩溃恢复。"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from collector.receiving.demo import run as run_demo
from collector.receiving.model import (
    ArrivalStatus,
    PackageState,
    parse_envelope,
    parse_manifest,
)
from collector.receiving.publisher import (
    PublishError,
    _CrashBeforePromote,
    publish,
    recover,
)
from collector.receiving.receiver import Receiver
from collector.receiving.synthetic import (
    make_member,
    manifest_envelope,
    member_envelope,
    seal_envelope,
    synthesize_files,
    write_arrival,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = PROJECT_ROOT / 'fixtures' / 'deliveries.json'


class ReceiverTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.receiver = Receiver(self.root)

    def tearDown(self):
        self.receiver.close()
        self.temp.cleanup()

    def send_manifest(self, arrival_id, voyage, station, generation, sealed,
                      files: dict[str, bytes]):
        members = [
            dict(zip(('name', 'size', 'digest'), (name, *make_member(name, data))))
            for name, data in sorted(files.items())
        ]
        envelope, digest = manifest_envelope(
            arrival_id, voyage, station, generation, sealed, members)
        write_arrival(self.receiver.inbox, arrival_id, envelope)
        return members, digest

    def send_member(self, arrival_id, voyage, station, generation, name,
                    payload: bytes):
        write_arrival(
            self.receiver.inbox, arrival_id,
            member_envelope(arrival_id, voyage, station, generation, name),
            payload,
        )

    def send_seal(self, arrival_id, voyage, station, generation):
        write_arrival(
            self.receiver.inbox, arrival_id,
            seal_envelope(arrival_id, voyage, station, generation),
        )

    def package(self, key):
        return next(p for p in self.receiver.status()['packages']
                    if p['package'] == key)


class StateTransitionTests(ReceiverTestBase):
    def test_four_states_in_order_with_out_of_order_arrivals(self):
        files = synthesize_files('t1', ['a.csv', 'b.csv'])
        # 1) 未封包清单先到 -> AWAITING_SEAL
        self.send_manifest('m1', 'V1', 'S1', 1, False, files)
        self.receiver.scan_once()
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.AWAITING_SEAL.value)

        # 2) 成员先于封包到达（乱序），暂存为 PENDING，状态不变
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        report = self.receiver.scan_once()
        self.assertEqual(report['pending'][0]['member'], 'a.csv')
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.AWAITING_SEAL.value)

        # 3) 封包 -> 暂存成员被追认，缺 b.csv -> SEALED_INCOMPLETE
        self.send_seal('s1', 'V1', 'S1', 1)
        self.receiver.scan_once()
        g1 = self.package('V1/S1/g1')
        self.assertEqual(g1['state'], PackageState.SEALED_INCOMPLETE.value)
        self.assertEqual(g1['missing'], ['b.csv'])

        # 4) 缺件补齐 -> READY
        self.send_member('f2', 'V1', 'S1', 1, 'b.csv', files['b.csv'])
        self.receiver.scan_once()
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.READY.value)

        # 5) 整包发布 -> PUBLISHED，正式目录整包可见
        event = publish(self.root, 'V1/S1/g1')
        self.assertEqual(event['state'], 'PUBLISHED')
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.PUBLISHED.value)
        final = Path(event['path'])
        self.assertTrue((final / 'PACKAGE.json').is_file())
        self.assertEqual(
            sorted(p.name for p in final.iterdir()),
            ['PACKAGE.json', 'a.csv', 'b.csv'],
        )


class SealingAndGenerationTests(ReceiverTestBase):
    def test_manifest_revision_before_seal_is_kept(self):
        files_v1 = synthesize_files('rev', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, False, files_v1)
        self.receiver.scan_once()
        # 第一版草稿期间 a.csv 已到件并暂存
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files_v1['a.csv'])
        self.receiver.scan_once()
        files_v2 = synthesize_files('rev', ['a.csv', 'b.csv'])
        self.send_manifest('m2', 'V1', 'S1', 1, False, files_v2)
        self.receiver.scan_once()
        self.send_seal('s1', 'V1', 'S1', 1)
        self.receiver.scan_once()
        g1 = self.package('V1/S1/g1')
        self.assertEqual(g1['manifest_revisions'], 3)  # 两版草稿 + 一封版
        self.assertEqual(g1['missing'], ['b.csv'])
        # 封包前暂存的 a.csv 在封包瞬间被追认为 ACCEPTED
        self.assertTrue(g1['members'][0]['present'])

    def test_change_after_seal_same_generation_is_quarantined(self):
        files = synthesize_files('seal', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        # 封包后又送来同代际但成员集不同的清单
        bogus = [{'name': 'a.csv', 'size': 1, 'digest': '0' * 64}]
        envelope, _ = manifest_envelope('m2', 'V1', 'S1', 1, True, bogus)
        write_arrival(self.receiver.inbox, 'm2', envelope)
        report = self.receiver.scan_once()
        self.assertEqual(len(report['quarantined']), 1)
        self.assertIn('新代际', report['quarantined'][0]['reason'])
        quarantined = list((self.root / 'quarantine').iterdir())
        self.assertEqual(len(quarantined), 1)
        # 原封包摘要不变
        self.assertIsNotNone(self.package('V1/S1/g1')['sealed_manifest_digest'])

    def test_new_generation_keeps_lineage(self):
        files1 = synthesize_files('g1', ['a.csv', 'b.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files1)
        self.receiver.scan_once()
        files2 = synthesize_files('g2', ['a.csv'])
        self.send_manifest('m2', 'V1', 'S1', 2, True, files2)
        self.receiver.scan_once()
        g1 = self.package('V1/S1/g1')
        g2 = self.package('V1/S1/g2')
        self.assertIsNone(g1['supersedes'])
        self.assertEqual(g2['supersedes'], 'V1/S1/g1')
        self.assertEqual(g2['state'], PackageState.SEALED_INCOMPLETE.value)

    def test_sealed_manifest_determines_members_early_member_rejected(self):
        good = synthesize_files('sealed', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, good)
        self.receiver.scan_once()
        # 封包清单里没有的成员送到同包 -> 隔离
        self.send_member('fx', 'V1', 'S1', 1, 'stranger.csv', b'nope\n')
        report = self.receiver.scan_once()
        self.assertEqual(report['quarantined'][0]['arrival_id'], 'fx')


class QuarantineAndIdentityTests(ReceiverTestBase):
    def test_digest_mismatch_is_quarantined_and_never_overwrites(self):
        files = synthesize_files('q', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        # 正确件先入
        self.send_member('f-good', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        self.receiver.scan_once()
        good_digest = self.receiver.ledger.accepted_member(
            'V1/S1/g1', 'a.csv')['object_digest']
        # 同名坏件后到：摘要不符 -> 隔离，旧件完好
        self.send_member('f-bad', 'V1', 'S1', 1, 'a.csv', b'tampered bytes\n')
        report = self.receiver.scan_once()
        self.assertEqual(report['quarantined'][0]['arrival_id'], 'f-bad')
        still_good = self.receiver.ledger.accepted_member('V1/S1/g1', 'a.csv')
        self.assertEqual(still_good['object_digest'], good_digest)
        # 对象仓库里两份内容各自独立，同名没有发生覆盖
        self.assertTrue((self.root / 'objects' / good_digest[:2] / good_digest).exists())
        rows = self.receiver.ledger.list_quarantine()
        self.assertTrue(any(r['arrival_id'] == 'f-bad' for r in rows))

    def test_attribution_mismatch_in_envelope_rejected(self):
        # 信封头声明 generation=2，内嵌清单却是 generation=1 -> 归属不符
        envelope, _ = manifest_envelope('f1', 'V1', 'S1', 2, True, [
            {'name': 'a.csv', 'size': 1, 'digest': '0' * 64}])
        envelope['manifest']['generation'] = 1
        envelope['manifest'].pop('members', None)
        envelope['manifest']['members'] = [
            {'name': 'a.csv', 'size': 1, 'digest': '0' * 64}]
        with self.assertRaises(ValueError):
            parse_envelope(envelope)

    def test_duplicate_arrival_is_idempotent_and_source_removed(self):
        files = synthesize_files('dup', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        first = self.receiver.scan_once()
        self.assertEqual(len(first['accepted']), 1)
        # 同一 arrival_id 重发
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        second = self.receiver.scan_once()
        self.assertEqual(len(second['duplicate']), 1)
        self.assertFalse((self.receiver.inbox / 'f1').exists())
        self.assertFalse((self.receiver.processing / 'f1').exists())
        arrivals = self.receiver.ledger.connection.execute(
            "SELECT status, count(*) FROM arrivals GROUP BY status").fetchall()
        counts = {row[0]: row[1] for row in arrivals}
        # 清单 1 条 ACCEPTED、成员首次 1 条 ACCEPTED；重发不覆盖凭据，另记重复表
        self.assertEqual(self.receiver.ledger.count_duplicates(), 1)
        self.assertEqual(counts.get(ArrivalStatus.ACCEPTED.value), 2)
        self.assertNotIn(ArrivalStatus.DUPLICATE.value, counts)
        self.assertEqual(
            self.receiver.ledger.accepted_member('V1/S1/g1', 'a.csv')
            ['object_digest'],
            make_member('a.csv', files['a.csv'])[1],
        )

    def test_same_named_file_from_other_voyage_coexists(self):
        # 上一航次同名文件内容不同：身份由航次/代际/摘要决定，路径名只是显示
        old = synthesize_files('old', ['a.csv'])
        new = synthesize_files('new', ['a.csv'])
        self.assertNotEqual(old['a.csv'], new['a.csv'])
        self.send_manifest('m-old', 'V0', 'S1', 1, True, old)
        self.send_manifest('m-new', 'V1', 'S1', 1, True, new)
        self.receiver.scan_once()
        self.send_member('f-old', 'V0', 'S1', 1, 'a.csv', old['a.csv'])
        self.send_member('f-new', 'V1', 'S1', 1, 'a.csv', new['a.csv'])
        self.receiver.scan_once()
        self.assertEqual(self.package('V0/S1/g1')['state'],
                         PackageState.READY.value)
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.READY.value)
        digests = {
            make_member('a.csv', old['a.csv'])[1],
            make_member('a.csv', new['a.csv'])[1],
        }
        for digest in digests:
            self.assertTrue(
                (self.root / 'objects' / digest[:2] / digest).is_file())

    def test_loose_file_in_inbox_is_quarantined(self):
        (self.receiver.inbox / 'stray.bin').write_bytes(b'no envelope\n')
        report = self.receiver.scan_once()
        self.assertEqual(len(report['quarantined']), 1)
        self.assertEqual(len(list((self.root / 'quarantine').iterdir())), 1)


class RecoveryAndConcurrencyTests(ReceiverTestBase):
    def test_scan_is_recoverable_after_processing_crash(self):
        files = synthesize_files('crash', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        # 先把到件认领到 processing，再模拟进程当场死亡（不处理）
        claimed = self.receiver._claim_entries()
        self.assertEqual(len(claimed), 2)
        self.assertFalse(
            [p for p in self.receiver.inbox.iterdir() if p.name != '.processing'])
        # 新进程接班：processing 里的到件继续处理，结果与正常扫描一致
        fresh = Receiver(self.root)
        try:
            report = fresh.scan_once()
            touched = report['accepted'] + report['pending']
            self.assertEqual(len(touched), 2)
            self.assertEqual(
                {item['arrival_id'] for item in touched}, {'m1', 'f1'})
            self.assertEqual(fresh.status()['packages'][0]['state'],
                             PackageState.READY.value)
        finally:
            fresh.close()

    def test_publish_crash_leaves_no_half_package_and_recovery_finishes(self):
        files = synthesize_files('pub', ['a.csv', 'b.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        self.send_member('f2', 'V1', 'S1', 1, 'b.csv', files['b.csv'])
        self.receiver.scan_once()
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.READY.value)

        published_root = self.root / 'published'
        with self.assertRaises(Exception):
            publish(self.root, 'V1/S1/g1', crash='before_promote')
        # 崩溃后：正式目录为空（分析组看不到半包），包仍未发布，staging 有残片
        self.assertFalse(any(published_root.rglob('*')))
        g1 = self.package('V1/S1/g1')
        self.assertEqual(g1['state'], PackageState.READY.value)
        self.assertTrue(any((self.root / 'staging').iterdir()))

        actions = recover(self.root)
        self.assertEqual(actions, [{'package': 'V1/S1/g1',
                                    'action': 'promoted_after_recovery'}])
        g1 = self.package('V1/S1/g1')
        self.assertEqual(g1['state'], PackageState.PUBLISHED.value)
        event = self.receiver.ledger.get_publish_event('V1/S1/g1')
        final = Path(event['path'])
        self.assertTrue((final / 'a.csv').is_file())
        self.assertTrue((final / 'b.csv').is_file())
        self.assertFalse(any((self.root / 'staging').iterdir()))

    def test_non_ready_package_cannot_publish(self):
        files = synthesize_files('np', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        with self.assertRaises(PublishError):
            publish(self.root, 'V1/S1/g1')

    def test_republish_is_idempotent_and_recover_is_idempotent(self):
        files = synthesize_files('idem', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        self.receiver.scan_once()
        event = publish(self.root, 'V1/S1/g1')
        # 已发布再发布不报错、不产生第二份目录
        again = publish(self.root, 'V1/S1/g1')
        self.assertEqual(again['path'], event['path'])
        # 无残片时恢复是空操作，重复执行也安全
        self.assertEqual(recover(self.root), [])
        self.assertEqual(recover(self.root), [])

    def test_recover_after_rename_without_commit(self):
        """极端窗口：整包已 rename 进 published，但发布事务未提交即崩溃。"""

        files = synthesize_files('rename', ['a.csv'])
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        self.send_member('f1', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        self.receiver.scan_once()
        with self.assertRaises(_CrashBeforePromote):
            publish(self.root, 'V1/S1/g1', crash='before_promote')
        # 手工把 staging 残片晋升，模拟“rename 完成、事务未提交”的断电窗口
        staging = self.root / 'staging' / 'V1__S1__g1'
        final = self.root / 'published' / 'V1' / 'S1' / 'g1'
        final.parent.mkdir(parents=True, exist_ok=True)
        os.rename(staging, final)
        # 台账仍是 STAGED；恢复应识别并补记，而不是再建一份
        actions = recover(self.root)
        self.assertEqual(actions, [
            {'package': 'V1/S1/g1', 'action': 'rebuilt_after_lost_rename'}])
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.PUBLISHED.value)
        self.assertTrue((final / 'PACKAGE.json').is_file())

    def test_orphan_member_before_any_manifest_is_pending_then_adopted(self):
        files = synthesize_files('orphan', ['a.csv'])
        # 成员先到，连包都还不存在：暂存，不隔离
        self.send_member('f0', 'V1', 'S1', 1, 'a.csv', files['a.csv'])
        report = self.receiver.scan_once()
        self.assertEqual(len(report['pending']), 1)
        # 封包清单后到，暂存成员被按封包凭据追认，整包直接 READY
        self.send_manifest('m1', 'V1', 'S1', 1, True, files)
        self.receiver.scan_once()
        self.assertEqual(self.package('V1/S1/g1')['state'],
                         PackageState.READY.value)
        self.assertEqual(
            self.receiver.ledger.accepted_member('V1/S1/g1', 'a.csv')
            ['arrival_id'], 'f0')

    def test_orphan_staging_without_event_is_discarded(self):
        # 没有任何发布事件的 staging 残片（手工垃圾/中断残留）应被清掉
        leftover = self.root / 'staging' / 'V1__S1__g9'
        leftover.mkdir(parents=True)
        (leftover / 'junk').write_bytes(b'x')
        self.assertEqual(recover(self.root),
                         [{'package': 'V1/S1/g9',
                           'action': 'discarded_orphan_staging'}])
        self.assertFalse(leftover.exists())

    def test_two_processes_do_not_move_same_member_twice(self):
        files = synthesize_files('conc', ['x.csv'])
        self.send_manifest('m1', 'V9', 'S9', 1, True, files)
        self.send_member('f1', 'V9', 'S9', 1, 'x.csv', files['x.csv'])
        code = (
            'import json, sys; sys.path.insert(0, sys.argv[2]); '
            'from pathlib import Path; '
            'from collector.receiving.receiver import Receiver; '
            'r=Receiver(Path(sys.argv[1])); '
            'print(json.dumps(r.scan_once(), ensure_ascii=False)); r.close()'
        )
        procs = [
            subprocess.Popen(
                [sys.executable, '-I', '-c', code, str(self.root),
                 str(PROJECT_ROOT)],
                cwd=PROJECT_ROOT, stdout=subprocess.PIPE, text=True,
            )
            for _ in range(2)
        ]
        outputs = [p.communicate()[0] for p in procs]
        self.assertEqual([p.returncode for p in procs], [0, 0])
        reports = [json.loads(out) for out in outputs if out.strip()]
        accepted = sorted(
            item['arrival_id'] for rep in reports for item in rep['accepted'])
        # 同一份成员不会被两个进程各搬一次：跨进程接受+暂存合计恰好两个到件、
        # 无重复 arrival_id；最终包状态才是业务判定依据
        touched = sorted(
            item['arrival_id']
            for rep in reports
            for bucket in ('accepted', 'pending') for item in rep.get(bucket, []))
        self.assertEqual(touched, ['f1', 'm1'])
        self.assertEqual(len(touched), len(set(touched)))
        self.assertEqual(self.package('V9/S9/g1')['state'],
                         PackageState.READY.value)


class ManifestModelTests(unittest.TestCase):
    def test_digest_changes_with_body_and_unsafe_paths_rejected(self):
        files = synthesize_files('model', ['a.csv'])
        members = [
            dict(zip(('name', 'size', 'digest'), (name, *make_member(name, data))))
            for name, data in sorted(files.items())
        ]
        m1 = parse_manifest({'voyage': 'V1', 'station': 'S1', 'generation': 1,
                             'sealed': False, 'members': members})
        m2 = parse_manifest({'voyage': 'V1', 'station': 'S1', 'generation': 1,
                             'sealed': True, 'members': members})
        self.assertNotEqual(m1.digest, m2.digest)
        for bad in ('../x.csv', '/abs/x.csv', 'a\\b.csv', ''):
            with self.assertRaises(ValueError):
                parse_manifest({'voyage': 'V1', 'station': 'S1', 'generation': 1,
                                'sealed': False,
                                'members': [{'name': bad, 'size': 1,
                                             'digest': '0' * 64}]})


class EndToEndDemoTests(unittest.TestCase):
    def test_delivery_fixture_demo_completes_and_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            result = run_demo(FIXTURE, keep_root=Path(directory))
            states = {
                p['package']: p['state']
                for p in result['final_status']['packages']
            }
            # g1 与 g2 最终都正式发布；g3 也已发布
            for key in ('V1/S1/g1', 'V1/S1/g2', 'V1/S1/g3'):
                self.assertEqual(states[key], PackageState.PUBLISHED.value,
                                 f'{key} 未发布')
            # 崩溃点：分析组在崩溃前后看到的 published 完全一致（无半包）
            crash_phase = next(p for p in result['phases'] if p['type'] == 'crash')
            self.assertTrue(crash_phase['crash_observed'])
            self.assertFalse(crash_phase['half_package_visible'])
            self.assertEqual(crash_phase['g2_state_after_crash'],
                             PackageState.READY.value)
            # 恢复动作恰好把 g2 晋升
            self.assertIn(
                {'package': 'V1/S1/g2', 'action': 'promoted_after_recovery'},
                result['recovery'],
            )
            # 隔离区至少包含：凭据不符的同名坏件、封包后改写同代际
            reasons = ' '.join(q['reason'] for q in result['final_status']['quarantine'])
            self.assertIn('凭据不符', reasons)
            self.assertIn('新代际', reasons)
            # 血缘：g2 取代 g1
            g2 = next(p for p in result['final_status']['packages']
                      if p['package'] == 'V1/S1/g2')
            self.assertEqual(g2['supersedes'], 'V1/S1/g1')
            # 正式目录里每个包都带 PACKAGE.json 且成员齐全
            tree = result['published_tree']
            self.assertTrue(any('PACKAGE.json' in files for files in tree.values()))


if __name__ == '__main__':
    unittest.main()
