import argparse
import json
from pathlib import Path
import tempfile

from .domain import collect
from .storage import Store
from .transport import Client, replay_server


def _print_json(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _cmd_demo(arguments):
    with tempfile.TemporaryDirectory() as directory:
        payload = Path(arguments.input).read_bytes()
        with replay_server({'/source': [dict(body=payload.decode('utf-8'))]}) as (base, calls):
            status, headers, raw = Client().request(base + '/source')
            if status != 200:
                raise RuntimeError('本地来源未返回成功响应')
            store = Store(Path(directory) / 'demo.sqlite3')
            rows = collect(json.loads(raw))
            digest = store.ingest('本地演示来源', raw, rows)
            _print_json(dict(digest=digest, records=store.records(), requests=len(calls)))
            store.close()


def _cmd_import(arguments):
    store = Store(arguments.db)
    try:
        raw = Path(arguments.input).read_bytes()
        store.ingest(str(Path(arguments.input)), raw, collect(json.loads(raw)))
        _print_json(store.records())
    finally:
        store.close()


def _cmd_show(arguments):
    store = Store(arguments.db)
    try:
        _print_json(store.records())
    finally:
        store.close()


def _cmd_serve(arguments):
    from .api import serve

    serve(arguments.db, arguments.port)


def _cmd_receive_scan(arguments):
    from .receiving.receiver import Receiver

    receiver = Receiver(Path(arguments.spool))
    try:
        _print_json(receiver.scan_once())
    finally:
        receiver.close()


def _cmd_receive_status(arguments):
    from .receiving.receiver import Receiver

    receiver = Receiver(Path(arguments.spool))
    try:
        _print_json(receiver.status())
    finally:
        receiver.close()


def _cmd_receive_publish(arguments):
    from .receiving.receiver import Receiver
    from .receiving.publisher import publish, publish_all_ready

    if arguments.package is None:
        _print_json(publish_all_ready(Path(arguments.spool)))
    else:
        _print_json(publish(Path(arguments.spool), arguments.package,
                            crash=arguments.crash))


def _cmd_receive_recover(arguments):
    from .receiving.publisher import recover

    _print_json(recover(Path(arguments.spool)))


def _cmd_receive_demo(arguments):
    from .receiving.demo import run

    keep = Path(arguments.keep_root) if arguments.keep_root else None
    _print_json(run(Path(arguments.fixture), keep_root=keep))


def build_parser():
    parser = argparse.ArgumentParser(description='本地采集与分批到件接收服务')
    sub = parser.add_subparsers(dest='command', required=True)

    legacy = sub.add_parser('demo', help='旧版采集演示（回环 HTTP + SQLite 入库）')
    legacy.add_argument('--input', default='fixtures/sample.json')
    legacy.set_defaults(func=_cmd_demo)

    legacy_import = sub.add_parser('import', help='旧版样本导入')
    legacy_import.add_argument('--input', default='fixtures/sample.json')
    legacy_import.add_argument('--db', default='var/records.sqlite3')
    legacy_import.set_defaults(func=_cmd_import)

    legacy_show = sub.add_parser('show', help='查看旧版台账记录')
    legacy_show.add_argument('--db', default='var/records.sqlite3')
    legacy_show.set_defaults(func=_cmd_show)

    legacy_serve = sub.add_parser('serve', help='旧版只读查询服务')
    legacy_serve.add_argument('--db', default='var/records.sqlite3')
    legacy_serve.add_argument('--port', type=int, default=8080)
    legacy_serve.set_defaults(func=_cmd_serve)

    scan = sub.add_parser('receive-scan', help='扫描接收目录一轮（可重复、可恢复）')
    scan.add_argument('--spool', required=True)
    scan.set_defaults(func=_cmd_receive_scan)

    status = sub.add_parser('receive-status', help='查看四态、缺口与隔离区')
    status.add_argument('--spool', required=True)
    status.set_defaults(func=_cmd_receive_status)

    publish_cmd = sub.add_parser('receive-publish', help='整包原子发布')
    publish_cmd.add_argument('--spool', required=True)
    publish_cmd.add_argument('--package', default=None,
                             help='缺省时发布所有 READY 包')
    publish_cmd.add_argument('--crash', choices=['before_promote'], default=None,
                             help='演示用：在晋升前模拟崩溃')
    publish_cmd.set_defaults(func=_cmd_receive_publish)

    recover_cmd = sub.add_parser('receive-recover', help='发布崩溃后的对账恢复')
    recover_cmd.add_argument('--spool', required=True)
    recover_cmd.set_defaults(func=_cmd_receive_recover)

    receive_demo = sub.add_parser(
        'receive-demo', help='按 deliveries.json 演示失序到件/封包更正/崩溃恢复')
    receive_demo.add_argument('--fixture', default='fixtures/deliveries.json')
    receive_demo.add_argument('--keep-root', default=None,
                              help='保留 spool 目录以便事后检查')
    receive_demo.set_defaults(func=_cmd_receive_demo)
    return parser


def main():
    arguments = build_parser().parse_args()
    arguments.func(arguments)


if __name__ == '__main__':
    main()
