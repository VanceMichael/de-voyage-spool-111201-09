"""陆续到件的航次数据包接收服务。

模块划分：
- model:     封包清单、到达信封、规范摘要与接收状态的领域模型
- ledger:    SQLite 台账，所有接收事实的持久凭据
- fsstore:   内容寻址对象仓库与目录布局
- receiver:  接收目录扫描、原子认领、隔离与状态推进
- publisher: 整包原子发布与崩溃对账
- synthetic: 合成小文件与信封的构造工具（演示/测试用）
- demo:      按 fixtures/deliveries.json 时间线演示的叙事脚本
"""

from .ledger import Ledger
from .model import (
    ArrivalKind,
    ArrivalStatus,
    Envelope,
    Manifest,
    MemberSpec,
    PackageState,
    canonical_bytes,
    manifest_digest,
    parse_envelope,
    parse_manifest,
)
from .publisher import publish, publish_all_ready, recover
from .receiver import Receiver

__all__ = [
    'ArrivalKind',
    'ArrivalStatus',
    'Envelope',
    'Ledger',
    'Manifest',
    'MemberSpec',
    'PackageState',
    'Receiver',
    'canonical_bytes',
    'manifest_digest',
    'parse_envelope',
    'parse_manifest',
    'publish',
    'publish_all_ready',
    'recover',
]
