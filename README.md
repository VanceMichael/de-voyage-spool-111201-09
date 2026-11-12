# 离线航次数据包·分批接收

面向调查船不稳定回传链路的接收服务：清单往往先到、成员乱序并可能重复到达、
重传包还混着上一航次的同名文件。服务以「航次 / 站位 / 包代际 / 清单摘要」
标识每一次交付，路径名只作显示，杜绝接收员靠移动文件夹判断“收齐”。

## 核心规则

- **身份不依赖路径名**：交付身份是航次+站位+代际+清单规范摘要（sha256）。
  成员字节进入内容寻址仓库 `objects/ab/<digest>`，同名不同内容各自独立存放，
  绝不按名字覆盖旧件。
- **封包才确定成员集合**：未封包清单可修改（保留全部草稿修订）；封包冻结成员集；
  封包后再改同一代际一律隔离，更正必须产生新代际，`supersedes` 记录血缘。
- **乱序与重复**：成员可早于/晚于清单到达，先暂存、封包时按冻结集合追认或隔离；
  字节一致的重发幂等忽略并保留原接收凭据，字节不同的同名件隔离。
- **不符即隔离**：大小、摘要或信封归属不符的到件整体移入 `quarantine/`，
  附 `reason.txt`，不污染正式目录。
- **可恢复的搬运**：扫描先经一次原子 `rename` 把到件认领到 `inbox/.processing/`，
  两个接收进程靠 `fcntl` 锁串行化，同一份成员不会被各搬一次；对象 fsync 落盘、
  SQLite 事务提交之后才删除来源目录。
- **整包发布边界**：发布在 `staging/` 构建并复核整包，再同文件系统原子 `rename`
  晋升到 `published/`。磁盘满或中途停止只会留下 staging 残片，分析组要么看不到、
  要么看到完整包，`receive-recover` 负责对账。
- **四态可分辨**：`AWAITING_SEAL`（清单未封包）、`SEALED_INCOMPLETE`（封包缺成员）、
  `READY`（完整待发布）、`PUBLISHED`（正式可用）。

## 安装与运行

Python 3.11，仅标准库，SQLite 随解释器提供：

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector receive-demo
```

`receive-demo` 按 `fixtures/deliveries.json` 的到件时间表，用合成小文件演示：
失序到件与封包前暂存、封包后更正产生新代际、发布前崩溃（独立子进程真实退出）
与对账恢复，并附加摘要不符隔离、重复到件、双进程并发三个压力情景。
默认使用临时目录并在结束后清理；加 `--keep-root <dir>` 可保留现场检查。

## 接收服务命令

```bash
# 一轮扫描：认领到件、校验、隔离或确认（可重复执行，崩溃后重跑即恢复）
python3 -m collector receive-scan   --spool var/spool

# 查看四态、成员缺口、暂存与隔离区
python3 -m collector receive-status --spool var/spool

# 整包发布（缺省发布所有 READY 包；--crash 仅用于演练崩溃）
python3 -m collector receive-publish --spool var/spool --package V1/S1/g2
python3 -m collector receive-publish --spool var/spool --crash before_promote

# 发布崩溃后的对账恢复：重建 staging 残片并完成晋升
python3 -m collector receive-recover --spool var/spool
```

## spool 目录布局

```
inbox/                 船方拷入到件的接收目录，每个到件一个子目录
  .processing/         已被原子认领、正在处理或等待崩溃恢复的到件
objects/ab/<digest>    内容寻址仓库，fsync 落盘，同名异内容共存
quarantine/<id>/       隔离区：原信封、载荷与 reason.txt
staging/               发布前整包临时构建区（半包不进入 published）
published/<voyage>/<station>/<generation>/   正式可用，仅由整包 rename 产生
  PACKAGE.json         发布描述符：身份、清单摘要、血缘、成员凭据
ledger.sqlite3         台账：包、清单修订、到件凭据、隔离与发布事件
.locks/                扫描锁与发布锁
```

到件单元格式：`inbox/<arrival_id>/envelope.json`（成员到件另带 `payload.bin`）。
信封声明航次/站位/代际归属；`fixtures/deliveries.json` 只描述事件时间线，
合成文件与信封由 `collector/receiving/synthetic.py` 生成。

## 旧版采集组件（保留）

`collector/domain.py`、`storage.py`、`transport.py`、`api.py` 仍支持完整包的
凭据校验与 SQLite 入库，对应 `demo / import / show / serve` 子命令，行为不变。
