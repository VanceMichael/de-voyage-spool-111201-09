# 本地运行说明（分批接收服务）

先运行单元测试，再执行演示。所有命令从项目根目录发起，仅依赖 Python 3.11 标准库，
不需要网络；演示中的崩溃环节在独立子进程中真实退出，再由恢复命令对账。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector receive-demo --keep-root var/demo-spool
```

## 日常运行

1. 船方把到件单元（`envelope.json`，成员另带 `payload.bin`）拷入 `inbox/`。
2. 定时或手动执行 `receive-scan --spool <根目录>`；可任意重复，扫描是幂等且
   可恢复的。两个接收进程同时运行时，文件锁让它们串行，同一份成员只处理一次。
3. 用 `receive-status` 区分四种状态，只有 `READY` 才允许进入发布；
   `SEALED_INCOMPLETE` 表示清单已封包但成员未齐，缺什么由 `missing` 列出。
4. `receive-publish` 构建并原子晋升整包；崩溃后执行 `receive-recover`，
   它会重建 `staging/` 残片、核对 `published/` 与台账后补完晋升。

## 故障与证据

- 大小、摘要或归属不符的到件不会被静默丢弃：整体进入 `quarantine/<id>/`，
  原因写在 `reason.txt`，台账 `quarantine` 表同步登记。
- 源文件清理只发生在对象 fsync 落盘且台账事务提交之后；处理中途崩溃时，
  到件留在 `inbox/.processing/`，下一轮扫描自动重新处理。
- 封包后试图改写同一代际会被隔离，原因中明确要求改用新代际；新代际包的
  `supersedes` 字段与 `PACKAGE.json` 保留可追溯关系，旧代际正式目录不删除。
- 发布是整包边界：`staging/` 中的残片对分析组不可见；`published/` 下每个目录
  都带 `PACKAGE.json`，成员以硬链接指向内容寻址仓库并在发布时逐个复核摘要。
- 需要长期保留证据时备份整个 spool 根目录（含 `ledger.sqlite3` 与 `objects/`）。

## 旧版命令

`demo / import / show / serve` 仍由 `python3 -m collector <command>` 提供，
针对完整包的一次性校验入库，与分批接收服务互不影响。
