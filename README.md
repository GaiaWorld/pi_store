# Container & Set

The code is described in the accompanying blog post: http://zmythleo.github.com/rust.html

## Building

## CommitLogger checkpoint 轮换

`CommitLogger` 的事务登记和物理 WAL 文件必须保持同一 checkpoint 身份。轮换可写文件前，
logger 会先在 checkpoint 映射锁内把非空 current 块完整提交到旧 writable，再执行 split 并
发布新 checkpoint；事务已经 append、但延迟 flush 尚未执行时，其 WAL 仍落入登记时的旧文件。

`LogFile::append` 返回的合法日志句柄从 1 开始。句柄 0 只供 crate 内部表示“强制提交调用时的
current 块”，调用方不得自行构造。公开 `LogFile::commit/delay_commit` 的签名和自动分裂语义
不变；`delay_commit` 成功只表示 WAL 块写入并唤醒 waiter，不保证自动 split 已经完成。需要
观察物理文件拓扑的维护方必须等待显式 `split` 或 `append_check_point` 返回。

确认扫描会把零长度只读 checkpoint 视为无需事务确认，但 `.bak` 只按队首连续前缀推进。因此
零长度中间文件不会永久阻塞后继文件，也不能让后继非空已确认 WAL 越过更早的非空未确认 WAL。

该修复不改变 WAL 编码、文件命名、replay、确认计数或公开 API。非空 checkpoint 轮换会等待
一次原本必须完成的 WAL sync；普通 append/flush 热路径只增加已提交句柄的 relaxed 原子快路。
`tests/commit_logger_checkpoint_rotation.rs` 使用真实 1/4-worker runtime、真实文件系统验证空、
已 flush、pending、已有延迟 owner 和零长度中间 checkpoint；`commit_logger` 局部测试同时保护
物理大小阈值只分裂一次及公开 `LogFile` 的兼容行为。

## Licence

This code is free for you to use under the MIT licence.
