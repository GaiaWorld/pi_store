//! 根 WAL 性能基准专用的有界原始事件采集器。
//!
//! 本模块只在显式 `benchmark-telemetry` feature 下存在。它用于比较单进程直接调用、
//! 同进程服务调用和跨进程 IPC 调用是否改变 [`crate::commit_logger::CommitLogger`] 的
//! flush 到达节奏、批次所有者、提交锁等待/持有、同步写和 waiter 恢复调度。
//!
//! 采集器不属于生产监控 API：每个进程只允许启动一次，容量在测量前固定，写满后只增加
//! dropped counter，绝不等待、扩容或改变业务结果。热路径使用 `try_send`，锁内禁止日志；
//! 调用方必须在全部业务 Future 和延迟 timer 收口后停止采集，并把 dropped=0 作为证据门禁。

use std::sync::{
    OnceLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};

const MAX_EVENT_CAPACITY: usize = 4_000_000;

/// 编译当前 telemetry 实现时实际使用的 `pi_store` 包版本。
///
/// 二维 A/B 基准会从不同 tag 构建相互隔离的可执行文件；运行期把本常量写入证据可防止
/// 工作目录中的 `cargo tree` 与可执行文件来源不一致。它只在显式 benchmark feature 下暴露。
pub const PI_STORE_BENCHMARK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// WAL commit 调用者分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalBenchmarkTrigger {
    /// 普通 `delay_commit` 请求；可能成为 size owner 或登记为 waiter。
    Request,
    /// 延迟提交窗口到期后的 timer owner。
    Timer,
    /// `CommitLogger` 检查点轮换前提交当前块。
    Checkpoint,
    /// 直接调用公开 `LogFile::commit` 的路径。
    Public,
}

/// 一条 WAL 基准事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalBenchmarkEvent {
    Append {
        log_uid: u64,
        commit_uid: [u8; 16],
        payload_bytes: u64,
    },
    FlushEnter {
        log_uid: u64,
    },
    ConfirmEnter {
        commit_uid: [u8; 16],
    },
    ConfirmDone {
        commit_uid: [u8; 16],
        removed: bool,
        remaining: u64,
    },
    TimerScheduled {
        log_uid: u64,
        timeout_ms: u64,
    },
    TimerFired {
        log_uid: u64,
    },
    LockAcquired {
        log_uid: u64,
        trigger: WalBenchmarkTrigger,
        wait_ns: u64,
        queued_waiters: u64,
        committed_uid: u64,
    },
    LockReleased {
        log_uid: u64,
        trigger: WalBenchmarkTrigger,
        hold_ns: u64,
    },
    FastPath {
        log_uid: u64,
        trigger: WalBenchmarkTrigger,
        committed_uid: u64,
    },
    EmptyBatch {
        requested_log_uid: u64,
        trigger: WalBenchmarkTrigger,
        committed_uid: u64,
    },
    WaiterRegistered {
        waiter_id: u64,
        log_uid: u64,
        queued_waiters: u64,
    },
    WaiterNotified {
        waiter_id: u64,
        success: bool,
    },
    WaiterResumed {
        waiter_id: u64,
        log_uid: u64,
        wait_ns: u64,
        success: bool,
    },
    SyncBegin {
        log_uid: u64,
        requested_log_uid: u64,
        trigger: WalBenchmarkTrigger,
        block_bytes: u64,
        queued_waiters: u64,
        previous_committed_uid: u64,
    },
    SyncEnd {
        log_uid: u64,
        requested_log_uid: u64,
        trigger: WalBenchmarkTrigger,
        block_bytes: u64,
        written_bytes: u64,
        elapsed_ns: u64,
        success: bool,
    },
}

/// 带进程内单调时间和全局顺序的一条记录。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBenchmarkRecord {
    pub sequence: u64,
    pub at_ns: u64,
    pub logger_id: u64,
    pub event: WalBenchmarkEvent,
}

/// 停止采集后取得的不可变快照。
#[derive(Clone, Debug)]
pub struct WalBenchmarkSnapshot {
    pub capacity: usize,
    pub dropped_events: u64,
    pub records: Vec<WalBenchmarkRecord>,
}

/// 不停止采集即可读取的事务闭环进度。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalBenchmarkProgress {
    pub appended: u64,
    pub confirmed: u64,
    pub timers_scheduled: u64,
    pub timers_fired: u64,
    pub emitted_events: u64,
    pub dropped_events: u64,
}

struct Collector {
    capacity: usize,
    sender: Sender<WalBenchmarkRecord>,
    receiver: Receiver<WalBenchmarkRecord>,
    started: AtomicBool,
    active: AtomicBool,
    writers: AtomicUsize,
    sequence: AtomicU64,
    dropped: AtomicU64,
    waiter_id: AtomicU64,
    appended: AtomicU64,
    confirmed: AtomicU64,
    timers_scheduled: AtomicU64,
    timers_fired: AtomicU64,
}

impl Collector {
    fn new(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        Self {
            capacity,
            sender,
            receiver,
            started: AtomicBool::new(false),
            active: AtomicBool::new(false),
            writers: AtomicUsize::new(0),
            sequence: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            waiter_id: AtomicU64::new(1),
            appended: AtomicU64::new(0),
            confirmed: AtomicU64::new(0),
            timers_scheduled: AtomicU64::new(0),
            timers_fired: AtomicU64::new(0),
        }
    }

    fn emit(&self, logger_id: u64, event: WalBenchmarkEvent) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        self.writers.fetch_add(1, Ordering::AcqRel);
        if !self.active.load(Ordering::Acquire) {
            self.writers.fetch_sub(1, Ordering::AcqRel);
            return;
        }
        match &event {
            WalBenchmarkEvent::Append { .. } => {
                self.appended.fetch_add(1, Ordering::Relaxed);
            }
            WalBenchmarkEvent::ConfirmDone { removed: true, .. } => {
                self.confirmed.fetch_add(1, Ordering::Relaxed);
            }
            WalBenchmarkEvent::TimerScheduled { .. } => {
                self.timers_scheduled.fetch_add(1, Ordering::Relaxed);
            }
            WalBenchmarkEvent::TimerFired { .. } => {
                self.timers_fired.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let record = WalBenchmarkRecord {
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            // `pi_time 0.5` 是 DB 进程内的共享单调轴；服务层 telemetry 使用同一函数，
            // 因而可以按 commit UID 关联 handler/prepare/commit 与根 WAL，而无需跨进程时钟。
            at_ns: pi_time::run_nanos(),
            logger_id,
            event,
        };
        if self.sender.try_send(record).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.writers.fetch_sub(1, Ordering::AcqRel);
    }
}

static COLLECTOR: OnceLock<Collector> = OnceLock::new();
static LOGGER_ID: AtomicU64 = AtomicU64::new(1);

/// 安装并启动当前进程唯一的一次 WAL 基准采集。
///
/// `capacity` 是进程内原始事件硬上限，合法范围为 `1..=4_000_000`。该函数必须在计量
/// barrier 前调用；同一进程重复启动会返回错误，避免两个 profile 混入同一单调时间轴。
pub fn start_wal_benchmark_telemetry(capacity: usize) -> Result<(), String> {
    if capacity == 0 || capacity > MAX_EVENT_CAPACITY {
        return Err(format!(
            "WAL benchmark telemetry capacity must be in 1..={MAX_EVENT_CAPACITY}, got {capacity}",
        ));
    }
    let collector = COLLECTOR.get_or_init(|| Collector::new(capacity));
    if collector.capacity != capacity {
        return Err(format!(
            "WAL benchmark telemetry was installed with capacity {}, requested {capacity}",
            collector.capacity,
        ));
    }
    if collector.started.swap(true, Ordering::AcqRel) {
        return Err("WAL benchmark telemetry can start only once per process".to_owned());
    }
    collector.active.store(true, Ordering::Release);
    Ok(())
}

/// 返回当前采集会话的 append/confirm 闭环进度，不停止或排空事件。
pub fn wal_benchmark_progress() -> Result<WalBenchmarkProgress, String> {
    let collector = COLLECTOR
        .get()
        .ok_or_else(|| "WAL benchmark telemetry was not started".to_owned())?;
    if !collector.active.load(Ordering::Acquire) {
        return Err("WAL benchmark telemetry is not active".to_owned());
    }
    Ok(WalBenchmarkProgress {
        appended: collector.appended.load(Ordering::Acquire),
        confirmed: collector.confirmed.load(Ordering::Acquire),
        timers_scheduled: collector.timers_scheduled.load(Ordering::Acquire),
        timers_fired: collector.timers_fired.load(Ordering::Acquire),
        emitted_events: collector.sequence.load(Ordering::Acquire),
        dropped_events: collector.dropped.load(Ordering::Acquire),
    })
}

/// 停止采集并排空当前进程快照。
///
/// 本函数先关闭准入，再等待已经进入 `emit` 的无阻塞写者离开，保证返回后不会有晚到事件。
/// 调用方仍负责先等待业务 Future、waiter 和 delay timer 收口；否则缺失的是调用方停止时机，
/// 不是采集器取消了底层任务。
pub fn finish_wal_benchmark_telemetry() -> Result<WalBenchmarkSnapshot, String> {
    let collector = COLLECTOR
        .get()
        .ok_or_else(|| "WAL benchmark telemetry was not started".to_owned())?;
    if !collector.active.swap(false, Ordering::AcqRel) {
        return Err("WAL benchmark telemetry is not active".to_owned());
    }
    while collector.writers.load(Ordering::Acquire) != 0 {
        std::thread::yield_now();
    }
    let mut records = Vec::with_capacity(collector.receiver.len());
    loop {
        match collector.receiver.try_recv() {
            Ok(record) => records.push(record),
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                return Err("WAL benchmark telemetry channel disconnected".to_owned());
            }
        }
    }
    records.sort_unstable_by_key(|record| record.sequence);
    Ok(WalBenchmarkSnapshot {
        capacity: collector.capacity,
        dropped_events: collector.dropped.load(Ordering::Acquire),
        records,
    })
}

pub(crate) fn emit_wal_benchmark_event(logger_id: u64, event: WalBenchmarkEvent) {
    if let Some(collector) = COLLECTOR.get() {
        collector.emit(logger_id, event);
    }
}

pub(crate) fn next_wal_benchmark_logger_id() -> u64 {
    LOGGER_ID.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn next_wal_benchmark_waiter_id() -> u64 {
    COLLECTOR
        .get()
        .filter(|collector| collector.active.load(Ordering::Acquire))
        .map(|collector| collector.waiter_id.fetch_add(1, Ordering::Relaxed))
        .unwrap_or(0)
}

pub(crate) fn duration_ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::sync::mpsc::sync_channel;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use pi_async_rt::rt::{
        AsyncRuntime, multi_thread::MultiTaskRuntimeBuilder, startup_global_time_loop,
    };
    use pi_async_transaction::AsyncCommitLog;
    use pi_guid::Guid;

    use super::*;
    use crate::commit_logger::CommitLoggerBuilder;

    #[test]
    fn test_wal_benchmark_collector_observes_real_flush_without_changing_result() {
        let _time_loop = startup_global_time_loop(1);
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(2)
            .build();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "pi_store_wal_benchmark_telemetry_{}_{}",
            std::process::id(),
            nonce,
        ));
        let (build_sender, build_receiver) = sync_channel(1);
        let build_rt = rt.clone();
        let build_root = root.clone();
        rt.spawn(async move {
            let result = CommitLoggerBuilder::new(build_rt, build_root)
                .log_file_limit(512 * 1024 * 1024)
                .collect_interval(5 * 60 * 1000)
                .build()
                .await;
            let _ = build_sender.send(result);
        })
        .expect("real CommitLogger build must be scheduled");
        let logger = build_receiver
            .recv_timeout(Duration::from_secs(20))
            .expect("real CommitLogger build must complete")
            .expect("real CommitLogger must build");

        start_wal_benchmark_telemetry(512).expect("collector must start once");
        let mut workers = Vec::new();
        for worker in 0..4u64 {
            workers.push(thread::spawn(move || {
                for index in 0..100u64 {
                    emit_wal_benchmark_event(
                        worker + 10,
                        WalBenchmarkEvent::FlushEnter {
                            log_uid: worker * 100 + index + 10_000,
                        },
                    );
                }
            }));
        }
        for worker in workers {
            worker.join().expect("collector worker must not panic");
        }

        let operation_logger = logger.clone();
        let operation_rt = rt.clone();
        let (operation_sender, operation_receiver) = sync_channel(1);
        rt.spawn(async move {
            let uid = Guid(0x4455);
            let handle = operation_logger
                .append(uid.clone(), vec![0x5a; 128])
                .await
                .expect("telemetry sample append must succeed");
            operation_logger
                .flush(handle)
                .await
                .expect("telemetry sample flush must succeed");
            operation_logger
                .confirm(uid)
                .await
                .expect("telemetry sample confirm must succeed");
            operation_rt.timeout(20).await;
            let _ = operation_sender.send(handle);
        })
        .expect("real WAL operation must be scheduled");
        let handle = operation_receiver
            .recv_timeout(Duration::from_secs(20))
            .expect("real WAL operation must complete");
        assert_ne!(handle, 0);
        assert_eq!(logger.append_total_count(), 1);
        assert_eq!(logger.confirm_total_count(), 1);
        let progress =
            wal_benchmark_progress().expect("active collector progress must be readable");
        assert_eq!(progress.appended, 1);
        assert_eq!(progress.confirmed, 1);
        assert_eq!(progress.timers_scheduled, 1);
        assert_eq!(progress.timers_fired, 1);
        assert_eq!(progress.dropped_events, 0);
        assert!(progress.emitted_events > 400);

        let snapshot = finish_wal_benchmark_telemetry().expect("collector must finish");
        assert_eq!(snapshot.capacity, 512);
        assert_eq!(snapshot.dropped_events, 0);
        assert!(snapshot.records.len() > 400);
        let sequences: BTreeSet<_> = snapshot
            .records
            .iter()
            .map(|record| record.sequence)
            .collect();
        assert_eq!(sequences.len(), snapshot.records.len());
        assert_eq!(sequences.first(), Some(&0));
        assert_eq!(
            sequences.last(),
            Some(&((snapshot.records.len() - 1) as u64)),
        );
        assert!(
            snapshot
                .records
                .windows(2)
                .all(|window| window[0].sequence < window[1].sequence)
        );

        for expected in [
            "append",
            "flush_enter",
            "confirm_enter",
            "confirm_done",
            "timer_scheduled",
            "timer_fired",
            "lock_acquired",
            "lock_released",
            "waiter_registered",
            "waiter_notified",
            "waiter_resumed",
            "sync_begin",
            "sync_end",
        ] {
            assert!(
                snapshot
                    .records
                    .iter()
                    .any(|record| match (&record.event, expected) {
                        (WalBenchmarkEvent::Append { .. }, "append")
                        | (WalBenchmarkEvent::FlushEnter { .. }, "flush_enter")
                        | (WalBenchmarkEvent::ConfirmEnter { .. }, "confirm_enter")
                        | (WalBenchmarkEvent::ConfirmDone { .. }, "confirm_done")
                        | (WalBenchmarkEvent::TimerScheduled { .. }, "timer_scheduled")
                        | (WalBenchmarkEvent::TimerFired { .. }, "timer_fired")
                        | (WalBenchmarkEvent::LockAcquired { .. }, "lock_acquired")
                        | (WalBenchmarkEvent::LockReleased { .. }, "lock_released")
                        | (WalBenchmarkEvent::WaiterRegistered { .. }, "waiter_registered")
                        | (WalBenchmarkEvent::WaiterNotified { .. }, "waiter_notified")
                        | (WalBenchmarkEvent::WaiterResumed { .. }, "waiter_resumed")
                        | (WalBenchmarkEvent::SyncBegin { .. }, "sync_begin")
                        | (WalBenchmarkEvent::SyncEnd { .. }, "sync_end") => true,
                        _ => false,
                    }),
                "real WAL path omitted {expected}",
            );
        }
        let timer_scheduled = snapshot
            .records
            .iter()
            .filter(|record| matches!(&record.event, WalBenchmarkEvent::TimerScheduled { .. }))
            .count();
        let timer_fired = snapshot
            .records
            .iter()
            .filter(|record| matches!(&record.event, WalBenchmarkEvent::TimerFired { .. }))
            .count();
        let timer_outcomes = snapshot
            .records
            .iter()
            .filter(|record| {
                matches!(
                    &record.event,
                    WalBenchmarkEvent::FastPath {
                        trigger: WalBenchmarkTrigger::Timer,
                        ..
                    } | WalBenchmarkEvent::EmptyBatch {
                        trigger: WalBenchmarkTrigger::Timer,
                        ..
                    } | WalBenchmarkEvent::SyncEnd {
                        trigger: WalBenchmarkTrigger::Timer,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(timer_scheduled, 1);
        assert_eq!(timer_fired, 1);
        assert_eq!(timer_outcomes, timer_fired);
        let requested_log_uid = snapshot
            .records
            .iter()
            .find_map(|record| match &record.event {
                WalBenchmarkEvent::SyncEnd {
                    trigger: WalBenchmarkTrigger::Timer,
                    requested_log_uid,
                    success: true,
                    ..
                } => Some(*requested_log_uid),
                _ => None,
            })
            .expect("the real delayed flush must perform one successful timer sync");
        let expected_requested_uid = if PI_STORE_BENCHMARK_VERSION == "0.11.1" {
            0
        } else {
            handle as u64
        };
        assert_eq!(requested_log_uid, expected_requested_uid);
        assert!(start_wal_benchmark_telemetry(512).is_err());
        assert!(finish_wal_benchmark_telemetry().is_err());

        drop(logger);
        fs::remove_dir_all(&root).expect("telemetry test directory must be removed");
    }
}
