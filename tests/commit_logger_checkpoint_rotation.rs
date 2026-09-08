//! `CommitLogger` 在 checkpoint 轮换前必须固定当前 WAL 块归属。
//!
//! 本 target 使用真实多线程 runtime、真实 `CommitLogger/LogFile` 和真实文件系统，并分别
//! 验证 1 worker 与 4 worker。每种 worker 配置运行在独立子进程中，防止 logger 的长期
//! collector 任务跨场景污染资源或时序。核心红线是：事务在旧 checkpoint 注册后，即使尚未
//! 调用 `flush`，轮换也必须先把其 WAL 写入旧文件；后续确认新 checkpoint 不能提前回收旧
//! checkpoint。确认回收还必须跳过没有事务可确认的零长度中间 checkpoint，同时不能让后继
//! 非空 WAL 越过更早的非空未确认 WAL。

use std::{
    env, fs,
    future::Future,
    panic,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    task::{Context, Poll},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use futures::task::noop_waker;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_guid::Guid;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;

const TEST_NAME: &str = "test_checkpoint_rotation_preserves_registered_wal_ownership";
const WORKERS_ENV: &str = "PI_STORE_CHECKPOINT_ROTATION_WORKERS";
const ROOT_ENV: &str = "PI_STORE_CHECKPOINT_ROTATION_ROOT";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(30);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(20);
const PAYLOAD_LEN: usize = 1024;

#[test]
fn test_checkpoint_rotation_preserves_registered_wal_ownership() {
    if let Ok(workers) = env::var(WORKERS_ENV) {
        install_abort_on_any_panic();
        let workers = workers
            .parse::<usize>()
            .expect("checkpoint rotation worker count must be a positive integer");
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("checkpoint rotation child must receive its root path"),
        );
        run_on_runtime(workers, RUNTIME_TIMEOUT, move |rt| async move {
            verify_rotation_matrix(rt, root, workers).await
        })
        .unwrap_or_else(|error| {
            panic!("checkpoint rotation matrix failed with {workers} workers: {error}",)
        });
        return;
    }

    for workers in [1usize, 4usize] {
        let root = unique_temp_root(workers);
        fs::create_dir_all(&root).expect("creating checkpoint rotation evidence root must succeed");
        if let Err(error) = run_worker_process(workers, &root, PROCESS_TIMEOUT) {
            panic!(
                "checkpoint rotation target failed with {workers} workers; evidence is preserved at {:?}: {error}",
                root,
            );
        }
        fs::remove_dir_all(&root).expect("cleaning checkpoint rotation evidence root must succeed");
    }
}

/// runtime worker panic 不会自动让 Rust 测试主线程失败；子进程必须把任意线程 panic 转换为
/// 非零退出，防止文件/runtime 后台异常被业务断言的 `Ok` 遮蔽。
fn install_abort_on_any_panic() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}

async fn verify_rotation_matrix(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    workers: usize,
) -> TestResult<()> {
    verify_empty_rotation(&rt, root.join("empty")).await?;
    verify_flushed_rotation(&rt, root.join("flushed"), Guid(0x1000 + workers as u128)).await?;
    verify_zero_length_checkpoint_does_not_block_confirmation(
        &rt,
        root.join("zero-length-middle"),
        Guid(0x1800 + workers as u128),
        Guid(0x1900 + workers as u128),
    )
    .await?;
    verify_pending_rotation(
        &rt,
        root.join("pending"),
        Guid(0x2000 + workers as u128),
        Guid(0x3000 + workers as u128),
    )
    .await?;
    verify_size_owner_invalidates_old_timer(
        &rt,
        root.join("size-owner-invalidates-timer"),
        Guid(0x3800 + workers as u128),
        Guid(0x3900 + workers as u128),
    )
    .await?;
    verify_cancelled_waiter_does_not_cancel_batch(
        &rt,
        root.join("cancelled-waiter"),
        Guid(0x3a00 + workers as u128),
        Guid(0x3b00 + workers as u128),
    )
    .await?;
    verify_existing_delay_owner_rotation(
        &rt,
        root.join("existing-delay-owner"),
        Guid(0x4000 + workers as u128),
        Guid(0x5000 + workers as u128),
    )
    .await
}

/// 验证丢弃一个尚在等待的 flush Future 不会取消独立运行的定时提交所有者。
///
/// 第一个 flush 经单次轮询后已经创建真实定时任务并登记等待项，但尚未发生 I/O；随后丢弃
/// 这个接收方，再让第二个事务作为活跃等待者加入同一批次。定时任务必须忽略第一个已关闭
/// 接收端，继续写完整批次并唤醒第二个等待者。这里验证的是“等待阶段取消”，不把正在持有
/// 底层裸文件指针执行 I/O 的所有者 Future 任意取消误写成安全合同。
async fn verify_cancelled_waiter_does_not_cancel_batch(
    rt: &MultiTaskRuntime<()>,
    path: PathBuf,
    cancelled_uid: Guid,
    live_uid: Guid,
) -> TestResult<()> {
    const DELAY_TIMEOUT_MS: usize = 10;

    let logger = build_logger_with_delay(rt, &path, DELAY_TIMEOUT_MS).await?;
    rt.timeout(1).await;
    let cancelled_handle = logger
        .append(cancelled_uid.clone(), vec![0xc1; 512])
        .await
        .map_err(|error| format!("appending cancelled-waiter WAL failed: {error}"))?;
    let live_handle = logger
        .append(live_uid.clone(), vec![0xc2; 512])
        .await
        .map_err(|error| format!("appending live-waiter WAL failed: {error}"))?;
    let checkpoint = logger
        .check_point_of(cancelled_uid.clone())
        .await
        .ok_or_else(|| "cancelled waiter omitted checkpoint registration".to_owned())?;
    expect_eq(
        "live waiter checkpoint registration",
        logger.check_point_of(live_uid.clone()).await,
        Some(checkpoint),
    )?;

    let mut cancelled_flush = logger.flush(cancelled_handle);
    let first_poll = {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        cancelled_flush.as_mut().poll(&mut context)
    };
    match first_poll {
        Poll::Pending => {}
        Poll::Ready(result) => {
            return Err(format!(
                "cancelled flush must first become a waiter, observed {result:?}",
            ));
        }
    }
    drop(cancelled_flush);
    expect_state(
        "cancelled waiter batch before timer",
        checkpoint_file_state(&path, checkpoint)?,
        CheckpointFileState::Active(0),
    )?;

    logger.flush(live_handle).await.map_err(|error| {
        format!("live waiter was not completed after peer cancellation: {error}")
    })?;
    require_active_nonempty(
        "timer owner must persist the batch after one receiver is cancelled",
        checkpoint_file_state(&path, checkpoint)?,
    )?;
    logger
        .confirm(live_uid)
        .await
        .map_err(|error| format!("confirming live waiter failed: {error}"))?;
    logger
        .confirm(cancelled_uid)
        .await
        .map_err(|error| format!("confirming cancelled waiter transaction failed: {error}"))?;
    expect_eq(
        "cancelled-waiter append count",
        logger.append_total_count(),
        2,
    )?;
    expect_eq(
        "cancelled-waiter confirm count",
        logger.confirm_total_count(),
        2,
    )?;
    expect_eq(
        "cancelled-waiter remaining registrations",
        logger.waiting_confirm_count().await,
        0,
    )
}

async fn verify_empty_rotation(rt: &MultiTaskRuntime<()>, path: PathBuf) -> TestResult<()> {
    let logger = build_logger(rt, &path).await?;
    let empty_uid = Guid(0xe001);
    let empty_handle = logger
        .append(empty_uid.clone(), Vec::<u8>::new())
        .await
        .map_err(|error| format!("appending empty WAL failed: {error}"))?;
    expect_eq("empty WAL handle", empty_handle, 0)?;
    expect_eq(
        "empty WAL must not register a checkpoint",
        logger.check_point_of(empty_uid.clone()).await,
        None,
    )?;
    logger
        .flush(empty_handle)
        .await
        .map_err(|error| format!("flushing empty WAL failed: {error}"))?;
    logger
        .confirm(empty_uid)
        .await
        .map_err(|error| format!("confirming ignored empty WAL failed: {error}"))?;

    let old_checkpoint = logger
        .current_check_point()
        .await
        .checked_sub(1)
        .ok_or_else(|| "initial checkpoint index underflowed".to_owned())?;
    expect_state(
        "empty checkpoint before rotation",
        checkpoint_file_state(&path, old_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;

    let new_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("rotating empty checkpoint failed: {error}"))?;
    expect_state(
        "empty old checkpoint after rotation",
        checkpoint_file_state(&path, old_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;
    expect_state(
        "empty new checkpoint after rotation",
        checkpoint_file_state(&path, new_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;
    expect_eq("empty append count", logger.append_total_count(), 0)?;
    expect_eq("empty confirm count", logger.confirm_total_count(), 0)?;
    expect_eq(
        "empty waiting count",
        logger.waiting_confirm_count().await,
        0,
    )
}

/// 验证大小所有者（size owner）完成旧批次后，旧延迟定时器只能对原句柄做幂等检查。
///
/// 这个场景不触发检查点轮换：先用超过 2 KiB 块阈值的 WAL 让首次 `flush` 立即成为大小
/// 所有者，再在它创建的延迟定时器到期前追加一个不足阈值的后继 WAL。v0.11.1 的句柄 0
/// 特权会让旧定时器把后继 WAL 写入同一个物理文件；正确实现必须保持文件长度完全不变，
/// 直到后继事务自己的 `flush` 到期。
async fn verify_size_owner_invalidates_old_timer(
    rt: &MultiTaskRuntime<()>,
    path: PathBuf,
    old_commit_uid: Guid,
    new_commit_uid: Guid,
) -> TestResult<()> {
    const BLOCK_LIMIT: usize = 2 * 1024;
    const DELAY_TIMEOUT_MS: usize = 10;
    const OLD_TIMER_SETTLE_TICKS: usize = 3;

    let logger = CommitLoggerBuilder::new(rt.clone(), &path)
        .log_block_limit(BLOCK_LIMIT)
        .delay_timeout(DELAY_TIMEOUT_MS)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building size-owner logger at {path:?} failed: {error}"))?;

    // 对齐到一次粗粒度时钟推进之后再创建旧定时器，使同步、后继 append 和定时器到期之间
    // 留出接近一个完整 tick；被测延迟仍是生产 runtime 的真实定时任务。
    rt.timeout(1).await;
    let old_handle = logger
        .append(old_commit_uid.clone(), vec![0xa1; BLOCK_LIMIT + 256])
        .await
        .map_err(|error| format!("appending size-owner WAL failed: {error}"))?;
    let checkpoint = logger
        .check_point_of(old_commit_uid.clone())
        .await
        .ok_or_else(|| "size-owner WAL omitted checkpoint registration".to_owned())?;
    logger
        .flush(old_handle)
        .await
        .map_err(|error| format!("size-owner flush failed: {error}"))?;
    let size_owner_len = require_active_nonempty(
        "size owner must synchronously persist its oversized block",
        checkpoint_file_state(&path, checkpoint)?,
    )?;

    let new_handle = logger
        .append(new_commit_uid.clone(), vec![0xb1; 512])
        .await
        .map_err(|error| format!("appending size-owner successor failed: {error}"))?;
    if new_handle <= old_handle {
        return Err(format!(
            "size-owner append handles must increase: old={old_handle}, new={new_handle}",
        ));
    }
    expect_eq(
        "size-owner successor checkpoint registration",
        logger.check_point_of(new_commit_uid.clone()).await,
        Some(checkpoint),
    )?;
    expect_state(
        "size-owner successor remains buffered before old timer",
        checkpoint_file_state(&path, checkpoint)?,
        CheckpointFileState::Active(size_owner_len),
    )?;

    for _ in 0..OLD_TIMER_SETTLE_TICKS {
        rt.timeout(DELAY_TIMEOUT_MS + 1).await;
    }
    expect_state(
        "expired size-owner timer must not persist successor WAL",
        checkpoint_file_state(&path, checkpoint)?,
        CheckpointFileState::Active(size_owner_len),
    )?;

    logger
        .flush(new_handle)
        .await
        .map_err(|error| format!("successor's own flush failed: {error}"))?;
    let final_len = require_active_nonempty(
        "successor's own flush must eventually persist its WAL",
        checkpoint_file_state(&path, checkpoint)?,
    )?;
    if final_len <= size_owner_len {
        return Err(format!(
            "successor flush did not grow the WAL: before={size_owner_len}, after={final_len}",
        ));
    }

    logger
        .confirm(new_commit_uid)
        .await
        .map_err(|error| format!("confirming size-owner successor first failed: {error}"))?;
    logger
        .confirm(old_commit_uid)
        .await
        .map_err(|error| format!("confirming size-owner predecessor failed: {error}"))?;
    expect_eq("size-owner append count", logger.append_total_count(), 2)?;
    expect_eq("size-owner confirm count", logger.confirm_total_count(), 2)?;
    expect_eq(
        "size-owner waiting count",
        logger.waiting_confirm_count().await,
        0,
    )?;
    require_backup_nonempty(
        "fully confirmed size-owner checkpoint",
        checkpoint_file_state(&path, checkpoint)?,
    )?;
    Ok(())
}

async fn verify_flushed_rotation(
    rt: &MultiTaskRuntime<()>,
    path: PathBuf,
    commit_uid: Guid,
) -> TestResult<()> {
    let logger = build_logger(rt, &path).await?;
    let handle = logger
        .append(commit_uid.clone(), vec![0x51; PAYLOAD_LEN])
        .await
        .map_err(|error| format!("appending flushed transaction failed: {error}"))?;
    if handle == 0 {
        return Err("non-empty LogFile append must return a nonzero handle".to_owned());
    }
    let old_checkpoint = logger
        .check_point_of(commit_uid.clone())
        .await
        .ok_or_else(|| "flushed transaction omitted checkpoint registration".to_owned())?;
    logger
        .flush(handle)
        .await
        .map_err(|error| format!("flushing transaction before rotation failed: {error}"))?;
    let old_len = require_active_nonempty(
        "flushed old checkpoint before rotation",
        checkpoint_file_state(&path, old_checkpoint)?,
    )?;

    let new_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("rotating flushed checkpoint failed: {error}"))?;
    expect_state(
        "flushed old checkpoint after rotation",
        checkpoint_file_state(&path, old_checkpoint)?,
        CheckpointFileState::Active(old_len),
    )?;
    expect_state(
        "new checkpoint after flushed rotation",
        checkpoint_file_state(&path, new_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;
    logger
        .confirm(commit_uid)
        .await
        .map_err(|error| format!("confirming flushed transaction failed: {error}"))?;
    expect_eq("flushed append count", logger.append_total_count(), 1)?;
    expect_eq("flushed confirm count", logger.confirm_total_count(), 1)?;
    expect_eq(
        "flushed waiting count",
        logger.waiting_confirm_count().await,
        0,
    )?;
    require_backup_nonempty(
        "confirmed flushed checkpoint",
        checkpoint_file_state(&path, old_checkpoint)?,
    )?;
    Ok(())
}

async fn verify_zero_length_checkpoint_does_not_block_confirmation(
    rt: &MultiTaskRuntime<()>,
    path: PathBuf,
    predecessor_uid: Guid,
    successor_uid: Guid,
) -> TestResult<()> {
    let logger = build_logger(rt, &path).await?;
    let predecessor_handle = logger
        .append(predecessor_uid.clone(), vec![0x59; PAYLOAD_LEN])
        .await
        .map_err(|error| format!("appending predecessor WAL failed: {error}"))?;
    logger
        .flush(predecessor_handle)
        .await
        .map_err(|error| format!("flushing predecessor WAL failed: {error}"))?;
    let predecessor_checkpoint = logger
        .check_point_of(predecessor_uid.clone())
        .await
        .ok_or_else(|| "predecessor omitted checkpoint registration".to_owned())?;

    let empty_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("creating empty middle checkpoint failed: {error}"))?;
    let successor_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("rotating empty middle checkpoint failed: {error}"))?;
    expect_state(
        "middle checkpoint must remain physically empty",
        checkpoint_file_state(&path, empty_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;

    let successor_handle = logger
        .append(successor_uid.clone(), vec![0x5a; PAYLOAD_LEN])
        .await
        .map_err(|error| format!("appending successor WAL failed: {error}"))?;
    logger
        .flush(successor_handle)
        .await
        .map_err(|error| format!("flushing successor WAL failed: {error}"))?;
    logger
        .confirm(successor_uid)
        .await
        .map_err(|error| format!("confirming successor WAL first failed: {error}"))?;

    require_active_nonempty(
        "unconfirmed predecessor must remain active",
        checkpoint_file_state(&path, predecessor_checkpoint)?,
    )?;
    expect_state(
        "empty checkpoint cannot pass an unconfirmed predecessor",
        checkpoint_file_state(&path, empty_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;
    require_active_nonempty(
        "confirmed successor cannot pass an unconfirmed predecessor",
        checkpoint_file_state(&path, successor_checkpoint)?,
    )?;

    logger
        .confirm(predecessor_uid)
        .await
        .map_err(|error| format!("confirming predecessor WAL failed: {error}"))?;
    require_backup_nonempty(
        "confirmed predecessor checkpoint",
        checkpoint_file_state(&path, predecessor_checkpoint)?,
    )?;
    expect_state(
        "zero-length checkpoint must not block ordered backup conversion",
        checkpoint_file_state(&path, empty_checkpoint)?,
        CheckpointFileState::Backup(0),
    )?;
    require_backup_nonempty(
        "confirmed successor after zero-length checkpoint",
        checkpoint_file_state(&path, successor_checkpoint)?,
    )?;
    expect_eq("zero-length append count", logger.append_total_count(), 2)?;
    expect_eq("zero-length confirm count", logger.confirm_total_count(), 2)?;
    expect_eq(
        "zero-length waiting count",
        logger.waiting_confirm_count().await,
        0,
    )
}

async fn verify_pending_rotation(
    rt: &MultiTaskRuntime<()>,
    path: PathBuf,
    old_commit_uid: Guid,
    new_commit_uid: Guid,
) -> TestResult<()> {
    let logger = build_logger(rt, &path).await?;
    let old_handle = logger
        .append(old_commit_uid.clone(), vec![0x61; PAYLOAD_LEN])
        .await
        .map_err(|error| format!("appending pending old transaction failed: {error}"))?;
    if old_handle == 0 {
        return Err("old LogFile append handle must be nonzero".to_owned());
    }
    let old_checkpoint = logger
        .check_point_of(old_commit_uid.clone())
        .await
        .ok_or_else(|| "pending old transaction omitted checkpoint registration".to_owned())?;
    expect_state(
        "pending old checkpoint before rotation",
        checkpoint_file_state(&path, old_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;

    // 这是原缺陷的确定性窗口：事务已登记到 old_checkpoint，但原 flush 尚未开始。
    let new_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("rotating pending checkpoint failed: {error}"))?;
    require_active_nonempty(
        "pending WAL must be committed to its registered old checkpoint",
        checkpoint_file_state(&path, old_checkpoint)?,
    )?;
    expect_state(
        "new checkpoint must remain empty immediately after pending rotation",
        checkpoint_file_state(&path, new_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;
    logger
        .flush(old_handle)
        .await
        .map_err(|error| format!("flushing already committed old transaction failed: {error}"))?;

    let new_handle = logger
        .append(new_commit_uid.clone(), vec![0x71; PAYLOAD_LEN])
        .await
        .map_err(|error| format!("appending new checkpoint transaction failed: {error}"))?;
    if new_handle <= old_handle {
        return Err(format!(
            "LogFile append handles must increase: old={old_handle}, new={new_handle}",
        ));
    }
    expect_eq(
        "new transaction checkpoint registration",
        logger.check_point_of(new_commit_uid.clone()).await,
        Some(new_checkpoint),
    )?;
    logger
        .flush(new_handle)
        .await
        .map_err(|error| format!("flushing new checkpoint transaction failed: {error}"))?;
    require_active_nonempty(
        "new transaction WAL must be in the new checkpoint",
        checkpoint_file_state(&path, new_checkpoint)?,
    )?;

    // 后继事务先确认时，前驱 checkpoint 仍有未确认事务，因此两个文件都必须保持 active。
    logger
        .confirm(new_commit_uid.clone())
        .await
        .map_err(|error| format!("confirming new transaction first failed: {error}"))?;
    expect_eq(
        "old transaction remains registered",
        logger.check_point_of(old_commit_uid.clone()).await,
        Some(old_checkpoint),
    )?;
    expect_eq(
        "new transaction registration removed",
        logger.check_point_of(new_commit_uid).await,
        None,
    )?;
    require_active_nonempty(
        "unconfirmed old checkpoint after successor confirmation",
        checkpoint_file_state(&path, old_checkpoint)?,
    )?;
    require_active_nonempty(
        "confirmed successor cannot pass unconfirmed predecessor",
        checkpoint_file_state(&path, new_checkpoint)?,
    )?;
    expect_eq(
        "pending waiting count after successor confirmation",
        logger.waiting_confirm_count().await,
        1,
    )?;

    logger
        .confirm(old_commit_uid)
        .await
        .map_err(|error| format!("confirming old transaction failed: {error}"))?;
    expect_eq("pending append count", logger.append_total_count(), 2)?;
    expect_eq("pending confirm count", logger.confirm_total_count(), 2)?;
    expect_eq(
        "pending waiting count after all confirmations",
        logger.waiting_confirm_count().await,
        0,
    )?;
    require_backup_nonempty(
        "confirmed old checkpoint",
        checkpoint_file_state(&path, old_checkpoint)?,
    )?;
    require_backup_nonempty(
        "confirmed successor checkpoint",
        checkpoint_file_state(&path, new_checkpoint)?,
    )?;
    Ok(())
}

async fn verify_existing_delay_owner_rotation(
    rt: &MultiTaskRuntime<()>,
    path: PathBuf,
    old_commit_uid: Guid,
    new_commit_uid: Guid,
) -> TestResult<()> {
    const DELAY_TIMEOUT_MS: usize = 10;
    const OLD_TIMER_SETTLE_TICKS: usize = 3;

    let logger = build_logger_with_delay(rt, &path, DELAY_TIMEOUT_MS).await?;
    // 本测试目标的全局时钟以 1000 ms 推进。先等待一个时钟刻度（tick），再立即建立 10 ms
    // 定时器，下一次时钟推进前就有确定性的充足窗口完成辅助提交、分裂和后继追加；这只控制
    // 测试调度，不修改生产 delay_timeout 或检查点实现。
    rt.timeout(1).await;
    let old_handle = logger
        .append(old_commit_uid.clone(), vec![0x81; PAYLOAD_LEN])
        .await
        .map_err(|error| format!("appending existing-owner old transaction failed: {error}"))?;
    if old_handle == 0 {
        return Err("existing-owner old handle must be nonzero".to_owned());
    }
    let old_checkpoint = logger
        .check_point_of(old_commit_uid.clone())
        .await
        .ok_or_else(|| {
            "existing-owner old transaction omitted checkpoint registration".to_owned()
        })?;

    // 单次 poll 必须把旧 flush 推进到：已取得延迟提交所有权（delay ownership）、已派发
    // 定时任务、已登记等待者（waiter）。这里故意使用空操作唤醒器，而不是当前 runtime
    // 任务的唤醒器。检查点随后会向等待者
    // 通道发送结果；若遗留当前任务的旧唤醒器，它可能在本段单次探测已经结束后再次调度同一
    // 外层 future，某些多 worker 运行时会因此并发 poll 同一任务。空操作唤醒器仍让真实
    // async-channel receiver 完成登记，但发送方只更新通道状态；后面对 old_flush 的正常
    // await 会重新 poll，并直接取得已就绪结果。
    let mut old_flush = logger.flush(old_handle);
    let first_poll = {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        old_flush.as_mut().poll(&mut context)
    };
    match first_poll {
        Poll::Pending => {}
        Poll::Ready(result) => {
            return Err(format!(
                "existing-owner old flush must be pending before rotation, observed {result:?}",
            ));
        }
    }
    expect_state(
        "existing-owner old checkpoint before rotation",
        checkpoint_file_state(&path, old_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;

    let new_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("rotating with an existing delay owner failed: {error}"))?;
    require_active_nonempty(
        "existing-owner helper must commit old WAL before rotation",
        checkpoint_file_state(&path, old_checkpoint)?,
    )?;
    expect_state(
        "existing-owner new checkpoint starts empty",
        checkpoint_file_state(&path, new_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;
    old_flush
        .await
        .map_err(|error| format!("existing-owner old waiter was not completed: {error}"))?;

    let new_handle = logger
        .append(new_commit_uid.clone(), vec![0x91; PAYLOAD_LEN])
        .await
        .map_err(|error| format!("appending existing-owner successor failed: {error}"))?;
    if new_handle <= old_handle {
        return Err(format!(
            "existing-owner append handles must increase: old={old_handle}, new={new_handle}",
        ));
    }
    expect_eq(
        "existing-owner successor registration",
        logger.check_point_of(new_commit_uid.clone()).await,
        Some(new_checkpoint),
    )?;
    expect_state(
        "existing-owner successor remains buffered before old timer",
        checkpoint_file_state(&path, new_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;

    // 检查点辅助提交已经持久化到 old_handle，因此旧定时器所属的原批次已经结束。定时器
    // 即使稍后才到期，也只能凭原始 old_handle 检查已提交水位并退出，绝不能把新检查点的
    // 当前块（current）当作自己的批次强制同步。这里连续等待三个 1000 ms 全局时钟推进周期：
    // 第一个周期使 10 ms 旧定时器到期，后两个周期给它充足机会取得提交锁并完整退出。测试使用
    // 真实运行时、真实 CommitLogger、真实文件和真实同步写；粗粒度时钟只固定先后顺序，不替换
    // 任何生产组件。
    for _ in 0..OLD_TIMER_SETTLE_TICKS {
        rt.timeout(DELAY_TIMEOUT_MS + 1).await;
    }
    expect_state(
        "expired predecessor timer must not commit successor checkpoint",
        checkpoint_file_state(&path, new_checkpoint)?,
        CheckpointFileState::Active(0),
    )?;

    // 由 successor 自己的合法 flush 负责最终活性。该调用既证明旧定时器无副作用不会导致
    // 数据丢失，也证明恢复原始句柄约束后，新批次仍能由自己的定时窗口正常落盘。
    logger
        .flush(new_handle)
        .await
        .map_err(|error| format!("flushing successor with its own delay window failed: {error}"))?;

    logger
        .confirm(new_commit_uid)
        .await
        .map_err(|error| format!("confirming existing-owner successor first failed: {error}"))?;
    expect_eq(
        "existing-owner predecessor remains registered",
        logger.check_point_of(old_commit_uid.clone()).await,
        Some(old_checkpoint),
    )?;
    logger
        .confirm(old_commit_uid)
        .await
        .map_err(|error| format!("confirming existing-owner predecessor failed: {error}"))?;
    expect_eq(
        "existing-owner append count",
        logger.append_total_count(),
        2,
    )?;
    expect_eq(
        "existing-owner confirm count",
        logger.confirm_total_count(),
        2,
    )?;
    expect_eq(
        "existing-owner waiting count",
        logger.waiting_confirm_count().await,
        0,
    )?;
    require_backup_nonempty(
        "existing-owner confirmed predecessor checkpoint",
        checkpoint_file_state(&path, old_checkpoint)?,
    )?;
    require_backup_nonempty(
        "existing-owner confirmed successor checkpoint",
        checkpoint_file_state(&path, new_checkpoint)?,
    )?;
    Ok(())
}

async fn build_logger(rt: &MultiTaskRuntime<()>, path: &Path) -> TestResult<CommitLogger> {
    build_logger_with_delay(rt, path, 10).await
}

async fn build_logger_with_delay(
    rt: &MultiTaskRuntime<()>,
    path: &Path,
    delay_timeout: usize,
) -> TestResult<CommitLogger> {
    CommitLoggerBuilder::new(rt.clone(), path)
        .delay_timeout(delay_timeout)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building real commit logger at {path:?} failed: {error}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckpointFileState {
    Active(u64),
    Backup(u64),
    Missing,
}

fn checkpoint_file_state(wal_path: &Path, checkpoint: usize) -> TestResult<CheckpointFileState> {
    let active = wal_path.join(format!("{checkpoint:09}"));
    let backup = active.with_extension("bak");
    let active_exists = active.is_file();
    let backup_exists = backup.is_file();
    if active_exists && backup_exists {
        return Err(format!(
            "checkpoint {checkpoint} exists as both active and backup files",
        ));
    }
    if active_exists {
        return fs::metadata(&active)
            .map(|metadata| CheckpointFileState::Active(metadata.len()))
            .map_err(|error| format!("reading active checkpoint {active:?} failed: {error}"));
    }
    if backup_exists {
        return fs::metadata(&backup)
            .map(|metadata| CheckpointFileState::Backup(metadata.len()))
            .map_err(|error| format!("reading backup checkpoint {backup:?} failed: {error}"));
    }
    Ok(CheckpointFileState::Missing)
}

fn require_active_nonempty(label: &str, state: CheckpointFileState) -> TestResult<u64> {
    match state {
        CheckpointFileState::Active(len) if len > 0 => Ok(len),
        observed => Err(format!(
            "{label}: expected Active(len > 0), observed {observed:?}",
        )),
    }
}

fn require_backup_nonempty(label: &str, state: CheckpointFileState) -> TestResult<u64> {
    match state {
        CheckpointFileState::Backup(len) if len > 0 => Ok(len),
        observed => Err(format!(
            "{label}: expected Backup(len > 0), observed {observed:?}",
        )),
    }
}

fn expect_state(
    label: &str,
    actual: CheckpointFileState,
    expected: CheckpointFileState,
) -> TestResult<()> {
    expect_eq(label, actual, expected)
}

fn expect_eq<T: std::fmt::Debug + PartialEq>(
    label: &str,
    actual: T,
    expected: T,
) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {expected:?}, observed {actual:?}",
        ))
    }
}

fn run_on_runtime<T, F, Fut>(workers: usize, timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    // 较粗的测试时钟配合 existing-delay-owner 场景确定性冻结旧 timer；每个 worker 配置都
    // 运行在独立子进程，不会影响其它 target 或生产全局时钟配置。
    let _time_loop = startup_global_time_loop(1000);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(workers)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning checkpoint rotation target failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("checkpoint rotation target exceeded {timeout:?}: {error}"))?
}

fn run_worker_process(workers: usize, root: &Path, timeout: Duration) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating checkpoint rotation executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(WORKERS_ENV, workers.to_string())
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| {
            format!("spawning checkpoint rotation process with {workers} workers failed: {error}")
        })?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "checkpoint rotation process with {workers} workers exited with {status}",
        ))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking checkpoint rotation child failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("checkpoint rotation child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn unique_temp_root(workers: usize) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_store_checkpoint_rotation_{workers}w_{}_{}",
        std::process::id(),
        nanos,
    ))
}
