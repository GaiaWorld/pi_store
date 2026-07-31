//! `CommitLogger` 在 checkpoint 轮换前必须固定当前 WAL 块归属。
//!
//! 本 target 使用真实多线程 runtime、真实 `CommitLogger/LogFile` 和真实文件系统，并分别
//! 验证 1 worker 与 4 worker。每种 worker 配置运行在独立子进程中，防止 logger 的长期
//! collector 任务跨场景污染资源或时序。核心红线是：事务在旧 checkpoint 注册后，即使尚未
//! 调用 `flush`，轮换也必须先把其 WAL 写入旧文件；后续确认新 checkpoint 不能提前回收旧
//! checkpoint。确认回收还必须跳过没有事务可确认的零长度中间 checkpoint，同时不能让后继
//! 非空 WAL 越过更早的非空未确认 WAL。

use std::{
    env,
    fs,
    future::{poll_fn, Future},
    panic,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    task::Poll,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
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
            env::var_os(ROOT_ENV)
                .expect("checkpoint rotation child must receive its root path"),
        );
        run_on_runtime(workers, RUNTIME_TIMEOUT, move |rt| async move {
            verify_rotation_matrix(rt, root, workers).await
        })
        .unwrap_or_else(|error| {
            panic!(
                "checkpoint rotation matrix failed with {workers} workers: {error}",
            )
        });
        return;
    }

    for workers in [1usize, 4usize] {
        let root = unique_temp_root(workers);
        fs::create_dir_all(&root)
            .expect("creating checkpoint rotation evidence root must succeed");
        if let Err(error) = run_worker_process(workers, &root, PROCESS_TIMEOUT) {
            panic!(
                "checkpoint rotation target failed with {workers} workers; evidence is preserved at {:?}: {error}",
                root,
            );
        }
        fs::remove_dir_all(&root)
            .expect("cleaning checkpoint rotation evidence root must succeed");
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
    verify_flushed_rotation(
        &rt,
        root.join("flushed"),
        Guid(0x1000 + workers as u128),
    )
    .await?;
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
    verify_existing_delay_owner_rotation(
        &rt,
        root.join("existing-delay-owner"),
        Guid(0x4000 + workers as u128),
        Guid(0x5000 + workers as u128),
    )
    .await
}

async fn verify_empty_rotation(
    rt: &MultiTaskRuntime<()>,
    path: PathBuf,
) -> TestResult<()> {
    let logger = build_logger(rt, &path).await?;
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
        return Err("LogFile append handle 0 is reserved for crate-internal control".to_owned());
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
    const TIMER_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(5);

    let logger = build_logger_with_delay(rt, &path, DELAY_TIMEOUT_MS).await?;
    // 本 target 的全局时钟以 1000ms 推进。先等待一个 tick，再立即建立 10ms timer，下一次
    // 时钟推进前就有确定性的充足窗口完成 helper、split 和 successor append；这只控制测试
    // 调度，不修改生产 delay_timeout 或 checkpoint 实现。
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
        .ok_or_else(|| "existing-owner old transaction omitted checkpoint registration".to_owned())?;

    // 单次 poll 必须把旧 flush 推进到：已取得 delay owner、已派发定时任务、已登记 waiter。
    // future 保持存活但暂不继续 poll，使后续 checkpoint helper 必须负责提交旧块并唤醒它。
    let mut old_flush = logger.flush(old_handle);
    let first_poll = poll_fn(|cx| Poll::Ready(old_flush.as_mut().poll(cx))).await;
    match first_poll {
        Poll::Pending => {},
        Poll::Ready(result) => {
            return Err(format!(
                "existing-owner old flush must be pending before rotation, observed {result:?}",
            ));
        },
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

    // checkpoint helper 已推进 old_handle。旧实现若仍用该句柄执行定时 commit，会命中
    // committed 快路并遗留 successor waiter；保留句柄0必须让旧 timer 刷新届时的 current。
    let deadline = Instant::now() + TIMER_OBSERVATION_TIMEOUT;
    loop {
        match checkpoint_file_state(&path, new_checkpoint)? {
            CheckpointFileState::Active(len) if len > 0 => break,
            CheckpointFileState::Active(0) if Instant::now() < deadline => {
                rt.timeout(1).await;
            },
            observed => {
                return Err(format!(
                    "existing delay timer did not commit successor WAL before {TIMER_OBSERVATION_TIMEOUT:?}, observed {observed:?}",
                ));
            },
        }
    }
    logger
        .flush(new_handle)
        .await
        .map_err(|error| format!("flushing timer-committed successor failed: {error}"))?;

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
    expect_eq("existing-owner append count", logger.append_total_count(), 2)?;
    expect_eq("existing-owner confirm count", logger.confirm_total_count(), 2)?;
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

async fn build_logger(
    rt: &MultiTaskRuntime<()>,
    path: &Path,
) -> TestResult<CommitLogger> {
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

fn checkpoint_file_state(
    wal_path: &Path,
    checkpoint: usize,
) -> TestResult<CheckpointFileState> {
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

fn require_active_nonempty(
    label: &str,
    state: CheckpointFileState,
) -> TestResult<u64> {
    match state {
        CheckpointFileState::Active(len) if len > 0 => Ok(len),
        observed => Err(format!(
            "{label}: expected Active(len > 0), observed {observed:?}",
        )),
    }
}

fn require_backup_nonempty(
    label: &str,
    state: CheckpointFileState,
) -> TestResult<u64> {
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

fn run_on_runtime<T, F, Fut>(
    workers: usize,
    timeout: Duration,
    build: F,
) -> TestResult<T>
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

fn run_worker_process(
    workers: usize,
    root: &Path,
    timeout: Duration,
) -> TestResult<()> {
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
            .map_err(|error| format!("checking checkpoint rotation child failed: {error}"))? {
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
