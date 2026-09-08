//! WAL 提交、检查点和重播的真实错误与取消边界专项。
//!
//! 本目标只验证现有可观察语义，不借故障注入改变生产实现：同步失败使用 Linux/Unix 的
//! `/dev/full`，分裂失败通过把测试专属日志目录临时替换为普通文件制造，取消场景则只丢弃
//! 正在等待检查点辅助任务的外层 Future。每个场景均运行在独立子进程和独立目录中；父进程
//! 严格串行等待并设置硬截止，防止后台运行时任务或故障文件句柄污染下一项证据。

use std::{
    env, fs,
    future::Future,
    io::{Error, ErrorKind},
    panic,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use futures::{executor::block_on, task::noop_waker};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_guid::Guid;
use pi_store::{
    commit_logger::{CommitLogger, CommitLoggerBuilder},
    log_store::log_file::{LogFile, LogMethod},
};

type TestResult<T = ()> = Result<T, String>;

const TEST_NAME: &str = "test_wal_commit_error_and_cancellation_boundaries";
const PHASE_ENV: &str = "PI_STORE_WAL_ERROR_PHASE";
const ROOT_ENV: &str = "PI_STORE_WAL_ERROR_ROOT";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(20);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug)]
enum Phase {
    InvalidRoot,
    SplitFailure,
    AutoSplitFailure,
    CheckpointHelperCancellation,
    ReplayCallbackFailure,
    #[cfg(unix)]
    SyncFailure,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::InvalidRoot => "invalid-root",
            Self::SplitFailure => "split-failure",
            Self::AutoSplitFailure => "auto-split-failure",
            Self::CheckpointHelperCancellation => "checkpoint-helper-cancellation",
            Self::ReplayCallbackFailure => "replay-callback-failure",
            #[cfg(unix)]
            Self::SyncFailure => "sync-failure",
        }
    }

    fn parse(value: &str) -> TestResult<Self> {
        match value {
            "invalid-root" => Ok(Self::InvalidRoot),
            "split-failure" => Ok(Self::SplitFailure),
            "auto-split-failure" => Ok(Self::AutoSplitFailure),
            "checkpoint-helper-cancellation" => Ok(Self::CheckpointHelperCancellation),
            "replay-callback-failure" => Ok(Self::ReplayCallbackFailure),
            #[cfg(unix)]
            "sync-failure" => Ok(Self::SyncFailure),
            other => Err(format!("unknown WAL error-boundary phase: {other}")),
        }
    }
}

/// 验证真实 I/O 错误、检查点分裂错误和安全取消点均按既有合同收口。
#[test]
fn test_wal_commit_error_and_cancellation_boundaries() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        install_abort_on_any_panic();
        let phase = Phase::parse(&phase).unwrap_or_else(|error| panic!("{error}"));
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("WAL error-boundary child must receive its root path"),
        );
        run_on_runtime(RUNTIME_TIMEOUT, move |rt| async move {
            run_phase(phase, rt, root).await
        })
        .unwrap_or_else(|error| {
            panic!("WAL error-boundary phase {} failed: {error}", phase.name(),)
        });
        return;
    }

    let mut phases = vec![
        Phase::InvalidRoot,
        Phase::SplitFailure,
        Phase::AutoSplitFailure,
        Phase::CheckpointHelperCancellation,
        Phase::ReplayCallbackFailure,
    ];

    #[cfg(unix)]
    if Path::new("/dev/full").exists() {
        phases.push(Phase::SyncFailure);
    } else {
        eprintln!("WAL_ERROR_BOUNDARY_SKIP phase=sync-failure reason=/dev/full-is-unavailable");
    }

    for phase in phases {
        let root = unique_temp_root(phase.name());
        fs::create_dir_all(&root).expect("creating WAL error-boundary root must succeed");
        if let Err(error) = run_phase_process(phase, &root, PROCESS_TIMEOUT) {
            panic!(
                "WAL error-boundary phase {} failed; evidence is preserved at {:?}: {error}",
                phase.name(),
                root,
            );
        }
        fs::remove_dir_all(&root).expect("cleaning WAL error-boundary root must succeed");
    }
}

async fn run_phase(phase: Phase, rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    match phase {
        Phase::InvalidRoot => verify_invalid_root(&rt, &root).await,
        Phase::SplitFailure => verify_split_failure(&rt, &root).await,
        Phase::AutoSplitFailure => verify_auto_split_failure(&rt, &root).await,
        Phase::CheckpointHelperCancellation => {
            verify_checkpoint_helper_cancellation(&rt, &root).await
        }
        Phase::ReplayCallbackFailure => verify_replay_callback_failure(&rt, &root).await,
        #[cfg(unix)]
        Phase::SyncFailure => verify_sync_failure(&rt, &root).await,
    }
}

async fn verify_invalid_root(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<()> {
    let invalid_root = root.join("ordinary-file-instead-of-directory");
    fs::write(&invalid_root, b"not a directory")
        .map_err(|error| format!("creating invalid WAL root failed: {error}"))?;

    let error = match CommitLoggerBuilder::new(rt.clone(), &invalid_root)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
    {
        Ok(_) => {
            return Err("building CommitLogger on an ordinary file unexpectedly succeeded".into())
        }
        Err(error) => error,
    };
    expect_eq("invalid root error kind", error.kind(), ErrorKind::Other)?;
    expect_contains(
        "invalid root error message",
        &error.to_string(),
        "Clean log dir failed",
    )
}

#[cfg(unix)]
async fn verify_sync_failure(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<()> {
    use std::os::unix::fs::symlink;

    let wal = root.join("wal");
    fs::create_dir_all(&wal)
        .map_err(|error| format!("creating sync-failure WAL directory failed: {error}"))?;
    let active = wal.join("000000001");
    symlink("/dev/full", &active)
        .map_err(|error| format!("linking test WAL to /dev/full failed: {error}"))?;

    let log = LogFile::open(rt.clone(), &wal, 8 * 1024, 2 * 1024 * 1024, None)
        .await
        .map_err(|error| format!("opening /dev/full-backed LogFile failed: {error}"))?;

    // 先验证直接强制提交所有者看到真实 ENOSPC 的包装错误，且提交水位没有被伪造为成功。
    let owner_handle = log.append(LogMethod::PlainAppend, b"owner", &[0x5a; 128]);
    let owner_error = log
        .commit(owner_handle, true, false, None)
        .await
        .expect_err("forcing a write to /dev/full must fail");
    expect_sync_error("direct owner", &owner_error)?;
    expect_eq("committed UID after owner failure", log.commited_uid(), 0)?;

    // 再构造一个定时提交所有者和三个等待者。三个 Future 先各轮询一次，确保全部已进入同一
    // 等待队列，然后才等待定时器的真实同步失败；这样可以精确断言错误广播，而不使用会重复
    // 轮询底层写 Future 的组合器。
    let first = log.append(LogMethod::PlainAppend, b"waiter-1", &[0x61; 128]);
    let second = log.append(LogMethod::PlainAppend, b"waiter-2", &[0x62; 128]);
    let third = log.append(LogMethod::PlainAppend, b"waiter-3", &[0x63; 128]);
    let mut first_flush = log.delay_commit(first, false, 1_000);
    let mut second_flush = log.delay_commit(second, false, 1_000);
    let mut third_flush = log.delay_commit(third, false, 1_000);
    expect_pending("first delayed waiter", poll_once(&mut first_flush))?;
    expect_pending("second delayed waiter", poll_once(&mut second_flush))?;
    expect_pending("third delayed waiter", poll_once(&mut third_flush))?;

    let first_error = first_flush
        .await
        .expect_err("first delayed waiter must receive sync failure");
    let second_error = second_flush
        .await
        .expect_err("second delayed waiter must receive sync failure");
    let third_error = third_flush
        .await
        .expect_err("third delayed waiter must receive sync failure");
    expect_sync_error("first delayed waiter", &first_error)?;
    expect_sync_error("second delayed waiter", &second_error)?;
    expect_sync_error("third delayed waiter", &third_error)?;
    expect_eq("committed UID after waiter failure", log.commited_uid(), 0)?;
    expect_eq("/dev/full-backed writable size", log.writable_size(), 0)?;

    // 使用独立目录把同一个真实 ENOSPC 注入检查点辅助提交。`new_check_point` 必须把辅助提交
    // 错误原样向上传播，并在 `?` 处停止：不能创建新文件、不能发布新检查点，也不能移除原
    // 事务登记。底层交换出的块不会恢复是既有致命 I/O 边界，本测试不在失败后伪造重试。
    let helper_wal = root.join("helper-wal");
    fs::create_dir_all(&helper_wal)
        .map_err(|error| format!("creating helper sync-failure directory failed: {error}"))?;
    symlink("/dev/full", helper_wal.join("000000001"))
        .map_err(|error| format!("linking helper WAL to /dev/full failed: {error}"))?;
    let logger = build_logger(rt, &helper_wal).await?;
    let uid = Guid(0x5400_0000_0000_0000_0000_0000_0000_0001);
    let _handle = logger
        .append(uid.clone(), vec![0xd4; 512])
        .await
        .map_err(|error| format!("appending helper sync-failure WAL failed: {error}"))?;
    let checkpoint = logger
        .check_point_of(uid.clone())
        .await
        .ok_or_else(|| "helper sync-failure WAL omitted checkpoint mapping".to_owned())?;
    let next_index_before_failure = logger.current_check_point().await;
    let helper_error = logger
        .append_check_point()
        .await
        .expect_err("checkpoint helper write to /dev/full must fail");
    expect_sync_error("checkpoint helper", &helper_error)?;
    expect_eq(
        "helper failure preserves checkpoint mapping",
        logger.check_point_of(uid.clone()).await,
        Some(checkpoint),
    )?;
    expect_eq(
        "helper failure preserves waiting registration",
        logger.waiting_confirm_count().await,
        1,
    )?;
    expect_eq("helper failure append count", logger.append_total_count(), 1)?;
    expect_eq("helper failure confirm count", logger.confirm_total_count(), 0)?;
    expect_eq(
        "helper failure must not consume split index",
        logger.current_check_point().await,
        next_index_before_failure,
    )?;
    if helper_wal
        .join(format!("{next_index_before_failure:09}"))
        .exists()
    {
        return Err("helper sync failure unexpectedly created a new checkpoint file".to_owned());
    }
    Ok(())
}

async fn verify_split_failure(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<()> {
    let wal = root.join("wal");
    let displaced_wal = root.join("wal-open-file-handles");
    let logger = build_logger(rt, &wal).await?;
    let uid = Guid(0x5100_0000_0000_0000_0000_0000_0000_0001);
    let payload = vec![0x71; 4 * 1024];
    let handle = logger
        .append(uid.clone(), payload)
        .await
        .map_err(|error| format!("appending split-failure WAL failed: {error}"))?;
    let registered_checkpoint = logger
        .check_point_of(uid.clone())
        .await
        .ok_or_else(|| "split-failure WAL was not registered to a checkpoint".to_owned())?;
    let next_index_before_failure = logger.current_check_point().await;

    // 已打开的旧文件句柄随目录重命名后仍有效；而后续 `path/new-file` 会因 path 已变为普通
    // 文件而稳定返回 NotADirectory。这样辅助提交的旧块同步真实成功，只有分裂失败。
    fs::rename(&wal, &displaced_wal)
        .map_err(|error| format!("displacing live WAL directory failed: {error}"))?;
    fs::write(&wal, b"block creating a child log file")
        .map_err(|error| format!("installing split obstruction failed: {error}"))?;

    let split_error = logger
        .append_check_point()
        .await
        .expect_err("checkpoint split through an ordinary file must fail");
    expect_eq(
        "checkpoint split error kind",
        split_error.kind(),
        ErrorKind::Other,
    )?;
    expect_contains(
        "checkpoint split error message",
        &split_error.to_string(),
        "Split log file failed",
    )?;
    expect_eq(
        "failed split must preserve transaction checkpoint mapping",
        logger.check_point_of(uid.clone()).await,
        Some(registered_checkpoint),
    )?;
    expect_eq(
        "failed split must preserve waiting transaction count",
        logger.waiting_confirm_count().await,
        1,
    )?;
    expect_eq(
        "append count after failed split",
        logger.append_total_count(),
        1,
    )?;
    expect_eq(
        "confirm count after failed split",
        logger.confirm_total_count(),
        0,
    )?;

    // `LogFile::split` 先 fetch_add 再创建文件，所以失败会消耗一个物理编号；这是既有可观察
    // 行为，不代表 CommitLogger 已发布了新检查点归属。测试把两种“编号”和“归属”明确区分。
    expect_eq(
        "failed split consumes one physical file index",
        logger.current_check_point().await,
        next_index_before_failure + 1,
    )?;

    fs::remove_file(&wal).map_err(|error| format!("removing split obstruction failed: {error}"))?;
    fs::rename(&displaced_wal, &wal)
        .map_err(|error| format!("restoring live WAL directory failed: {error}"))?;
    let old_active = wal.join(format!("{registered_checkpoint:09}"));
    let old_len = fs::metadata(&old_active)
        .map_err(|error| format!("reading helper-synced old checkpoint failed: {error}"))?
        .len();
    if old_len == 0 {
        return Err("helper succeeded before split failure but old checkpoint stayed empty".into());
    }
    if wal.join(format!("{next_index_before_failure:09}")).exists() {
        return Err("failed split unexpectedly created its requested physical file".into());
    }

    // 辅助提交已经同步目标句柄；原刷新应走水位快路。恢复目录后的下一次轮换使用跳号后的
    // 编号成功发布，随后确认仍可按旧检查点归属把原文件转成 `.bak`。
    logger
        .flush(handle)
        .await
        .map_err(|error| format!("flushing helper-synced handle failed: {error}"))?;
    let recovered_checkpoint = logger.append_check_point().await.map_err(|error| {
        format!("checkpoint rotation after restoring directory failed: {error}")
    })?;
    expect_eq(
        "recovered split index",
        recovered_checkpoint,
        next_index_before_failure + 1,
    )?;
    logger
        .confirm(uid.clone())
        .await
        .map_err(|error| format!("confirming transaction after split recovery failed: {error}"))?;
    expect_eq(
        "confirmed transaction mapping",
        logger.check_point_of(uid).await,
        None,
    )?;
    expect_eq(
        "waiting transaction count after split recovery",
        logger.waiting_confirm_count().await,
        0,
    )?;
    expect_eq(
        "confirm count after split recovery",
        logger.confirm_total_count(),
        1,
    )?;
    if !old_active.with_extension("bak").is_file() {
        return Err("confirmed old checkpoint was not converted to .bak".into());
    }
    Ok(())
}

/// 验证公开 `LogFile` 在“WAL 已同步、自动分裂创建新文件失败”时保留既有可观察顺序。
///
/// 这与检查点显式 split 失败不同：提交所有者会先把整块写入旧文件并唤醒等待者，随后才尝试
/// 自动分裂。因此等待者仍看到本批 WAL 同步成功，所有者看到文件创建错误；历史实现不会推进
/// `commited_uid`。本测试只冻结现状，不把这种部分成功边界解释成可安全重试。
async fn verify_auto_split_failure(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<()> {
    const FILE_LIMIT: usize = 1024 * 1024;

    let wal = root.join("wal");
    let displaced_wal = root.join("wal-open-file-handles");
    let log = LogFile::open(rt.clone(), &wal, 8 * 1024, FILE_LIMIT, None)
        .await
        .map_err(|error| format!("opening auto-split failure LogFile failed: {error}"))?;
    let next_index_before_failure = log.current_log_index();

    let waiter_handle = log.append(LogMethod::PlainAppend, b"waiter", &[0xa1; 128]);
    let mut waiter = log.delay_commit(waiter_handle, false, 10_000);
    expect_pending("auto-split waiter before owner", poll_once(&mut waiter))?;

    let owner_payload = vec![0xb2; FILE_LIMIT + 64 * 1024];
    let owner_handle = log.append(
        LogMethod::PlainAppend,
        b"owner",
        &owner_payload,
    );
    fs::rename(&wal, &displaced_wal)
        .map_err(|error| format!("displacing auto-split WAL directory failed: {error}"))?;
    fs::write(&wal, b"block automatic child file creation")
        .map_err(|error| format!("installing auto-split obstruction failed: {error}"))?;

    let owner_error = log
        .commit(owner_handle, true, false, None)
        .await
        .expect_err("automatic split through an ordinary file must fail");
    expect_eq(
        "auto-split owner error kind",
        owner_error.kind(),
        ErrorKind::Other,
    )?;
    expect_contains(
        "auto-split owner error message",
        &owner_error.to_string(),
        "Append log file failed",
    )?;

    waiter
        .await
        .map_err(|error| format!("already-synced waiter received owner split error: {error}"))?;
    expect_eq(
        "automatic split failure must not advance committed UID",
        log.commited_uid(),
        0,
    )?;
    expect_eq(
        "automatic split failure consumes one physical file index",
        log.current_log_index(),
        next_index_before_failure + 1,
    )?;
    if log.writable_size() <= FILE_LIMIT {
        return Err(format!(
            "auto-split failure did not account the synchronized old file: {} <= {FILE_LIMIT}",
            log.writable_size(),
        ));
    }
    let old_active = displaced_wal.join("000000001");
    let old_len = old_active
        .metadata()
        .map_err(|error| format!("reading auto-split synchronized WAL failed: {error}"))?
        .len() as usize;
    if old_len <= FILE_LIMIT {
        return Err(format!(
            "auto-split failure old WAL is unexpectedly short: {old_len} <= {FILE_LIMIT}",
        ));
    }
    if displaced_wal.join(format!("{next_index_before_failure:09}")).exists() {
        return Err("failed automatic split unexpectedly created its requested file".to_owned());
    }
    Ok(())
}

async fn verify_checkpoint_helper_cancellation(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<()> {
    let wal = root.join("wal");
    let logger = build_logger(rt, &wal).await?;
    let uid = Guid(0x5200_0000_0000_0000_0000_0000_0000_0001);
    let handle = logger
        .append(uid.clone(), vec![0x82; 8 * 1024])
        .await
        .map_err(|error| format!("appending cancellation WAL failed: {error}"))?;
    let checkpoint = logger
        .check_point_of(uid.clone())
        .await
        .ok_or_else(|| "cancellation WAL was not registered to a checkpoint".to_owned())?;
    let next_index_before_cancel = logger.current_check_point().await;
    let active = wal.join(format!("{checkpoint:09}"));

    let mut rotation = logger.append_check_point();
    expect_pending(
        "checkpoint future before cancellation",
        poll_once(&mut rotation),
    )?;
    drop(rotation);

    // 只取消接收辅助提交结果的外层 Future。辅助提交是自持有任务，必须继续完成 `commit_inner`
    // 的写入、裸指针归还、等待者唤醒和水位更新；外层被丢弃后则绝不能继续执行分裂。
    wait_for_nonempty_file(rt, &active, Duration::from_secs(3)).await?;
    logger
        .flush(handle)
        .await
        .map_err(|error| format!("flushing after helper receiver cancellation failed: {error}"))?;
    expect_eq(
        "cancelled outer checkpoint must not split",
        logger.current_check_point().await,
        next_index_before_cancel,
    )?;
    expect_eq(
        "cancelled outer checkpoint must preserve registration",
        logger.check_point_of(uid.clone()).await,
        Some(checkpoint),
    )?;
    if wal.join(format!("{next_index_before_cancel:09}")).exists() {
        return Err("cancelled outer checkpoint unexpectedly created a new WAL file".into());
    }

    let new_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("rotating after helper cancellation failed: {error}"))?;
    expect_eq(
        "first completed split after helper cancellation",
        new_checkpoint,
        next_index_before_cancel,
    )?;
    logger
        .confirm(uid)
        .await
        .map_err(|error| format!("confirming after helper cancellation failed: {error}"))?;
    expect_eq(
        "waiting count after helper cancellation recovery",
        logger.waiting_confirm_count().await,
        0,
    )
}

async fn verify_replay_callback_failure(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<()> {
    let wal = root.join("wal");
    let uid = Guid(0x5300_0000_0000_0000_0000_0000_0000_0001);
    let payload = vec![0x93; 2 * 1024];

    // 先用真实 LogFile 生成没有进程内检查点表状态的 WAL，等价于新进程只从磁盘恢复。
    let raw = LogFile::open(rt.clone(), &wal, 8 * 1024, 2 * 1024 * 1024, None)
        .await
        .map_err(|error| format!("opening raw replay WAL failed: {error}"))?;
    let handle = raw.append(
        LogMethod::PlainAppend,
        uid.0.to_le_bytes().as_ref(),
        &payload,
    );
    raw.commit(handle, true, false, None)
        .await
        .map_err(|error| format!("syncing raw replay WAL failed: {error}"))?;
    drop(raw);

    let logger = build_logger(rt, &wal).await?;
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_copy = callback_count.clone();
    let callback_logger = logger.clone();
    let expected_uid = uid.clone();
    let replay_error = logger
        .start_replay::<Vec<u8>, _>(Arc::new(move |replayed_uid, replayed_payload| {
            callback_count_copy.fetch_add(1, Ordering::SeqCst);
            if replayed_uid != expected_uid || replayed_payload != payload {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "injected callback received unexpected WAL data",
                ));
            }
            block_on(async {
                let replay_handle = callback_logger
                    .append_replay(replayed_uid, replayed_payload)
                    .await?;
                callback_logger.flush_replay(replay_handle).await?;
                Err(Error::new(
                    ErrorKind::InvalidData,
                    "intentional replay callback failure",
                ))
            })
        }))
        .await
        .expect_err("injected replay callback failure must reach the caller");
    expect_eq(
        "replay callback error kind",
        replay_error.kind(),
        ErrorKind::Other,
    )?;
    expect_contains(
        "replay callback error message",
        &replay_error.to_string(),
        "intentional replay callback failure",
    )?;
    expect_eq(
        "replay callback invocation count",
        callback_count.load(Ordering::SeqCst),
        1,
    )?;
    expect_eq(
        "registered replay transaction count after callback error",
        logger.waiting_confirm_count().await,
        1,
    )?;

    // start_replay 的错误不会隐式结束重播。此时普通 confirm 必须进入缓冲，只有显式
    // finish_replay 才切回正常模式并实际确认；重复 finish 保持幂等。
    logger.confirm(uid.clone()).await.map_err(|error| {
        format!("buffering confirm after replay callback error failed: {error}")
    })?;
    expect_eq(
        "confirm count while replaying",
        logger.confirm_total_count(),
        0,
    )?;
    expect_eq(
        "waiting count while replay confirm is buffered",
        logger.waiting_confirm_count().await,
        1,
    )?;
    logger
        .finish_replay()
        .await
        .map_err(|error| format!("finishing failed replay session failed: {error}"))?;
    expect_eq(
        "confirm count after finish replay",
        logger.confirm_total_count(),
        1,
    )?;
    expect_eq(
        "waiting count after finish replay",
        logger.waiting_confirm_count().await,
        0,
    )?;
    logger
        .finish_replay()
        .await
        .map_err(|error| format!("repeating finish_replay failed: {error}"))?;
    expect_eq(
        "confirm count after repeated finish replay",
        logger.confirm_total_count(),
        1,
    )
}

async fn build_logger(rt: &MultiTaskRuntime<()>, path: &Path) -> TestResult<CommitLogger> {
    CommitLoggerBuilder::new(rt.clone(), path)
        .log_block_limit(8 * 1024)
        .delay_timeout(10)
        .log_file_limit(32 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {path:?} failed: {error}"))
}

fn poll_once<T>(
    future: &mut futures::future::BoxFuture<'_, std::io::Result<T>>,
) -> Poll<std::io::Result<T>> {
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    future.as_mut().poll(&mut context)
}

fn expect_pending<T: std::fmt::Debug>(
    label: &str,
    poll: Poll<std::io::Result<T>>,
) -> TestResult<()> {
    match poll {
        Poll::Pending => Ok(()),
        Poll::Ready(result) => Err(format!(
            "{label}: expected Poll::Pending before owner completion, observed {result:?}",
        )),
    }
}

async fn wait_for_nonempty_file(
    rt: &MultiTaskRuntime<()>,
    path: &Path,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(metadata) = fs::metadata(path) {
            if metadata.len() > 0 {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "waiting for helper-owned WAL write exceeded {timeout:?}: {path:?}",
            ));
        }
        rt.timeout(1).await;
    }
}

fn expect_sync_error(label: &str, error: &Error) -> TestResult<()> {
    expect_eq(
        &format!("{label} error kind"),
        error.kind(),
        ErrorKind::Other,
    )?;
    expect_contains(
        &format!("{label} error message"),
        &error.to_string(),
        "Sync log failed",
    )
}

fn expect_contains(label: &str, actual: &str, expected: &str) -> TestResult<()> {
    if actual.contains(expected) {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {actual:?} to contain {expected:?}",
        ))
    }
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

fn install_abort_on_any_panic() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning WAL error-boundary phase failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("WAL error-boundary phase exceeded {timeout:?}: {error}"))?
}

fn run_phase_process(phase: Phase, root: &Path, timeout: Duration) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating WAL error-boundary executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase.name())
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| {
            format!(
                "spawning WAL error-boundary phase {} failed: {error}",
                phase.name(),
            )
        })?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "WAL error-boundary phase {} exited with {status}",
            phase.name(),
        ))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking WAL error-boundary child failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("WAL error-boundary child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn unique_temp_root(phase: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_store_wal_error_{phase}_{}_{}",
        std::process::id(),
        nanos,
    ))
}
