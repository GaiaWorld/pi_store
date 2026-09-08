//! WAL 重新打开、重播与确认回收的真实进程专项。
//!
//! 测试按“写入后直接退出 -> 新进程重播 -> 再次新进程确认无重复”三个阶段运行。每个阶段
//! 都使用真实多线程运行时、`CommitLogger`、同步文件写和本地文件系统；父进程严格串行启动
//! 子进程并施加硬截止。这样既保留了真实进程生命周期边界，也不会让后台整理任务跨场景共享。

use std::{
    env, fs,
    future::Future,
    io::{Error, ErrorKind},
    panic,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use futures::executor::block_on;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_guid::Guid;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;

const TEST_NAME: &str = "test_wal_reopen_replay_and_confirm_is_exact";
const PHASE_ENV: &str = "PI_STORE_WAL_REPLAY_PHASE";
const WORKERS_ENV: &str = "PI_STORE_WAL_REPLAY_WORKERS";
const ROOT_ENV: &str = "PI_STORE_WAL_REPLAY_ROOT";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(30);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(20);
const RECORDS_PER_CHECKPOINT: usize = 4;
const CHECKPOINT_COUNT: usize = 3;
const PAYLOAD_LENGTHS: [usize; RECORDS_PER_CHECKPOINT] = [64, 1_024, 8_192, 32_768];

#[derive(Clone, Copy, Debug)]
enum Phase {
    WriteAndExit,
    ReplayAndConfirm,
    VerifyNoReplay,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::WriteAndExit => "write-and-exit",
            Self::ReplayAndConfirm => "replay-and-confirm",
            Self::VerifyNoReplay => "verify-no-replay",
        }
    }

    fn parse(value: &str) -> TestResult<Self> {
        match value {
            "write-and-exit" => Ok(Self::WriteAndExit),
            "replay-and-confirm" => Ok(Self::ReplayAndConfirm),
            "verify-no-replay" => Ok(Self::VerifyNoReplay),
            other => Err(format!("unknown WAL replay phase: {other}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExpectedRecord {
    uid: u128,
    payload: Vec<u8>,
}

/// 验证同步成功的 WAL 在直接退出后恰好重播一次，并在确认后不再出现。
#[test]
fn test_wal_reopen_replay_and_confirm_is_exact() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        install_abort_on_any_panic();
        let phase = Phase::parse(&phase).unwrap_or_else(|error| panic!("{error}"));
        let workers = env::var(WORKERS_ENV)
            .expect("WAL replay child must receive worker count")
            .parse::<usize>()
            .expect("WAL replay worker count must be a positive integer");
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("WAL replay child must receive its root path"),
        );

        run_on_runtime(workers, RUNTIME_TIMEOUT, move |rt| async move {
            run_phase(phase, rt, root, workers).await
        })
        .unwrap_or_else(|error| {
            panic!(
                "WAL replay phase {} failed with {workers} workers: {error}",
                phase.name(),
            )
        });

        if matches!(phase, Phase::WriteAndExit) {
            // 所有 `flush` 已经返回，数据已通过 Sync(true) 持久化。这里直接结束进程，不运行
            // logger/runtime 析构，模拟应用在持久化门禁之后、确认之前异常退出。
            std::process::exit(0);
        }
        return;
    }

    for workers in [1usize, 4usize] {
        let root = unique_temp_root(workers);
        fs::create_dir_all(&root).expect("creating WAL replay evidence root must succeed");

        for phase in [
            Phase::WriteAndExit,
            Phase::ReplayAndConfirm,
            Phase::VerifyNoReplay,
        ] {
            if let Err(error) = run_phase_process(phase, workers, &root, PROCESS_TIMEOUT) {
                panic!(
                    "WAL replay target failed in phase {} with {workers} workers; evidence is preserved at {:?}: {error}",
                    phase.name(),
                    root,
                );
            }
        }

        fs::remove_dir_all(&root).expect("cleaning WAL replay evidence root must succeed");
    }
}

async fn run_phase(
    phase: Phase,
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    workers: usize,
) -> TestResult<()> {
    let wal_path = root.join("wal");
    match phase {
        Phase::WriteAndExit => write_unconfirmed_wal(&rt, &wal_path, workers).await,
        Phase::ReplayAndConfirm => replay_and_confirm_wal(&rt, &wal_path, workers).await,
        Phase::VerifyNoReplay => verify_confirmed_wal_is_not_replayed(&rt, &wal_path).await,
    }
}

async fn write_unconfirmed_wal(
    rt: &MultiTaskRuntime<()>,
    wal_path: &Path,
    workers: usize,
) -> TestResult<()> {
    let logger = build_logger(rt, wal_path).await?;
    let expected = expected_records(workers);

    for checkpoint in 0..CHECKPOINT_COUNT {
        let range_start = checkpoint * RECORDS_PER_CHECKPOINT;
        let range_end = range_start + RECORDS_PER_CHECKPOINT;
        let mut handles = Vec::with_capacity(RECORDS_PER_CHECKPOINT);
        for record in &expected[range_start..range_end] {
            let handle = logger
                .append(Guid(record.uid), record.payload.clone())
                .await
                .map_err(|error| format!("appending WAL uid {:#x} failed: {error}", record.uid))?;
            if handle == 0 {
                return Err(format!(
                    "nonempty WAL uid {:#x} returned handle 0",
                    record.uid,
                ));
            }
            handles.push(handle);
        }

        // 本目标验证重播，不把多个 flush future 组合在同一个父 future 中。pi_async_file 0.9.0
        // 的写 future 缺少“写任务已经派发”的在途状态；组合器因兄弟 future 被唤醒而再次
        // 轮询提交所有者（owner）时，会重复派发同一物理写。该既有依赖问题另行归档。这里
        // 第一个句柄
        // 真实同步包含整批记录的块，其余句柄顺序验证已提交水位快速路径。
        for handle in handles {
            logger.flush(handle).await.map_err(|error| {
                format!("flushing checkpoint batch {checkpoint} failed: {error}")
            })?;
        }

        if checkpoint + 1 < CHECKPOINT_COUNT {
            logger.append_check_point().await.map_err(|error| {
                format!("rotating checkpoint batch {checkpoint} failed: {error}")
            })?;
        }
    }

    expect_eq(
        "writer append count",
        logger.append_total_count(),
        expected.len(),
    )?;
    expect_eq("writer confirm count", logger.confirm_total_count(), 0)?;
    expect_eq(
        "writer waiting count",
        logger.waiting_confirm_count().await,
        expected.len(),
    )?;
    Ok(())
}

async fn replay_and_confirm_wal(
    rt: &MultiTaskRuntime<()>,
    wal_path: &Path,
    workers: usize,
) -> TestResult<()> {
    let logger = build_logger(rt, wal_path).await?;
    let expected = expected_records(workers);
    let observed = Arc::new(Mutex::new(Vec::<ExpectedRecord>::new()));
    let callback_observed = observed.clone();
    let callback_logger = logger.clone();

    let replay_result = logger
        .start_replay::<Vec<u8>, _>(Arc::new(move |uid: Guid, payload: Vec<u8>| {
            callback_observed
                .lock()
                .map_err(|_| Error::new(ErrorKind::Other, "replay observation mutex poisoned"))?
                .push(ExpectedRecord {
                    uid: uid.0,
                    payload: payload.clone(),
                });

            // 加载器会在同步回调返回后推进到下一个物理检查点，因此该条记录的重播登记、
            // 空刷新门禁和确认缓冲必须在返回前全部完成。这里使用局部 block_on；三个 future
            // 只操作 CommitLogger 自身的无竞争状态，不依赖新 runtime 调度，单 worker 也不会
            // 因等待同一运行时上的另一个任务而自锁。
            block_on(async {
                let handle = callback_logger.append_replay(uid.clone(), payload).await?;
                callback_logger.flush_replay(handle).await?;
                callback_logger.confirm_replay(uid).await
            })
        }))
        .await
        .map_err(|error| format!("replaying WAL failed: {error}"))?;

    let expected_bytes = expected
        .iter()
        .map(|record| 16 + record.payload.len())
        .sum::<usize>();
    expect_eq("replay record count", replay_result.0, expected.len())?;
    expect_eq("replay payload bytes", replay_result.1, expected_bytes)?;
    let observed = observed
        .lock()
        .map_err(|_| "replay observation mutex poisoned".to_owned())?
        .clone();
    expect_eq(
        "replay callback order and payload",
        observed,
        expected.clone(),
    )?;

    // `confirm_replay` 只缓冲确认；在 `finish_replay` 之前，所有记录仍应登记在对应检查点。
    expect_eq(
        "replay waiting count before finish",
        logger.waiting_confirm_count().await,
        expected.len(),
    )?;
    expect_eq(
        "replay append count before finish",
        logger.append_total_count(),
        expected.len(),
    )?;
    expect_eq(
        "replay confirm count before finish",
        logger.confirm_total_count(),
        0,
    )?;

    logger
        .finish_replay()
        .await
        .map_err(|error| format!("finishing WAL replay failed: {error}"))?;
    expect_eq(
        "replay waiting count after finish",
        logger.waiting_confirm_count().await,
        0,
    )?;
    expect_eq(
        "replay confirm count after finish",
        logger.confirm_total_count(),
        expected.len(),
    )?;

    // 完成函数和未知事务确认都应保持既有幂等/无操作语义，不得重复增加确认计数。
    logger
        .finish_replay()
        .await
        .map_err(|error| format!("repeating finish_replay failed: {error}"))?;
    logger
        .confirm(Guid(u128::MAX - workers as u128))
        .await
        .map_err(|error| format!("confirming unknown uid failed: {error}"))?;
    expect_eq(
        "idempotent replay confirm count",
        logger.confirm_total_count(),
        expected.len(),
    )?;
    Ok(())
}

async fn verify_confirmed_wal_is_not_replayed(
    rt: &MultiTaskRuntime<()>,
    wal_path: &Path,
) -> TestResult<()> {
    let logger = build_logger(rt, wal_path).await?;
    let observed = Arc::new(Mutex::new(Vec::<u128>::new()));
    let callback_observed = observed.clone();
    let result = logger
        .start_replay::<Vec<u8>, _>(Arc::new(move |uid: Guid, _payload: Vec<u8>| {
            callback_observed
                .lock()
                .map_err(|_| Error::new(ErrorKind::Other, "final replay mutex poisoned"))?
                .push(uid.0);
            Ok(())
        }))
        .await
        .map_err(|error| format!("final empty replay failed: {error}"))?;
    logger
        .finish_replay()
        .await
        .map_err(|error| format!("finishing final empty replay failed: {error}"))?;

    expect_eq("final replay result", result, (0, 0))?;
    expect_eq(
        "confirmed WAL must not replay again",
        observed
            .lock()
            .map_err(|_| "final replay mutex poisoned".to_owned())?
            .clone(),
        Vec::<u128>::new(),
    )?;
    expect_eq("final append count", logger.append_total_count(), 0)?;
    expect_eq("final confirm count", logger.confirm_total_count(), 0)?;
    expect_eq(
        "final waiting count",
        logger.waiting_confirm_count().await,
        0,
    )
}

async fn build_logger(rt: &MultiTaskRuntime<()>, path: &Path) -> TestResult<CommitLogger> {
    CommitLoggerBuilder::new(rt.clone(), path)
        .log_block_limit(4 * 1024)
        .delay_timeout(1)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building real commit logger at {path:?} failed: {error}"))
}

fn expected_records(workers: usize) -> Vec<ExpectedRecord> {
    let mut records = Vec::with_capacity(RECORDS_PER_CHECKPOINT * CHECKPOINT_COUNT);
    for checkpoint in 0..CHECKPOINT_COUNT {
        for (index, len) in PAYLOAD_LENGTHS.into_iter().enumerate() {
            let sequence = checkpoint * RECORDS_PER_CHECKPOINT + index;
            let uid = 0x7100_0000_0000_0000u128 + ((workers as u128) << 32) + sequence as u128;
            records.push(ExpectedRecord {
                uid,
                payload: deterministic_payload(uid, len),
            });
        }
    }
    records
}

fn deterministic_payload(seed: u128, len: usize) -> Vec<u8> {
    let mut state = (seed as u64) ^ ((seed >> 64) as u64) ^ 0x9e37_79b9_7f4a_7c15;
    let mut payload = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(state as u8);
    }
    payload
}

fn install_abort_on_any_panic() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}

fn run_on_runtime<T, F, Fut>(workers: usize, timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(workers)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning WAL replay phase failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("WAL replay phase exceeded {timeout:?}: {error}"))?
}

fn run_phase_process(
    phase: Phase,
    workers: usize,
    root: &Path,
    timeout: Duration,
) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating WAL replay test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase.name())
        .env(WORKERS_ENV, workers.to_string())
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| {
            format!(
                "spawning WAL replay phase {} with {workers} workers failed: {error}",
                phase.name(),
            )
        })?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "WAL replay phase {} with {workers} workers exited with {status}",
            phase.name(),
        ))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking WAL replay child failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("WAL replay child exceeded {timeout:?}"));
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
        "pi_store_wal_replay_{workers}w_{}_{}",
        std::process::id(),
        nanos,
    ))
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
