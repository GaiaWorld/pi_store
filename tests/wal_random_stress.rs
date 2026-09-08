//! 长时间、高频、资源有界的 WAL 随机压力专项。
//!
//! 本目标以四个固定种子生成 70% 小片、25% 中片和 5% 大片数据，并让 93% 的计划操作进入
//! 真实 `CommitLogger` 持久化链。每个种子都在独立的写入子进程中完成高频提交并直接退出，
//! 再由新的重播子进程逐字节核验。所有种子和阶段严格串行，内存、目录大小、在途任务数和
//! 墙钟时间都有硬上限。

use std::{
    collections::HashSet,
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

use async_channel::{bounded as async_bounded, Receiver};
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

const TEST_NAME: &str = "test_resource_bounded_random_wal_stress";
const PHASE_ENV: &str = "PI_STORE_WAL_STRESS_PHASE";
const ROOT_ENV: &str = "PI_STORE_WAL_STRESS_ROOT";
const SEED_ENV: &str = "PI_STORE_WAL_STRESS_SEED";
const TARGET_BYTES_ENV: &str = "PI_STORE_WAL_STRESS_BYTES";

const DEFAULT_TARGET_BYTES: usize = 64 * 1024 * 1024;
const MIN_TARGET_BYTES: usize = 1024 * 1024;
const MAX_TARGET_BYTES: usize = 256 * 1024 * 1024;
const DIRECTORY_LIMIT_NUMERATOR: usize = 3;
const DIRECTORY_LIMIT_DENOMINATOR: usize = 2;
const ACTIVE_FLUSH_LIMIT: usize = 64;
const CHECKPOINT_INTERVAL_BYTES: usize = 8 * 1024 * 1024;
const RUNTIME_WORKERS: usize = 4;
const WRITE_TIMEOUT: Duration = Duration::from_secs(120);
const REPLAY_TIMEOUT: Duration = Duration::from_secs(60);
const FULL_TIMEOUT: Duration = Duration::from_secs(180);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(115);
const SEEDS: [u64; 4] = [
    0x12d6_87a4_5b39_c0ef,
    0x9e37_79b9_7f4a_7c15,
    0xa076_1d64_78bd_642f,
    0xe703_7ed1_a0b4_28db,
];

#[derive(Clone, Copy, Debug)]
enum Phase {
    WriteAndExit,
    ReplayAndConfirm,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::WriteAndExit => "write-and-exit",
            Self::ReplayAndConfirm => "replay-and-confirm",
        }
    }

    fn parse(value: &str) -> TestResult<Self> {
        match value {
            "write-and-exit" => Ok(Self::WriteAndExit),
            "replay-and-confirm" => Ok(Self::ReplayAndConfirm),
            other => Err(format!("unknown WAL stress phase: {other}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PayloadClass {
    Small,
    Medium,
    Large,
}

#[derive(Clone, Debug)]
struct PlannedRecord {
    uid: u128,
    payload_seed: u64,
    len: usize,
    class: PayloadClass,
}

#[derive(Debug)]
struct WorkloadPlan {
    records: Vec<PlannedRecord>,
    nonpersistent: usize,
    payload_bytes: usize,
    class_counts: [usize; 3],
}

#[derive(Debug)]
struct FlushOutcome {
    uid: u128,
    elapsed_micros: u128,
    result: std::io::Result<()>,
}

#[derive(Debug)]
struct ReplayState {
    next_index: usize,
    seen: HashSet<u128>,
}

/// 以固定种子验证混合大小 WAL 的高频提交、进程重启、重播和确认闭环。
#[test]
fn test_resource_bounded_random_wal_stress() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        install_abort_on_any_panic();
        let phase = Phase::parse(&phase).unwrap_or_else(|error| panic!("{error}"));
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("WAL stress child must receive its root path"),
        );
        let seed = parse_env_u64(SEED_ENV).expect("WAL stress child must receive a valid seed");
        let target_bytes = parse_target_bytes().unwrap_or_else(|error| panic!("{error}"));

        run_on_runtime(RUNTIME_TIMEOUT, move |rt| async move {
            match phase {
                Phase::WriteAndExit => write_stress(&rt, &root, seed, target_bytes).await,
                Phase::ReplayAndConfirm => {
                    replay_and_confirm_stress(&rt, &root, seed, target_bytes).await
                }
            }
        })
        .unwrap_or_else(|error| {
            panic!(
                "WAL stress phase {} failed for seed {seed:#018x}: {error}",
                phase.name(),
            )
        });

        if matches!(phase, Phase::WriteAndExit) {
            // 所有 flush 已通过 Sync(true) 返回。直接结束进程，刻意跳过 logger/runtime 析构，
            // 保留“持久化已完成、事务尚未确认时异常退出”的恢复边界。
            std::process::exit(0);
        }
        return;
    }

    let target_bytes = parse_target_bytes().unwrap_or_else(|error| panic!("{error}"));
    let started = Instant::now();
    for (seed_index, seed) in SEEDS.into_iter().enumerate() {
        let root = unique_temp_root(seed_index, seed);
        fs::create_dir_all(&root).expect("creating WAL stress evidence root must succeed");

        for (phase, phase_limit) in [
            (Phase::WriteAndExit, WRITE_TIMEOUT),
            (Phase::ReplayAndConfirm, REPLAY_TIMEOUT),
        ] {
            let remaining = FULL_TIMEOUT.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                panic!(
                    "WAL stress exceeded full timeout {FULL_TIMEOUT:?}; evidence is preserved at {root:?}",
                );
            }
            let timeout = std::cmp::min(remaining, phase_limit);
            if let Err(error) = run_phase_process(phase, &root, seed, target_bytes, timeout) {
                panic!(
                    "WAL stress failed in phase {} for seed {seed:#018x}; evidence is preserved at {:?}: {error}",
                    phase.name(),
                    root,
                );
            }
        }

        fs::remove_dir_all(&root).expect("cleaning WAL stress evidence root must succeed");
    }

    assert!(
        started.elapsed() <= FULL_TIMEOUT,
        "WAL stress exceeded full timeout {FULL_TIMEOUT:?}: {:?}",
        started.elapsed(),
    );
}

async fn write_stress(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
    seed: u64,
    target_bytes: usize,
) -> TestResult<()> {
    let wal_path = root.join("wal");
    let logger = build_logger(rt, &wal_path).await?;
    let plan = build_plan(seed, target_bytes);
    validate_plan(&plan, target_bytes)?;
    let directory_limit = directory_limit(target_bytes);

    let (outcome_tx, outcome_rx) = async_bounded::<FlushOutcome>(ACTIVE_FLUSH_LIMIT);
    let mut active = 0usize;
    let mut completed = 0usize;
    let mut other_errors = 0usize;
    let mut observed_uids = HashSet::with_capacity(plan.records.len());
    let mut latencies = Vec::with_capacity(plan.records.len());
    let mut bytes_since_checkpoint = 0usize;

    for (index, record) in plan.records.iter().enumerate() {
        let payload = make_payload(record.payload_seed, record.len);
        let handle = logger
            .append(Guid(record.uid), payload)
            .await
            .map_err(|error| {
                format!(
                    "appending stress record {index} uid {:#x} failed: {error}",
                    record.uid,
                )
            })?;
        if handle == 0 {
            return Err(format!(
                "nonempty stress record {index} uid {:#x} returned handle 0",
                record.uid,
            ));
        }

        let task_logger = logger.clone();
        let task_sender = outcome_tx.clone();
        let uid = record.uid;
        rt.spawn(async move {
            let started = Instant::now();
            let result = task_logger.flush(handle).await;
            let _ = task_sender
                .send(FlushOutcome {
                    uid,
                    elapsed_micros: started.elapsed().as_micros(),
                    result,
                })
                .await;
        })
        .map_err(|error| format!("spawning stress flush {index} failed: {error:?}"))?;
        active += 1;
        bytes_since_checkpoint += record.len;

        if bytes_since_checkpoint >= CHECKPOINT_INTERVAL_BYTES {
            logger.append_check_point().await.map_err(|error| {
                format!("rotating stress checkpoint at record {index} failed: {error}")
            })?;
            bytes_since_checkpoint = 0;
        }

        if active == ACTIVE_FLUSH_LIMIT {
            consume_outcome(
                &outcome_rx,
                &mut active,
                &mut completed,
                &mut other_errors,
                &mut observed_uids,
                &mut latencies,
            )
            .await?;
        }

        if index % 128 == 0 {
            enforce_directory_limit(&wal_path, directory_limit)?;
        }
    }

    while active > 0 {
        consume_outcome(
            &outcome_rx,
            &mut active,
            &mut completed,
            &mut other_errors,
            &mut observed_uids,
            &mut latencies,
        )
        .await?;
    }
    drop(outcome_tx);

    // 把最终已同步批次显式切成只读文件，重播进程会同时面对多个非空文件和一个空 current。
    logger
        .append_check_point()
        .await
        .map_err(|error| format!("sealing final stress checkpoint failed: {error}"))?;
    let directory_bytes = enforce_directory_limit(&wal_path, directory_limit)?;

    expect_eq(
        "stress completed flush count",
        completed,
        plan.records.len(),
    )?;
    expect_eq("stress other error count", other_errors, 0)?;
    expect_eq(
        "stress unique flush result count",
        observed_uids.len(),
        plan.records.len(),
    )?;
    expect_eq(
        "stress append total",
        logger.append_total_count(),
        plan.records.len(),
    )?;
    expect_eq(
        "stress confirm total before crash",
        logger.confirm_total_count(),
        0,
    )?;
    expect_eq(
        "stress waiting count before crash",
        logger.waiting_confirm_count().await,
        plan.records.len(),
    )?;

    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50);
    let p99 = percentile(&latencies, 99);
    eprintln!(
        "WAL_STRESS_WRITE seed={seed:#018x} records={} nonpersistent={} payload_bytes={} directory_bytes={} small={} medium={} large={} flush_p50_us={} flush_p99_us={}",
        plan.records.len(),
        plan.nonpersistent,
        plan.payload_bytes,
        directory_bytes,
        plan.class_counts[0],
        plan.class_counts[1],
        plan.class_counts[2],
        p50,
        p99,
    );
    Ok(())
}

async fn consume_outcome(
    receiver: &Receiver<FlushOutcome>,
    active: &mut usize,
    completed: &mut usize,
    other_errors: &mut usize,
    observed_uids: &mut HashSet<u128>,
    latencies: &mut Vec<u128>,
) -> TestResult<()> {
    let outcome = receiver
        .recv()
        .await
        .map_err(|error| format!("stress outcome channel closed early: {error}"))?;
    *active -= 1;
    match outcome.result {
        Ok(()) => {
            *completed += 1;
            latencies.push(outcome.elapsed_micros);
            if !observed_uids.insert(outcome.uid) {
                return Err(format!(
                    "stress flush uid {:#x} completed more than once",
                    outcome.uid,
                ));
            }
        }
        Err(error) => {
            *other_errors += 1;
            return Err(format!(
                "stress flush uid {:#x} failed unexpectedly: {error}",
                outcome.uid,
            ));
        }
    }
    Ok(())
}

async fn replay_and_confirm_stress(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
    seed: u64,
    target_bytes: usize,
) -> TestResult<()> {
    let wal_path = root.join("wal");
    let logger = build_logger(rt, &wal_path).await?;
    let plan = Arc::new(build_plan(seed, target_bytes));
    validate_plan(&plan, target_bytes)?;
    let expected_bytes = plan
        .records
        .iter()
        .map(|record| 16 + record.len)
        .sum::<usize>();
    let state = Arc::new(Mutex::new(ReplayState {
        next_index: 0,
        seen: HashSet::with_capacity(plan.records.len()),
    }));
    let callback_plan = plan.clone();
    let callback_state = state.clone();
    let callback_logger = logger.clone();

    let started = Instant::now();
    let replay_result = logger
        .start_replay::<Vec<u8>, _>(Arc::new(move |uid: Guid, payload: Vec<u8>| {
            let mut state = callback_state
                .lock()
                .map_err(|_| Error::new(ErrorKind::Other, "stress replay state poisoned"))?;
            let index = state.next_index;
            let expected = callback_plan.records.get(index).ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("stress replay produced unexpected extra uid {:#x}", uid.0),
                )
            })?;
            if uid.0 != expected.uid {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "stress replay order mismatch at {index}: expected {:#x}, observed {:#x}",
                        expected.uid, uid.0,
                    ),
                ));
            }
            if payload.len() != expected.len {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "stress replay length mismatch at {index}: expected {}, observed {}",
                        expected.len,
                        payload.len(),
                    ),
                ));
            }
            let expected_payload = make_payload(expected.payload_seed, expected.len);
            if payload != expected_payload {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "stress replay payload mismatch at {index}, uid {:#x}",
                        uid.0
                    ),
                ));
            }
            if !state.seen.insert(uid.0) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("stress replay duplicated uid {:#x}", uid.0),
                ));
            }
            state.next_index += 1;
            drop(state);

            // 必须在回调返回、加载器推进检查点之前完成这条记录的重播登记和确认缓冲。
            block_on(async {
                let handle = callback_logger.append_replay(uid.clone(), payload).await?;
                callback_logger.flush_replay(handle).await?;
                callback_logger.confirm_replay(uid).await
            })
        }))
        .await
        .map_err(|error| format!("replaying stress WAL failed: {error}"))?;

    expect_eq("stress replay count", replay_result.0, plan.records.len())?;
    expect_eq("stress replay bytes", replay_result.1, expected_bytes)?;
    {
        let state = state
            .lock()
            .map_err(|_| "stress replay state poisoned".to_owned())?;
        expect_eq(
            "stress replay callback count",
            state.next_index,
            plan.records.len(),
        )?;
        expect_eq(
            "stress replay unique uid count",
            state.seen.len(),
            plan.records.len(),
        )?;
    }
    expect_eq(
        "stress replay waiting before finish",
        logger.waiting_confirm_count().await,
        plan.records.len(),
    )?;
    expect_eq(
        "stress replay confirm before finish",
        logger.confirm_total_count(),
        0,
    )?;

    logger
        .finish_replay()
        .await
        .map_err(|error| format!("finishing stress replay failed: {error}"))?;
    expect_eq(
        "stress replay waiting after finish",
        logger.waiting_confirm_count().await,
        0,
    )?;
    expect_eq(
        "stress replay confirm after finish",
        logger.confirm_total_count(),
        plan.records.len(),
    )?;
    let directory_bytes = enforce_directory_limit(&wal_path, directory_limit(target_bytes))?;
    eprintln!(
        "WAL_STRESS_REPLAY seed={seed:#018x} records={} payload_bytes={} directory_bytes={} elapsed_ms={}",
        plan.records.len(),
        plan.payload_bytes,
        directory_bytes,
        started.elapsed().as_millis(),
    );
    Ok(())
}

async fn build_logger(rt: &MultiTaskRuntime<()>, path: &Path) -> TestResult<CommitLogger> {
    CommitLoggerBuilder::new(rt.clone(), path)
        .log_block_limit(2 * 1024 * 1024)
        .delay_timeout(1)
        .log_file_limit(32 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building stress CommitLogger at {path:?} failed: {error}"))
}

fn build_plan(seed: u64, target_bytes: usize) -> WorkloadPlan {
    let mut random = SplitMix64::new(seed);
    let mut records = Vec::new();
    let mut nonpersistent = 0usize;
    let mut payload_bytes = 0usize;
    let mut class_counts = [0usize; 3];

    while payload_bytes < target_bytes {
        if random.next_u64() % 100 >= 93 {
            nonpersistent += 1;
            continue;
        }

        let class_roll = random.next_u64() % 100;
        let (class, min, max, class_index) = if class_roll < 70 {
            (PayloadClass::Small, 64usize, 1024usize, 0usize)
        } else if class_roll < 95 {
            (PayloadClass::Medium, 4 * 1024, 64 * 1024, 1usize)
        } else {
            (PayloadClass::Large, 256 * 1024, 1024 * 1024, 2usize)
        };
        let len = random.range_inclusive(min, max);
        if payload_bytes + len > target_bytes {
            break;
        }

        let sequence = records.len();
        let uid = 0x7300_0000_0000_0000_0000_0000_0000_0000u128
            ^ ((seed as u128) << 48)
            ^ sequence as u128;
        records.push(PlannedRecord {
            uid,
            payload_seed: random.next_u64() ^ sequence as u64,
            len,
            class,
        });
        class_counts[class_index] += 1;
        payload_bytes += len;
    }

    WorkloadPlan {
        records,
        nonpersistent,
        payload_bytes,
        class_counts,
    }
}

fn validate_plan(plan: &WorkloadPlan, target_bytes: usize) -> TestResult<()> {
    if plan.records.is_empty() {
        return Err("stress plan unexpectedly contains no persistent records".to_owned());
    }
    if plan.payload_bytes > target_bytes {
        return Err(format!(
            "stress plan exceeds payload limit: {} > {target_bytes}",
            plan.payload_bytes,
        ));
    }
    if target_bytes - plan.payload_bytes > 1024 * 1024 {
        return Err(format!(
            "stress plan stopped too far below target: target={target_bytes}, actual={}",
            plan.payload_bytes,
        ));
    }
    if plan.class_counts.iter().any(|count| *count == 0) {
        return Err(format!(
            "stress plan must contain every payload class: {:?}",
            plan.class_counts,
        ));
    }
    if plan.nonpersistent == 0 {
        return Err("stress plan must contain nonpersistent operations".to_owned());
    }
    for record in &plan.records {
        let valid = match record.class {
            PayloadClass::Small => (64..=1024).contains(&record.len),
            PayloadClass::Medium => (4 * 1024..=64 * 1024).contains(&record.len),
            PayloadClass::Large => (256 * 1024..=1024 * 1024).contains(&record.len),
        };
        if !valid {
            return Err(format!(
                "stress record {:#x} has invalid {:?} length {}",
                record.uid, record.class, record.len,
            ));
        }
    }
    Ok(())
}

fn make_payload(seed: u64, len: usize) -> Vec<u8> {
    let mut random = SplitMix64::new(seed);
    let mut payload = Vec::with_capacity(len);
    while payload.len() + 8 <= len {
        payload.extend_from_slice(&random.next_u64().to_le_bytes());
    }
    if payload.len() < len {
        let tail = random.next_u64().to_le_bytes();
        payload.extend_from_slice(&tail[..len - payload.len()]);
    }
    payload
}

#[derive(Clone, Copy, Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn range_inclusive(&mut self, min: usize, max: usize) -> usize {
        min + (self.next_u64() as usize % (max - min + 1))
    }
}

fn directory_limit(target_bytes: usize) -> usize {
    target_bytes.saturating_mul(DIRECTORY_LIMIT_NUMERATOR) / DIRECTORY_LIMIT_DENOMINATOR
}

fn enforce_directory_limit(path: &Path, limit: usize) -> TestResult<usize> {
    let size = directory_size(path)?;
    if size > limit {
        Err(format!(
            "stress WAL directory exceeded hard limit: path={path:?}, size={size}, limit={limit}",
        ))
    } else {
        Ok(size)
    }
}

fn directory_size(path: &Path) -> TestResult<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let mut size = 0usize;
    for entry in fs::read_dir(path)
        .map_err(|error| format!("reading stress directory {path:?} failed: {error}"))?
    {
        let entry =
            entry.map_err(|error| format!("reading stress directory entry failed: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("reading stress file metadata failed: {error}"))?;
        if metadata.is_file() {
            size = size
                .checked_add(metadata.len() as usize)
                .ok_or_else(|| "stress directory size overflowed usize".to_owned())?;
        }
    }
    Ok(size)
}

fn percentile(sorted: &[u128], percent: usize) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) * percent) / 100;
    sorted[index]
}

fn parse_target_bytes() -> TestResult<usize> {
    let value = match env::var(TARGET_BYTES_ENV) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("invalid {TARGET_BYTES_ENV} value {value:?}: {error}"))?,
        Err(env::VarError::NotPresent) => DEFAULT_TARGET_BYTES,
        Err(error) => return Err(format!("reading {TARGET_BYTES_ENV} failed: {error}")),
    };
    if !(MIN_TARGET_BYTES..=MAX_TARGET_BYTES).contains(&value) {
        return Err(format!(
            "{TARGET_BYTES_ENV} must be within {MIN_TARGET_BYTES}..={MAX_TARGET_BYTES}, observed {value}",
        ));
    }
    Ok(value)
}

fn parse_env_u64(name: &str) -> TestResult<u64> {
    let value = env::var(name).map_err(|error| format!("reading {name} failed: {error}"))?;
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name} value {value:?}: {error}"))
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
        .init_worker_size(RUNTIME_WORKERS)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning WAL stress phase failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("WAL stress phase exceeded {timeout:?}: {error}"))?
}

fn run_phase_process(
    phase: Phase,
    root: &Path,
    seed: u64,
    target_bytes: usize,
    timeout: Duration,
) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating WAL stress test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase.name())
        .env(ROOT_ENV, root)
        .env(SEED_ENV, seed.to_string())
        .env(TARGET_BYTES_ENV, target_bytes.to_string())
        .spawn()
        .map_err(|error| {
            format!(
                "spawning WAL stress phase {} for seed {seed:#018x} failed: {error}",
                phase.name(),
            )
        })?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "WAL stress phase {} for seed {seed:#018x} exited with {status}",
            phase.name(),
        ))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking WAL stress child failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("WAL stress child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn unique_temp_root(seed_index: usize, seed: u64) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_store_wal_stress_{seed_index}_{seed:016x}_{}_{}",
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
