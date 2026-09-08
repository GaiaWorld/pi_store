//! 资源有界、逐配置串行运行的真实 WAL 提交基准。
//!
//! 与默认 `libtest` 基准不同，本文件使用 `harness = false`，每次执行固定预热和固定样本量，
//! 因而不会由统计框架无上限地重复磁盘写入。父进程按固定顺序为每个配置启动独立子进程；
//! 每个子进程独占运行时、`CommitLogger` 和临时目录，退出并清理后才开始下一项。配置内的并发
//! 仅用于形成真实提交锁竞争，不代表多个独立基准并行执行。

use std::{
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
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

type BenchResult<T = ()> = Result<T, String>;

const CONFIG_ENV: &str = "PI_STORE_WAL_BENCH_CONFIG";
const ROOT_ENV: &str = "PI_STORE_WAL_BENCH_ROOT";
const SAMPLE_BYTES_ENV: &str = "PI_STORE_WAL_BENCH_SAMPLE_BYTES";
const DEFAULT_SAMPLE_BYTES: usize = 8 * 1024 * 1024;
const MIN_SAMPLE_BYTES: usize = 1024 * 1024;
const MAX_SAMPLE_BYTES: usize = 16 * 1024 * 1024;
const MAX_WARMUP_BYTES: usize = 2 * 1024 * 1024;
const SAMPLE_COUNT: usize = 3;
const RUNTIME_WORKERS: usize = 4;
const DIRECTORY_LIMIT: usize = 64 * 1024 * 1024;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(90);
const FULL_TIMEOUT: Duration = Duration::from_secs(300);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(80);

#[derive(Clone, Copy, Debug)]
enum PayloadMode {
    Fixed(usize),
    Mixed,
}

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    id: &'static str,
    block_limit: usize,
    payload: PayloadMode,
    concurrency: usize,
    index: usize,
}

const CONFIGS: [BenchConfig; 5] = [
    BenchConfig {
        id: "BENCH-01",
        block_limit: 8 * 1024,
        payload: PayloadMode::Fixed(256),
        concurrency: 64,
        index: 1,
    },
    BenchConfig {
        id: "BENCH-02",
        block_limit: 8 * 1024,
        payload: PayloadMode::Fixed(16 * 1024),
        concurrency: 64,
        index: 2,
    },
    BenchConfig {
        id: "BENCH-03",
        block_limit: 8 * 1024,
        payload: PayloadMode::Fixed(16 * 1024),
        concurrency: 256,
        index: 3,
    },
    BenchConfig {
        id: "BENCH-04",
        block_limit: 1024 * 1024,
        payload: PayloadMode::Fixed(16 * 1024),
        concurrency: 256,
        index: 4,
    },
    BenchConfig {
        id: "BENCH-05",
        block_limit: 2 * 1024 * 1024,
        payload: PayloadMode::Mixed,
        concurrency: 64,
        index: 5,
    },
];

fn main() {
    if let Ok(config_id) = env::var(CONFIG_ENV) {
        install_abort_on_any_panic();
        let config = CONFIGS
            .iter()
            .copied()
            .find(|config| config.id == config_id)
            .unwrap_or_else(|| panic!("unknown WAL benchmark configuration: {config_id}"));
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("WAL benchmark child must receive its root path"),
        );
        let sample_bytes = parse_sample_bytes().unwrap_or_else(|error| panic!("{error}"));
        if let Err(error) = run_on_runtime(RUNTIME_TIMEOUT, move |rt| async move {
            run_config(rt, root, config, sample_bytes).await
        }) {
            panic!("{} failed: {error}", config.id);
        }
        return;
    }

    if let Err(error) = run_all_configs() {
        eprintln!("WAL_COMMIT_BENCH_FAILED {error}");
        std::process::exit(1);
    }
}

fn run_all_configs() -> BenchResult<()> {
    let sample_bytes = parse_sample_bytes()?;
    let started = Instant::now();
    println!(
        "WAL_BENCH_ENV workers={RUNTIME_WORKERS} samples={SAMPLE_COUNT} sample_bytes={sample_bytes} warmup_bytes={} directory_limit={DIRECTORY_LIMIT} page_cache=per-config-warmup-no-system-cache-drop serial=true",
        sample_bytes.min(MAX_WARMUP_BYTES),
    );

    for config in CONFIGS {
        let elapsed = started.elapsed();
        if elapsed >= FULL_TIMEOUT {
            return Err(format!(
                "full benchmark exceeded {FULL_TIMEOUT:?} before {}",
                config.id,
            ));
        }
        let root = unique_temp_root(config.id);
        fs::create_dir_all(&root)
            .map_err(|error| format!("creating {} benchmark root failed: {error}", config.id))?;
        let remaining = FULL_TIMEOUT - elapsed;
        let timeout = std::cmp::min(PROCESS_TIMEOUT, remaining);
        if let Err(error) = run_config_process(config, &root, sample_bytes, timeout) {
            return Err(format!(
                "{} failed; evidence is preserved at {root:?}: {error}",
                config.id,
            ));
        }
        fs::remove_dir_all(&root)
            .map_err(|error| format!("cleaning {} benchmark root failed: {error}", config.id))?;
    }

    println!(
        "WAL_BENCH_COMPLETE configs={} elapsed_ms={} serial=true",
        CONFIGS.len(),
        started.elapsed().as_millis(),
    );
    Ok(())
}

async fn run_config(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    config: BenchConfig,
    sample_bytes: usize,
) -> BenchResult<()> {
    let wal = root.join("wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal)
        .log_block_limit(config.block_limit)
        .delay_timeout(1)
        .log_file_limit(512 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building real CommitLogger failed: {error}"))?;

    let warmup_bytes = sample_bytes.min(MAX_WARMUP_BYTES);
    let mut next_sequence = 0usize;
    let warmup_lengths =
        build_payload_lengths(config.payload, warmup_bytes, seed_for(config, usize::MAX));
    let warmup = run_workload(&rt, &logger, config, warmup_lengths, &mut next_sequence).await?;

    let mut all_uids = warmup.uids;
    let mut all_latencies = Vec::new();
    let mut measured_payload = 0usize;
    let mut measured_operations = 0usize;
    let mut measured_elapsed = Duration::ZERO;
    let mut total_payload = warmup.payload_bytes;
    let mut total_operations = warmup.operations;

    for sample in 0..SAMPLE_COUNT {
        let lengths = build_payload_lengths(config.payload, sample_bytes, seed_for(config, sample));
        let sample_started = Instant::now();
        let result = run_workload(&rt, &logger, config, lengths, &mut next_sequence).await?;
        let sample_elapsed = sample_started.elapsed();
        println!(
            "WAL_BENCH_SAMPLE config={} sample={} operations={} payload_bytes={} elapsed_us={}",
            config.id,
            sample + 1,
            result.operations,
            result.payload_bytes,
            sample_elapsed.as_micros(),
        );
        measured_elapsed += sample_elapsed;
        measured_payload += result.payload_bytes;
        measured_operations += result.operations;
        total_payload += result.payload_bytes;
        total_operations += result.operations;
        all_latencies.extend(result.flush_latencies_us);
        all_uids.extend(result.uids);
    }

    if logger.append_total_count() != total_operations {
        return Err(format!(
            "append counter mismatch: expected {total_operations}, observed {}",
            logger.append_total_count(),
        ));
    }
    for uid in all_uids {
        logger
            .confirm(uid)
            .await
            .map_err(|error| format!("confirming benchmark WAL failed: {error}"))?;
    }
    if logger.confirm_total_count() != total_operations {
        return Err(format!(
            "confirm counter mismatch: expected {total_operations}, observed {}",
            logger.confirm_total_count(),
        ));
    }
    if logger.waiting_confirm_count().await != 0 {
        return Err("benchmark left transactions waiting for confirmation".into());
    }

    // 所有 flush 已经完成；短暂等待仍在队列中的旧定时任务退出，再解析稳定的物理 WAL。
    rt.timeout(5).await;
    let physical = inspect_wal_directory(&wal)?;
    if physical.blocks == 0 {
        return Err("nonempty benchmark produced no physical WAL blocks".into());
    }
    if physical.directory_bytes > DIRECTORY_LIMIT {
        return Err(format!(
            "benchmark directory exceeded hard limit: {} > {DIRECTORY_LIMIT}",
            physical.directory_bytes,
        ));
    }

    all_latencies.sort_unstable();
    let elapsed_seconds = measured_elapsed.as_secs_f64();
    if elapsed_seconds == 0.0 {
        return Err("measured benchmark duration was zero".into());
    }
    let throughput_mib_s = measured_payload as f64 / (1024.0 * 1024.0) / elapsed_seconds;
    let operations_s = measured_operations as f64 / elapsed_seconds;
    let bytes_per_sync = physical.encoded_bytes as f64 / physical.blocks as f64;
    let payload_per_sync = total_payload as f64 / physical.blocks as f64;
    println!(
        "WAL_BENCH_RESULT config={} block_limit={} payload={:?} concurrency={} warmup_bytes={} samples={} measured_operations={} measured_payload_bytes={} elapsed_us={} throughput_mib_s={:.3} operations_s={:.1} flush_p50_us={} flush_p90_us={} flush_p95_us={} flush_p99_us={} physical_blocks={} encoded_bytes={} bytes_per_sync={:.1} payload_per_sync={:.1} directory_bytes={} total_operations={} total_payload_bytes={}",
        config.id,
        config.block_limit,
        config.payload,
        config.concurrency,
        warmup_bytes,
        SAMPLE_COUNT,
        measured_operations,
        measured_payload,
        measured_elapsed.as_micros(),
        throughput_mib_s,
        operations_s,
        percentile(&all_latencies, 50),
        percentile(&all_latencies, 90),
        percentile(&all_latencies, 95),
        percentile(&all_latencies, 99),
        physical.blocks,
        physical.encoded_bytes,
        bytes_per_sync,
        payload_per_sync,
        physical.directory_bytes,
        total_operations,
        total_payload,
    );
    Ok(())
}

struct WorkloadResult {
    uids: Vec<Guid>,
    flush_latencies_us: Vec<u128>,
    payload_bytes: usize,
    operations: usize,
}

async fn run_workload(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    config: BenchConfig,
    lengths: Vec<usize>,
    next_sequence: &mut usize,
) -> BenchResult<WorkloadResult> {
    if lengths.is_empty() {
        return Err("benchmark workload unexpectedly contains no records".into());
    }
    let payload_bytes = lengths.iter().sum();
    let operations = lengths.len();
    let mut uids = Vec::with_capacity(operations);
    let mut flush_latencies_us = Vec::with_capacity(operations);
    let mut offset = 0usize;

    while offset < lengths.len() {
        let end = std::cmp::min(offset + config.concurrency, lengths.len());
        let chunk = &lengths[offset..end];
        let (sender, receiver) = async_channel::bounded(chunk.len());
        let mut spawned = 0usize;
        let mut spawn_error = None;

        for length in chunk {
            let sequence = *next_sequence;
            *next_sequence += 1;
            let uid = Guid(
                0xb000_0000_0000_0000_0000_0000_0000_0000u128
                    ^ ((config.index as u128) << 112)
                    ^ sequence as u128,
            );
            let task_logger = logger.clone();
            let task_sender = sender.clone();
            let payload_length = *length;
            let payload_seed = seed_for(config, sequence);
            match rt.spawn(async move {
                let payload = make_payload(payload_seed, payload_length);
                let outcome = async {
                    let handle = task_logger
                        .append(uid.clone(), payload)
                        .await
                        .map_err(|error| format!("append failed: {error}"))?;
                    if handle == 0 {
                        return Err("nonempty WAL returned handle 0".to_owned());
                    }
                    let flush_started = Instant::now();
                    task_logger
                        .flush(handle)
                        .await
                        .map_err(|error| format!("flush failed: {error}"))?;
                    Ok((uid, flush_started.elapsed().as_micros()))
                }
                .await;
                let _ = task_sender.send(outcome).await;
            }) {
                Ok(_) => spawned += 1,
                Err(error) => {
                    spawn_error = Some(format!("spawning WAL operation failed: {error:?}"));
                    break;
                }
            }
        }
        drop(sender);

        let mut operation_errors = Vec::new();
        for _ in 0..spawned {
            match receiver.recv().await {
                Ok(Ok((uid, latency))) => {
                    uids.push(uid);
                    flush_latencies_us.push(latency);
                }
                Ok(Err(error)) => operation_errors.push(error),
                Err(error) => operation_errors.push(format!("result channel closed: {error}")),
            }
        }
        if let Some(error) = spawn_error {
            operation_errors.push(error);
        }
        if !operation_errors.is_empty() {
            return Err(format!(
                "{} operation errors in one chunk: {}",
                operation_errors.len(),
                operation_errors.join("; "),
            ));
        }
        if spawned != chunk.len() {
            return Err(format!(
                "spawned operation count mismatch: expected {}, observed {spawned}",
                chunk.len(),
            ));
        }
        offset = end;
    }

    if uids.len() != operations || flush_latencies_us.len() != operations {
        return Err(format!(
            "completed operation count mismatch: expected {operations}, uids={}, latencies={}",
            uids.len(),
            flush_latencies_us.len(),
        ));
    }
    Ok(WorkloadResult {
        uids,
        flush_latencies_us,
        payload_bytes,
        operations,
    })
}

fn build_payload_lengths(mode: PayloadMode, target: usize, seed: u64) -> Vec<usize> {
    match mode {
        PayloadMode::Fixed(length) => {
            let count = target.saturating_add(length - 1) / length;
            vec![length; count]
        }
        PayloadMode::Mixed => {
            let mut random = SplitMix64::new(seed);
            let mut lengths = Vec::new();
            let mut total = 0usize;
            while total < target {
                let roll = random.next_u64() % 100;
                let (min, max) = if roll < 70 {
                    (64usize, 1024usize)
                } else if roll < 95 {
                    (4 * 1024, 64 * 1024)
                } else {
                    (256 * 1024, 1024 * 1024)
                };
                let remaining = target - total;
                if remaining < 64 {
                    break;
                }
                let length = std::cmp::min(random.range_inclusive(min, max), remaining);
                lengths.push(length);
                total += length;
            }
            lengths
        }
    }
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

fn seed_for(config: BenchConfig, sample: usize) -> u64 {
    0x9e37_79b9_7f4a_7c15u64
        ^ (config.index as u64).wrapping_mul(0xa076_1d64_78bd_642f)
        ^ (sample as u64).wrapping_mul(0xe703_7ed1_a0b4_28db)
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

struct PhysicalWalStats {
    blocks: usize,
    encoded_bytes: usize,
    directory_bytes: usize,
}

fn inspect_wal_directory(path: &Path) -> BenchResult<PhysicalWalStats> {
    let mut blocks = 0usize;
    let mut encoded_bytes = 0usize;
    let mut directory_bytes = 0usize;
    for entry in fs::read_dir(path)
        .map_err(|error| format!("reading WAL benchmark directory failed: {error}"))?
    {
        let entry =
            entry.map_err(|error| format!("reading WAL directory entry failed: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("reading WAL file metadata failed: {error}"))?;
        if !metadata.is_file() {
            continue;
        }
        directory_bytes = directory_bytes
            .checked_add(metadata.len() as usize)
            .ok_or_else(|| "WAL directory size overflowed usize".to_owned())?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let numeric_stem = name
            .strip_suffix(".bak")
            .unwrap_or(&name)
            .parse::<usize>()
            .is_ok();
        if !numeric_stem || metadata.len() == 0 {
            continue;
        }
        let bytes = fs::read(entry.path())
            .map_err(|error| format!("reading physical WAL blocks failed: {error}"))?;
        blocks += count_physical_blocks(&bytes)?;
        encoded_bytes += bytes.len();
    }
    Ok(PhysicalWalStats {
        blocks,
        encoded_bytes,
        directory_bytes,
    })
}

fn count_physical_blocks(bytes: &[u8]) -> BenchResult<usize> {
    const HEADER_LEN: usize = 16;
    let mut cursor = bytes.len();
    let mut blocks = 0usize;
    while cursor > 0 {
        if cursor < HEADER_LEN {
            return Err(format!(
                "physical WAL has {cursor} trailing bytes, shorter than its header",
            ));
        }
        let payload_len = u32::from_le_bytes(
            bytes[cursor - 4..cursor]
                .try_into()
                .expect("four-byte WAL length slice must convert"),
        ) as usize;
        let block_len = payload_len
            .checked_add(HEADER_LEN)
            .ok_or_else(|| "physical WAL block length overflowed usize".to_owned())?;
        if block_len > cursor {
            return Err(format!(
                "physical WAL block length {block_len} exceeds remaining {cursor} bytes",
            ));
        }
        cursor -= block_len;
        blocks += 1;
    }
    Ok(blocks)
}

fn percentile(sorted: &[u128], percent: usize) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) * percent) / 100;
    sorted[index]
}

fn parse_sample_bytes() -> BenchResult<usize> {
    let value = match env::var(SAMPLE_BYTES_ENV) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("invalid {SAMPLE_BYTES_ENV} value {value:?}: {error}"))?,
        Err(env::VarError::NotPresent) => DEFAULT_SAMPLE_BYTES,
        Err(error) => return Err(format!("reading {SAMPLE_BYTES_ENV} failed: {error}")),
    };
    if !(MIN_SAMPLE_BYTES..=MAX_SAMPLE_BYTES).contains(&value) {
        return Err(format!(
            "{SAMPLE_BYTES_ENV} must be within {MIN_SAMPLE_BYTES}..={MAX_SAMPLE_BYTES}, observed {value}",
        ));
    }
    Ok(value)
}

fn install_abort_on_any_panic() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> BenchResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = BenchResult<T>> + Send + 'static,
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
    .map_err(|error| format!("spawning WAL benchmark configuration failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("WAL benchmark configuration exceeded {timeout:?}: {error}"))?
}

fn run_config_process(
    config: BenchConfig,
    root: &Path,
    sample_bytes: usize,
    timeout: Duration,
) -> BenchResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating WAL benchmark executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .env(CONFIG_ENV, config.id)
        .env(ROOT_ENV, root)
        .env(SAMPLE_BYTES_ENV, sample_bytes.to_string())
        .spawn()
        .map_err(|error| format!("spawning {} benchmark process failed: {error}", config.id))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "{} benchmark process exited with {status}",
            config.id
        ))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> BenchResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking WAL benchmark child failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("WAL benchmark child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn unique_temp_root(config: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_store_wal_bench_{config}_{}_{}",
        std::process::id(),
        nanos,
    ))
}
