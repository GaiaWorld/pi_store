//! 基于文件的事务提交日志（commit log）与检查点管理。
//!
//! 正常调用链是 `append -> flush -> 发布业务状态 -> confirm`：`append` 同时把事务登记到
//! 当前检查点，`flush` 等待预写日志（write-ahead log，WAL）同步，`confirm` 只在事务已经
//! 不再需要该 WAL 恢复时撤销登记。检查点轮换必须在同一把 `check_points` 锁内完成“提交旧
//! 内存块、切换物理文件、发布新检查点”，从而保证登记归属和实际文件归属一致。
//!
//! 底层 [`LogFile`] 仍支持按物理文件大小自动分裂，但本模块的普通 `flush` 会关闭该能力；
//! `CommitLogger` 是其检查点文件边界的唯一发布者。延迟提交仍使用 `LogFile::append` 返回的
//! 真实日志编号，过期定时器只能检查自己原批次是否完成，不能强制同步后继批次。

use std::convert::TryInto;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc,
                atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}};

use futures::future::{FutureExt, BoxFuture};
use async_lock::Mutex;
use bytes::BufMut;

use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{AsyncRuntime, multi_thread::MultiTaskRuntime}};
use pi_async_transaction::AsyncCommitLog;

use crate::log_store::log_file::{PairLoader, LogMethod, LogFile, log_file_name_to_usize, PairLoaderExt};

#[cfg(feature = "wal-trace")]
use crate::wal_trace::{f, kind, Logger as TraceLogger, Span, Trace};

///
/// 默认的提交日志的文件大小，为了防止自动生成新的可写文件，所以默认为最大
///
const DEFAULT_COMMIT_LOG_FILE_SIZE: usize = 16 * 1024 * 1024 * 1024;

///
/// 默认的提交日志加载缓冲区大小，单位字节
///
const DEFAULT_LOAD_BUFFER_LEN: u64 = 8192;

///
/// 默认的提交日志的块大小，单位B
///
const DEFAULT_COMMIT_LOG_BLOCK_SIZE: usize = 8192;

///
/// 默认的延迟提交的超时时长，单位ms
///
const DEFAULT_DELAY_COMMIT_TIMEOUT: usize = 1;

///
/// 默认的提交日志生成可写文件长度的最大限制，单位B
///
const DEFAULT_COMMIT_LOG_FILE_MAX_LIMIT: u64 = 32 * 1024 * 1024;

///
/// 默认的提交日志记录器的定时整理间隔时长，单位ms
///
const DEFAULT_COMMIT_LOG_COLLECT_INTERVAL: usize = 10 * 1000;

/// 带日志方法和块同步时间的提交日志重播扩展。
pub trait CommitLoggerExt: AsyncCommitLog {
    /// 开始重播，逐条回调事务唯一编号、日志方法、块同步时间和负载。
    ///
    /// `Some(...)` 表示一条重播记录，最终的 `None` 表示输入结束。成功返回重播记录数及按本
    /// 模块口径统计的字节数。回调同步执行；记录回调的错误会被包装为 `ErrorKind::Other` 返回，
    /// 结束标记 `None` 的回调返回值按历史语义被忽略。无论成功、空日志还是回调失败，调用方仍
    /// 必须按 [`AsyncCommitLog::finish_replay`] 的协议显式结束重播状态。
    fn start_replay_ext<B, F>(&self, callback: Arc<F>)
        -> BoxFuture<'static, Result<(usize, usize)>>
    where B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
          F: Fn(Option<(Self::Cid, LogMethod, u64, B)>) -> Result<()> + Send + Sync + 'static;
}

/// [`CommitLogger`] 的配置构建器。
///
/// 配置方法消费并返回构建器。越界数值不会报错或截断到最近边界，而是恢复为对应默认值；
/// 只有 [`CommitLoggerBuilder::build`] 执行目录和文件 I/O。
pub struct CommitLoggerBuilder {
    rt:                 MultiTaskRuntime<()>,   //异步运行时
    path:               PathBuf,                //提交日志记录器的日志文件所在路径
    log_block_limit:    usize,                  //日志文件的块大小限制，单位字节
    delay_timeout:      usize,                  //延迟刷新提交日志的时间，单位毫秒
    log_file_limit:     u64,                    //日志文件的可写文件大小限制，单位字节
    collect_interval:   usize,                  //提交日志记录器的定时整理间隔时长，单位毫秒
}

// SAFETY: 构建器只持有可跨线程共享的运行时句柄、路径和整数配置，且构建前没有内部后台任务
// 或别名可变引用。保留历史显式实现，避免在本次 WAL 语义修复中改变类型边界。
unsafe impl Send for CommitLoggerBuilder {}
unsafe impl Sync for CommitLoggerBuilder {}

impl CommitLoggerBuilder {
    /// 使用给定多线程运行时和 WAL 目录创建默认配置。
    ///
    /// 本方法不访问文件系统。默认块阈值为 8 KiB，延迟提交时限为 1 ms，上层检查点文件软
    /// 限制为 32 MiB，后台整理间隔为 10 s。
    pub fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                               dir: P) -> Self {
        CommitLoggerBuilder {
            rt,
            path: dir.as_ref().to_path_buf(),
            log_block_limit: DEFAULT_COMMIT_LOG_BLOCK_SIZE,
            delay_timeout: DEFAULT_DELAY_COMMIT_TIMEOUT,
            log_file_limit: DEFAULT_COMMIT_LOG_FILE_MAX_LIMIT,
            collect_interval: DEFAULT_COMMIT_LOG_COLLECT_INTERVAL,
        }
    }

    /// 设置内存日志块的批量提交软阈值，单位为字节。
    ///
    /// 合法闭区间为 2 KiB 到 32 MiB；越界值恢复为默认 8 KiB。达到或超过阈值的刷新调用
    /// 可以成为大小提交所有者（size owner），同步整个当前块。
    pub fn log_block_limit(mut self, mut limit: usize) -> Self {
        if limit < 2048 || limit > 32 * 1024 * 1024 {
            limit = DEFAULT_COMMIT_LOG_BLOCK_SIZE
        }

        self.log_block_limit = limit;
        self
    }

    /// 设置低流量批次的延迟提交时限，单位为毫秒。
    ///
    /// 合法闭区间为 1 到 10 ms；越界值恢复为默认 1 ms。它是运行时定时器的软时限，任务
    /// 调度和全局提交锁排队可能使实际完成时间更晚。
    pub fn delay_timeout(mut self, mut timeout: usize) -> Self {
        if timeout < 1 || timeout > 10 {
            timeout = DEFAULT_DELAY_COMMIT_TIMEOUT
        }

        self.delay_timeout = timeout;
        self
    }

    /// 设置 `CommitLogger` 检查点文件的轮换软阈值，单位为字节。
    ///
    /// 合法闭区间为 2 MiB 到 2 GiB；越界值恢复为默认 32 MiB。追加跨过阈值不会在热路径
    /// 立即切换文件；确认操作或后台整理观察到阈值后，才在检查点锁内执行受控轮换。
    pub fn log_file_limit(mut self, mut limit: u64) -> Self {
        if limit < 2 * 1024 * 1024 || limit > 2 * 1024 * 1024 * 1024 {
            limit = DEFAULT_COMMIT_LOG_FILE_MAX_LIMIT;
        }

        self.log_file_limit = limit;
        self
    }

    /// 设置后台检查点整理循环的间隔，单位为毫秒。
    ///
    /// 合法闭区间为 5 s 到 5 min；越界值恢复为默认 10 s。
    pub fn collect_interval(mut self, mut interval: usize) -> Self {
        if interval < 5 * 1000 || interval > 5 * 60 * 1000 {
            interval = DEFAULT_COMMIT_LOG_COLLECT_INTERVAL;
        }

        self.collect_interval = interval;
        self
    }

    /// 打开 WAL 目录、初始化检查点状态并启动后台整理任务。
    ///
    /// 构建失败返回文件系统或日志打开错误。成功后，所有 `CommitLogger` 克隆共享同一状态；
    /// 后台整理任务也持有一个克隆并按配置持续运行，当前 API 没有显式关闭接口。
    pub async fn build(mut self) -> Result<CommitLogger> {
        let file = LogFile::open(self.rt.clone(),
                                 self.path.clone(),
                                 self.log_block_limit,
                                 DEFAULT_COMMIT_LOG_FILE_SIZE, //避免日志文件自动生成可写文件
                                 None).await?;

        let rt = self.rt;
        let delay_timeout = self.delay_timeout;
        let log_file_limit = self.log_file_limit;
        let writed_size = AtomicU64::new(0); //初始化已写入当前可写文件的字节数量
        let check_point_counter = Arc::new(AtomicU64::new(0)); //初始化可写检查点的计数器
        let check_point_path = Arc::new(file.writable_path().unwrap()); //获取可写检查点的文件路径
        let writable = SpinLock::new((check_point_counter, check_point_path)); //初始化可写检查点
        let only_reads = SpinLock::new(VecDeque::new());
        let check_points = Mutex::new(XHashMap::default());
        let is_replaying = AtomicBool::new(false); //默认没有重播
        let replay_only_reads = SpinLock::new(VecDeque::new());
        let replay_confirm_buf = SpinLock::new(VecDeque::new());
        let commit_log_count = AtomicUsize::new(0);
        let confirm_commited_count = AtomicUsize::new(0);

        let inner = InnerCommitLogger {
            #[cfg(feature = "wal-trace")]
            trace: std::sync::OnceLock::new(),
            rt: rt.clone(),
            file,
            delay_timeout,
            log_file_limit,
            writed_size,
            writable,
            only_reads,
            check_points,
            is_replaying,
            replay_only_reads,
            replay_confirm_buf,
            commit_log_count,
            confirm_commited_count,
        };
        let commit_logger = CommitLogger(Arc::new(inner));
        #[cfg(feature = "wal-trace")]
        { let trace = TraceLogger::register(commit_logger.0.file.path(),
            &commit_logger.0.file.writable_path().unwrap(), rt.get_id(), self.log_block_limit,
            delay_timeout, log_file_limit, self.collect_interval);
          let _ = commit_logger.0.trace.set(trace); }

        //启动提交日志记录器的定时整理
        let commit_logger_copy = commit_logger.clone();
        let timeout = self.collect_interval;
        let _ = rt.spawn(async move {
            loop {
                collect_commit_logger(&commit_logger_copy, timeout).await;
            }
        });

        Ok(commit_logger)
    }
}

/// 基于 [`LogFile`] 的可克隆事务提交日志记录器。
///
/// 该类型实现 [`AsyncCommitLog`]。非空 `append` 返回的句柄必须先交给 `flush`；只有成功返回
/// 才能把依赖该 WAL 的业务状态视为可发布。业务提交不再需要恢复后，应以同一个事务唯一编号
/// 调用 `confirm`。空负载被忽略并返回句柄 `0`，对应 `flush(0)` 是无 I/O 的成功空操作。
///
/// `append`、检查点轮换和确认会竞争检查点异步锁；`flush` 进一步受底层全局提交锁串行化，
/// 可能等待批量同步 I/O。重播期间的确认会先缓冲，直到显式完成重播。重复确认或未知事务编号
/// 保持无操作成功语义。
#[derive(Clone)]
pub struct CommitLogger(Arc<InnerCommitLogger>);

// SAFETY: 所有克隆通过 Arc 共享状态；事务到检查点的映射由异步锁保护，短状态由自旋锁或原子
// 变量保护，文件提交委托给 LogFile 的串行化边界。本次修复没有新增或改变 unsafe 状态。
unsafe impl Send for CommitLogger {}
unsafe impl Sync for CommitLogger {}

#[cfg(feature = "wal-trace")]
impl CommitLogger {
    fn trace(&self, kind: usize) -> Trace {
        Trace::new(self.0.trace.get().unwrap_or(&TraceLogger::default()), kind)
    }
}

impl AsyncCommitLog for CommitLogger {
    type C = usize;
    type Cid = Guid;

    fn append<B>(&self, commit_uid: Self::Cid, log: B) -> BoxFuture<'static, Result<Self::C>>
        where B: BufMut + AsRef<[u8]> + Send + Sized + 'static {
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::APPEND);
        #[cfg(feature = "wal-trace")]
        { let now = trace.now(); if let Some(r) = trace.primary_mut() { r.set(f::START, now); r.set(f::CALL_START, now); r.cid(commit_uid.0); } }
        let logger = self.clone();

        async move {
            #[cfg(feature = "wal-trace")]
            let mut span = trace.root();
            #[cfg(feature = "wal-trace")]
            { span.mark(f::FIRST_POLL); span.set(f::BYTES, log.as_ref().len() as u64); }
            if log.as_ref().len() == 0 {
                //无效的提交日志，则忽略
                #[cfg(feature = "wal-trace")]
                { span.set(f::UID, 0); span.finish("success"); }
                return Ok(0);
            }

            #[cfg(feature = "wal-trace")]
            span.mark(f::CHECK_WAIT);
            let mut check_pointes_locked = logger.0.check_points.lock().await;
            #[cfg(feature = "wal-trace")]
            { span.mark(f::CHECK_ACQUIRED); span.mark(f::APPEND_START); }

            //追加指定的提交日志
            #[cfg(not(feature = "wal-trace"))]
            let log_handle = logger.0.file.append(LogMethod::PlainAppend,
                                                  commit_uid.0.to_le_bytes().as_ref(),
                                                  log.as_ref());
            #[cfg(feature = "wal-trace")]
            let log_handle = logger.0.file.append_observed(LogMethod::PlainAppend,
                commit_uid.0.to_le_bytes().as_ref(), log.as_ref(), &mut span);
            #[cfg(feature = "wal-trace")]
            { span.mark(f::APPEND_END); span.set(f::UID, log_handle as u64); }

            //增加已写入当前可写文件的字节数量
            logger.0.writed_size.fetch_add(log.as_ref().len() as u64 + 16, Ordering::Relaxed);
            //增加提交日志的数量
            logger.0.commit_log_count.fetch_add(1, Ordering::Relaxed);

            //注册本次事务到检查点表
            {
                let (counter, path) = &*logger.0.writable.lock();
                counter.fetch_add(1, Ordering::AcqRel); //增加可写检查点未确认事务的计数
                check_pointes_locked.insert(commit_uid, (counter.clone(), path.clone()));
                #[cfg(feature = "wal-trace")]
                { span.mark(f::REGISTER_END); span.shared_path(path.clone()); }
            }
            drop(check_pointes_locked);
            #[cfg(feature = "wal-trace")]
            span.finish("success");

            Ok(log_handle)
        }.boxed()
    }

    fn flush(&self, log_handle: Self::C) -> BoxFuture<'static, Result<()>> {
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::FLUSH);
        #[cfg(feature = "wal-trace")]
        if let Some(r) = trace.primary_mut() { r.set(f::UID, log_handle as u64); }
        let mut logger = self.clone();

        async move {
            #[cfg(feature = "wal-trace")]
            { let mut span = trace.root(); span.begin(); let id = span.id();
              let result = logger.0.file.delay_commit_observed(log_handle, logger.0.delay_timeout, span.trace, id).await;
              span.result(&result); return result; }
            // CommitLogger 的 checkpoint 是事务到物理 WAL 文件的唯一所有权来源，文件轮换
            // 必须统一由 new_check_point 在 check_points 锁内执行。底层 LogFile 的大小阈值
            // 自动分裂在这里必须关闭，否则可能在上层发布新 checkpoint 前改变物理文件。
            #[cfg(not(feature = "wal-trace"))]
            logger.0.file.delay_commit_without_auto_split(log_handle,
                                                          false,
                                                          logger.0.delay_timeout).await
        }.boxed()
    }

    fn confirm(&self, commit_uid: Self::Cid) -> BoxFuture<'static, Result<()>> {
        if self.0.is_replaying.load(Ordering::Relaxed) {
            //提交日志记录器正在重播，则确认提交的提交唯一id将会被缓冲，并立即返回
            //等待重播完成后，再确认
            return self.confirm_replay(commit_uid);
        }

        let logger = self.clone();
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::CONFIRM);
        #[cfg(feature = "wal-trace")]
        if let Some(r) = trace.primary_mut() { r.cid(commit_uid.0); r.mode = "normal"; }
        async move {
            confirm_normal(&logger, commit_uid,
                #[cfg(feature = "wal-trace")] &mut trace,
                #[cfg(feature = "wal-trace")] None).await
        }.boxed()
    }

    fn start_replay<B, F>(&self, mut callback: Arc<F>) -> BoxFuture<'static, Result<(usize, usize)>>
        where B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
              F: Fn(Self::Cid, B) -> Result<()> + Send + Sync + 'static {
        self.0.is_replaying.store(true, Ordering::SeqCst); //设置为正在重播
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::REPLAY);
        let commit_logger = self.clone();

        async move {
            #[cfg(feature = "wal-trace")]
            let mut span = trace.root();
            #[cfg(feature = "wal-trace")]
            { span.rec().mode = "read"; span.begin(); }
            let result = async {
            if let Some(writable_path) = commit_logger.0.file.writable_path() {
                //提交日志记录器，当前有可写日志文件
                match writable_path.metadata() {
                    Err(e) => {
                        //获取提交日志记录器的当前可写日志文件的元信息失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, path: {:?}, reason: {:?}", writable_path, e)));
                    },
                    Ok(meta) => {
                        //获取提交日志记录器的当前可写日志文件的元信息成功
                        if meta.len() == 0 && commit_logger.0.file.readable_amount() == 0 {
                            //提交日志记录器的当前没有提交日志，则停止重播，并立即返回
                            return Ok((0, 0));
                        }
                    }
                }
            }

            //提交日志记录器当前有未确认的提交日志，则开始重播
            //首先强制生成新的可写文件，以保证所有需要重播的提交日志文件都是只读日志文件
            if let Err(e) = commit_logger.0.file.split_inner(
                #[cfg(feature = "wal-trace")] { let id = span.id(); Some((span.trace, id, "replay")) }).await {
                //强制生成新的可写文件失败，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, reason: {:?}", e)));
            }

            //设置需要重播的所有有效的只读日志文件
            let mut invalid_only_read_paths = Vec::new(); //无效的只读日志文件路径列表
            let mut only_read_paths = commit_logger.0.file.all_readable_path();
            for only_read_path in only_read_paths {
                match only_read_path.metadata() {
                    Err(e) => {
                        //获取只读日志文件的元信息失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, path: {:?}, reason: {:?}", only_read_path, e)));
                    },
                    Ok(meta) => {
                        //获取只读日志文件的元信息成功
                        if meta.len() == 0 {
                            //只读日志文件没有内容，则不将无效的只读日志文件追加到需要重播的提交日志的只读日志文件路径列表
                            //注意不要在重播完成之前将无效的只读日志文件设置为备份的只读日志文件，这会导致日志文件无法正常加载只读日志文件
                            invalid_only_read_paths.push(only_read_path);
                            continue;
                        }
                    }
                }

                //将有效的只读日志文件追加到需要重播的提交日志的只读日志文件路径列表
                commit_logger.0.replay_only_reads.lock().push_back(only_read_path);
            }
            if let Some(path) = commit_logger.0.replay_only_reads.lock().pop_front() {
                //存在需要重播的只读日志文件，则将需要重播的首个只读日志文件，设置为首个可写检查点
                *commit_logger.0.writable.lock() = (Arc::new(AtomicU64::new(0)), Arc::new(path));
            }

            //构建提交日志加载器
            let mut loader = CommitLoggerLoader {
                logger: commit_logger.clone(),
                buf: Vec::new(),
                log_file: None,
                callback,
                result: Ok((0, 0)),
                marker: PhantomData,
            };

            //从前往后的加载提交日志
            if let Err(e) = commit_logger.0.file.load_before(&mut loader,
                                                             None,
                                                             DEFAULT_LOAD_BUFFER_LEN,
                                                             true).await {
                //加载提交日志错误，则立即返回错误原因
                return Err(e);
            }

            //将无效的只读日志文件设置为备份的只读日志文件
            for invalid_only_read_path in invalid_only_read_paths {
                if let Err(e) = commit_logger
                    .0
                    .file.readable_to_back_inner(invalid_only_read_path.clone(),
                        #[cfg(feature = "wal-trace")] { let id = span.id(); Some((span.trace, id, "replay_empty_cleanup")) })
                    .await {
                    //将无效的只读日志文件设置为备份的只读日志文件错误，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, path: {:?}, reason: {:?}", invalid_only_read_path, e)));
                }
            }

            loader.result()
            }.await;
            #[cfg(feature = "wal-trace")]
            { span.result(&result); if let Ok((records, bytes)) = &result { span.set(f::RETURN_COUNT, *records as u64); span.set(f::RETURN_BYTES, *bytes as u64); } }
            result
        }.boxed()
    }

    fn append_replay<B>(&self, commit_uid: Self::Cid, _log: B) -> BoxFuture<'static, Result<Self::C>>
        where B: BufMut + AsRef<[u8]> + Send + Sized + 'static {
        let logger = self.clone();

        async move {
            let mut check_pointes_locked = logger.0.check_points.lock().await;

            //重播将忽略追加提交日志，但必须注册本次重播事务到检查点表
            let (counter, path) = &*logger.0.writable.lock();
            counter.fetch_add(1, Ordering::AcqRel); //增加可写检查点未确认事务的计数
            check_pointes_locked.insert(commit_uid, (counter.clone(), path.clone()));

            //增加提交日志的数量
            logger.0.commit_log_count.fetch_add(1, Ordering::Relaxed);

            Ok(0)
        }.boxed()
    }

    fn flush_replay(&self, _log_handle: Self::C) -> BoxFuture<'static, Result<()>> {
        async move {
            //重播忽略追加提交日志，则忽略刷新提交日志
            Ok(())
        }.boxed()
    }

    fn confirm_replay(&self, commit_uid: Self::Cid) -> BoxFuture<'static, Result<()>> {
        let logger = self.clone();
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::CONFIRM);
        #[cfg(feature = "wal-trace")]
        if let Some(r) = trace.primary_mut() { r.cid(commit_uid.0); r.mode = "replay_buffered"; }

        async move {
            #[cfg(feature = "wal-trace")]
            let mut span = trace.root();
            #[cfg(feature = "wal-trace")]
            span.begin();
            //重播时的确认提交日志，不允许直接确认，需要缓冲确认的提交唯一id，并在完成重播时统一确认
            logger.0.replay_confirm_buf.lock().push_back(commit_uid);
            #[cfg(feature = "wal-trace")]
            span.finish("success");
            Ok(())
        }.boxed()
    }

    fn finish_replay(&self) -> BoxFuture<'static, Result<()>> {
        let logger = self.clone();
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::REPLAY);

        async move {
            #[cfg(feature = "wal-trace")]
            let mut span = trace.root();
            #[cfg(feature = "wal-trace")]
            { span.rec().mode = "finish"; span.begin(); }
            let result = async {
            //设置为已完成重播
            logger.0.is_replaying.store(false, Ordering::SeqCst);

            //执行重播时缓冲的确认提交日志
            let replay_confirms = &mut *logger.0.replay_confirm_buf.lock();
            #[cfg(feature = "wal-trace")]
            { span.set(f::BUFFERED, replay_confirms.len() as u64); span.set(f::CONFIRMED, 0); }
            while let Some(commit_uid) = replay_confirms.pop_front() {
                #[cfg(not(feature = "wal-trace"))]
                let _ = logger.confirm(commit_uid).await?;
                #[cfg(feature = "wal-trace")]
                {
                    // 原同步入口每次读取模式；不缓存状态，不提前释放重播缓冲锁。
                    if logger.0.is_replaying.load(Ordering::Relaxed) {
                        let mut child = span.child(kind::CONFIRM, "", None);
                        child.rec().mode = "replay_buffered"; child.rec().cid(commit_uid.0); child.begin();
                        logger.0.replay_confirm_buf.lock().push_back(commit_uid);
                        child.finish("success");
                    } else {
                        let id = span.id(); confirm_normal(&logger, commit_uid, span.trace, Some(id)).await?;
                    }
                    let count = span.rec().get(f::CONFIRMED).unwrap_or(0) + 1; span.set(f::CONFIRMED, count);
                }
            }

            Ok(())
            }.await;
            #[cfg(feature = "wal-trace")]
            span.result(&result);
            result
        }.boxed()
    }

    fn check_point_of(&self, commit_uid: Self::Cid) -> BoxFuture<'static, Option<usize>> {
        let logger = self.clone();

        async move {
            let check_point_path = if let Some((_counter, check_point_path)) = logger.0.check_points.lock().await.get(&commit_uid) {
                check_point_path.as_ref().clone()
            } else {
                return None;
            };

            if let Some(file_name) = check_point_path.file_name() {
                if let Some(file_name_str) = file_name.to_str() {
                    return log_file_name_to_usize(file_name_str);
                }
            }

            None
        }.boxed()
    }

    fn current_check_point(&self) -> BoxFuture<'static, usize> {
        let logger = self.clone();

        async move {
            // AsyncCommitLog 的历史命名沿用至今：该值实际是 LogFile 下一次分裂将分配的物理
            // 文件编号，而非当前可写文件名。当前检查点文件编号通常是返回值减一；分裂失败会
            // 消耗编号，因此调用方若需要事务的权威归属，应使用 check_point_of。
            logger
                .0
                .file
                .current_log_index()
        }.boxed()
    }

    fn append_check_point(&self) -> BoxFuture<'static, Result<usize>> {
        let logger = self.clone();
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::MAINTENANCE);

        async move {
            #[cfg(feature = "wal-trace")]
            let mut parent = trace.root();
            // 立即生成新的可写检查点，并把上一个可写检查点标为尚未全部确认。返回值是实际
            // 新建文件编号，与 current_check_point 的“下一待分配编号”口径不同。
            let _check_pointes_locked = logger.0.check_points.lock().await;
            new_check_point_inner(&logger, false,
                #[cfg(feature = "wal-trace")] Some((&mut parent, "explicit_append_checkpoint"))).await
        }.boxed()
    }

    fn waiting_confirm_count(&self) -> BoxFuture<'static, usize> {
        let logger = self.clone();

        async move {
            logger
                .0
                .check_points
                .lock()
                .await
                .len()
        }.boxed()
    }

    fn append_total_count(&self) -> usize {
        self
            .0
            .commit_log_count
            .load(Ordering::Relaxed)
    }

    fn confirm_total_count(&self) -> usize {
        self
            .0
            .confirm_commited_count
            .load(Ordering::Relaxed)
    }
}

// 为提交日志文件异步创建新的可写检查点。
//
// 所有调用点都必须先持有 check_points 异步锁。该锁不仅保护事务到检查点的映射，还阻止
// append 在“提交当前块 -> 分裂文件 -> 发布新检查点”之间注册新事务，因此这个维护序列对
// CommitLogger 来说是原子的；锁内不持有 writable/only_reads 自旋锁跨 await。
//
// 当前块必须先成功落入当前映射对应的旧文件，才能切换 LogFile 的 writable。否则 append
// 已登记到旧检查点、尚未 flush 的事务会在 split 后写入新文件，确认回收时可能把实际 WAL
// 标记为 .bak，导致 repair/replay 忽略仍未完成持久化的事务。辅助提交失败时禁止 split，
// 原检查点映射和 writable 均保持不变；但底层 commit 的既有 I/O 失败语义不会恢复已经交换
// 出的内存块，磁盘、文件系统或 runtime 失败后的事务安全不属于本层保证，不能据此重试。
//
// commit_pending_block 自身通过独立任务完成内部指针归还和等待者（waiter）唤醒；后续
// split 仍沿用 LogFile 的既有取消边界。调用方不得在 split 的文件创建 await 中途取消
// checkpoint future。
async fn new_check_point(logger: &CommitLogger,
                         is_finish_confirm: bool) -> Result<usize> {
    new_check_point_inner(logger, is_finish_confirm,
        #[cfg(feature = "wal-trace")] None).await
}

async fn new_check_point_inner(logger: &CommitLogger, is_finish_confirm: bool,
    #[cfg(feature = "wal-trace")] parent: Option<(&mut Span<'_>, &'static str)>) -> Result<usize> {
    #[cfg(feature = "wal-trace")]
    let mut disabled = Trace::disabled();
    #[cfg(feature = "wal-trace")]
    let mut span = match parent {
        Some((parent, trigger)) => { let link = if parent.rec().get(f::ROTATION1).is_none() { f::ROTATION1 } else { f::ROTATION2 }; parent.child(kind::ROTATE, trigger, Some(link)) }
        None => disabled.span(kind::ROTATE, 0, ""),
    };
    #[cfg(feature = "wal-trace")]
    { span.set(f::MARK_OLD, u64::from(is_finish_confirm)); span.rec().stage = "pending_commit"; span.begin(); }
    let pending = logger.0.file.commit_pending_inner(
        #[cfg(feature = "wal-trace")] Some(&mut span)).await;
    #[cfg(feature = "wal-trace")]
    { if let Some(start) = span.rec().get(f::START) { let elapsed = span.now().saturating_sub(start); span.set(f::PENDING_NS, elapsed); }
      if pending.is_err() { span.result(&pending); } }
    pending?;
    #[cfg(feature = "wal-trace")]
    { span.rec().stage = "split"; }
    let split = logger.0.file.split_inner(
        #[cfg(feature = "wal-trace")] { let id = span.id(); Some((span.trace, id, "checkpoint")) }).await;
    #[cfg(feature = "wal-trace")]
    if split.is_err() { span.result(&split); }
    let log_index = split?; //当前块已落入旧检查点后，才允许生成新的可写文件
    #[cfg(feature = "wal-trace")]
    { span.set(f::NEW_FILE, log_index as u64); span.rec().stage = "publish_checkpoint"; }

    //设置新的可写检查点
    let check_point_counter = Arc::new(AtomicU64::new(0)); //初始化可写检查点的计数器
    let check_point_path = Arc::new(logger.0.file.writable_path().unwrap()); //获取可写检查点的文件路径
    *logger.0.writable.lock() = (check_point_counter, check_point_path);

    //将上一个可写检查点的日志文件追加到只读检查点的文件路径列表，等待这个检查点的所有事务的提交确认
    let only_read_path = logger.0.file.last_readable_path();
    #[cfg(feature = "wal-trace")]
    if let Some(index) = only_read_path.file_name().and_then(|n| n.to_str()).and_then(|n| n.split('.').next()).and_then(|n| n.parse::<u64>().ok()) { span.set(f::OLD_FILE, index); }
    logger.0.only_reads.lock().push_back((only_read_path, is_finish_confirm));

    //重置新的可写日志文件的已写入字节数量
    logger.0.writed_size.store(0, Ordering::Relaxed);

    #[cfg(feature = "wal-trace")]
    span.finish("success");
    Ok(log_index)
}

// 与原 confirm 共用唯一业务体。诊断上下文显式借用，重播调用不会绕过祖先暂存区。
async fn confirm_normal(logger: &CommitLogger, commit_uid: Guid,
    #[cfg(feature = "wal-trace")] trace: &mut Trace,
    #[cfg(feature = "wal-trace")] parent: Option<u64>) -> Result<()> {
    #[cfg(feature = "wal-trace")]
    let mut span = match parent { Some(id) => trace.span(kind::CONFIRM, id, ""), None => trace.root() };
    #[cfg(feature = "wal-trace")]
    { span.rec().cid(commit_uid.0); span.rec().mode = "normal"; span.begin(); }
    let mut check_pointes_locked = logger.0.check_points.lock().await;
    #[cfg(feature = "wal-trace")]
    let mut check_pointes_locked = span.hold(check_pointes_locked, false, true);

    if logger.0.writed_size.load(Ordering::Relaxed) >= logger.0.log_file_limit {
        // 原语义：容量轮换先于 CID 查找，轮换错误不改变 confirm 的返回值。
        let _ = new_check_point_inner(logger, false,
            #[cfg(feature = "wal-trace")] Some((&mut check_pointes_locked.span, "confirm_size_limit"))).await;
    }
    let registration = check_pointes_locked.remove(&commit_uid);
    #[cfg(feature = "wal-trace")]
    check_pointes_locked.span.set(f::FOUND, u64::from(registration.is_some()));
    if let Some((counter, check_point_path)) = registration {
        logger.0.confirm_commited_count.fetch_add(1, Ordering::Relaxed);
        let pending = counter.fetch_sub(1, Ordering::AcqRel);
        #[cfg(feature = "wal-trace")]
        { check_pointes_locked.span.set(f::PENDING_COUNT, pending); check_pointes_locked.span.shared_path(check_point_path.clone()); }
        if pending == 1 {
            if check_point_path.as_ref() == logger.0.writable.lock().1.as_ref() {
                let _ = new_check_point_inner(logger, true,
                    #[cfg(feature = "wal-trace")] Some((&mut check_pointes_locked.span, "confirm_checkpoint_drained"))).await;
            }
            let mut swap = VecDeque::new();
            {
                let only_reads = &mut *logger.0.only_reads.lock();
                for (path, is_finish_confirm) in only_reads.iter_mut() {
                    if check_point_path.as_ref() == path { *is_finish_confirm = true; }
                    else { match path.metadata() {
                        Err(e) => warn!("Confirm commited transaction failed, path: {:?}, reason: {:?}", path, e),
                        Ok(meta) => {
                            // 空检查点没有 CID 推进确认；仍须服从下面的队首顺序门禁。
                            if meta.len() == 0 { *is_finish_confirm = true; }
                        }
                    } }
                }
                let mut prev = true;
                while let Some((path, is_finish_confirm)) = only_reads.pop_front() {
                    if prev && is_finish_confirm {
                        let result = logger.0.file.readable_to_back_inner(path,
                            #[cfg(feature = "wal-trace")] { let id = check_pointes_locked.span.id(); Some((check_pointes_locked.span.trace, id, "confirm_cleanup")) }).await;
                        #[cfg(feature = "wal-trace")]
                        if let Err(e) = &result { check_pointes_locked.span.rec().error(e); }
                        result?;
                    } else if !prev { swap.push_back((path, is_finish_confirm)); }
                    else { swap.push_back((path, is_finish_confirm)); prev = false; }
                }
            }
            *logger.0.only_reads.lock() = swap;
        }
    }
    #[cfg(feature = "wal-trace")]
    { check_pointes_locked.span.rec().outcome = "success"; }
    Ok(())
}

// 整理提交日志记录器，在重播时不允许整理
async fn collect_commit_logger(logger: &CommitLogger, timeout: usize) {
    //等待指定时长后，开始整理提交日志记录器
    logger.0.rt.timeout(timeout).await;

    if logger.0.is_replaying.load(Ordering::Relaxed) {
        //如果提交日志记录器，当前正在重播，则忽略整理
        return;
    }

    #[cfg(feature = "wal-trace")]
    let mut trace = logger.trace(kind::MAINTENANCE);
    #[cfg(feature = "wal-trace")]
    let mut parent = trace.root();
    //获取检查点表的异步锁
    let check_pointes_locked = logger.0.check_points.lock().await;

    //检查是否需要生成新的可写检查点
    if logger.0.writed_size.load(Ordering::Relaxed) >= logger.0.log_file_limit {
        //提交日志的当前可写检查点对应的可写文件，已写入字节数量已达限制
        //则立即强制生成新的可写检查点，并设置上一个可写检查点的状态为未完成确认
        new_check_point_inner(&logger, false,
            #[cfg(feature = "wal-trace")] Some((&mut parent, "background_size_limit"))).await;
    }

    drop(check_pointes_locked); //立即释放检查点表的异步锁
}

impl CommitLoggerExt for CommitLogger {
    fn start_replay_ext<B, F>(&self, mut callback: Arc<F>)
                              -> BoxFuture<'static, Result<(usize, usize)>>
    where B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
          F: Fn(Option<(Self::Cid, LogMethod, u64, B)>) -> Result<()> + Send + Sync + 'static
    {
        self.0.is_replaying.store(true, Ordering::SeqCst); //设置为正在重播
        #[cfg(feature = "wal-trace")]
        let mut trace = self.trace(kind::REPLAY);
        let commit_logger = self.clone();

        async move {
            #[cfg(feature = "wal-trace")]
            let mut span = trace.root();
            #[cfg(feature = "wal-trace")]
            { span.rec().mode = "read_ext"; span.begin(); }
            let result = async {
            if let Some(writable_path) = commit_logger.0.file.writable_path() {
                //提交日志记录器，当前有可写日志文件
                match writable_path.metadata() {
                    Err(e) => {
                        //获取提交日志记录器的当前可写日志文件的元信息失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Replay commit log failed, path: {:?}, reason: {:?}",
                                                      writable_path,
                                                      e)));
                    },
                    Ok(meta) => {
                        //获取提交日志记录器的当前可写日志文件的元信息成功
                        if meta.len() == 0 && commit_logger.0.file.readable_amount() == 0 {
                            //提交日志记录器的当前没有提交日志，则停止重播，并立即返回
                            return Ok((0, 0));
                        }
                    }
                }
            }

            //提交日志记录器当前有未确认的提交日志，则开始重播
            //首先强制生成新的可写文件，以保证所有需要重播的提交日志文件都是只读日志文件
            if let Err(e) = commit_logger.0.file.split_inner(
                #[cfg(feature = "wal-trace")] { let id = span.id(); Some((span.trace, id, "replay_ext")) }).await {
                //强制生成新的可写文件失败，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Replay commit log failed, reason: {:?}",
                                              e)));
            }

            //设置需要重播的所有有效的只读日志文件
            let mut invalid_only_read_paths = Vec::new(); //无效的只读日志文件路径列表
            let mut only_read_paths = commit_logger.0.file.all_readable_path();
            for only_read_path in only_read_paths {
                match only_read_path.metadata() {
                    Err(e) => {
                        //获取只读日志文件的元信息失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Replay commit log failed, path: {:?}, reason: {:?}",
                                                      only_read_path,
                                                      e)));
                    },
                    Ok(meta) => {
                        //获取只读日志文件的元信息成功
                        if meta.len() == 0 {
                            //只读日志文件没有内容，则不将无效的只读日志文件追加到需要重播的提交日志的只读日志文件路径列表
                            //注意不要在重播完成之前将无效的只读日志文件设置为备份的只读日志文件，这会导致日志文件无法正常加载只读日志文件
                            invalid_only_read_paths.push(only_read_path);
                            continue;
                        }
                    }
                }

                //将有效的只读日志文件追加到需要重播的提交日志的只读日志文件路径列表
                commit_logger.0.replay_only_reads.lock().push_back(only_read_path);
            }
            if let Some(path) = commit_logger.0.replay_only_reads.lock().pop_front() {
                //存在需要重播的只读日志文件，则将需要重播的首个只读日志文件，设置为首个可写检查点
                *commit_logger.0.writable.lock() = (Arc::new(AtomicU64::new(0)), Arc::new(path));
            }

            //构建提交日志加载器
            let mut loader = CommitLoggerLoaderExt {
                logger: commit_logger.clone(),
                buf: Vec::new(),
                log_file: None,
                callback,
                result: Ok((0, 0)),
                marker: PhantomData,
            };

            //从前往后的加载提交日志
            if let Err(e) = commit_logger.0.file.load_before_with_payload_time(&mut loader,
                                                                               None,
                                                                               DEFAULT_LOAD_BUFFER_LEN,
                                                                               true).await {
                //加载提交日志错误，则立即返回错误原因
                return Err(e);
            }

            //将无效的只读日志文件设置为备份的只读日志文件
            for invalid_only_read_path in invalid_only_read_paths {
                if let Err(e) = commit_logger
                    .0
                    .file.readable_to_back_inner(invalid_only_read_path.clone(),
                        #[cfg(feature = "wal-trace")] { let id = span.id(); Some((span.trace, id, "replay_ext_empty_cleanup")) })
                    .await {
                    //将无效的只读日志文件设置为备份的只读日志文件错误，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Replay commit log failed, path: {:?}, reason: {:?}",
                                                  invalid_only_read_path,
                                                  e)));
                }
            }

            loader.result()
            }.await;
            #[cfg(feature = "wal-trace")]
            { span.result(&result); if let Ok((records, bytes)) = &result { span.set(f::RETURN_COUNT, *records as u64); span.set(f::RETURN_BYTES, *bytes as u64); } }
            result
        }.boxed()
    }
}

// 基于日志文件的内部提交日志记录器
struct InnerCommitLogger {
    #[cfg(feature = "wal-trace")]
    trace:                  std::sync::OnceLock<TraceLogger>,
    rt:                     MultiTaskRuntime<()>,                                   //异步运行时
    file:                   LogFile,                                                //日志文件
    delay_timeout:          usize,                                                  //延迟刷新提交日志的时间，单位毫秒
    log_file_limit:         u64,                                                    //日志文件的可写文件的最大限制
    writed_size:            AtomicU64,                                              //已写入当前可写文件的字节数量
    writable:               SpinLock<(Arc<AtomicU64>, Arc<PathBuf>)>,               //提交日志记录器的可写检查点
    only_reads:             SpinLock<VecDeque<(PathBuf, bool)>>,                    //提交日志记录器的只读检查点的文件路径列表
    check_points:           Mutex<XHashMap<Guid, (Arc<AtomicU64>, Arc<PathBuf>)>>,  //提交日志记录器的检查点表
    is_replaying:           AtomicBool,                                             //是否正在重播
    replay_only_reads:      SpinLock<VecDeque<PathBuf>>,                            //需要重播的提交日志的只读日志文件路径列表
    replay_confirm_buf:     SpinLock<VecDeque<Guid>>,                               //已确认的重播事务的提交唯一id缓冲区
    commit_log_count:       AtomicUsize,                                            //提交日志的数量
    confirm_commited_count: AtomicUsize,                                            //确认提交的数量
}

// 提交日志加载器
struct CommitLoggerLoader<
    B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
    F: Fn(Guid, B) -> Result<()> + Send + 'static,
> {
    logger:     CommitLogger,           //提交日志记录器
    buf:        Vec<(Guid, Vec<u8>)>,   //提交日志缓冲区
    log_file:   Option<PathBuf>,        //当前正在加载的日志文件路径
    callback:   Arc<F>,                 //提交日志的重播回调
    result:     Result<(usize, usize)>, //加载的结果
    marker:     PhantomData<B>,
}

impl<
    B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
    F: Fn(Guid, B) -> Result<()> + Send + 'static,
> PairLoader for CommitLoggerLoader<B, F> {
    fn is_require(&self, _log_file: Option<&PathBuf>, _key: &Vec<u8>) -> bool {
        //提交日志的所有日志都需要加载
        true
    }

    fn load(&mut self,
            log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        if self.result.is_err() {
            //如果加载结果已经设置为错误，则忽略后续的所有加载
            return;
        }

        if let Some(log_file) = log_file {
            if self.log_file.is_none() {
                //正在加载首个日志文件的首个键值对，则设置当前正在加载的日志文件路径到提交日志加载器
                self.log_file = Some(log_file.clone());
            }

            if self.log_file.as_ref().unwrap() != log_file {
                //提交日志加载器正在加载的日志文件与正在加载的日志文件不相同
                //则表示已加载完一个日志文件，则从提交日志加载器的日志缓冲区的栈顶开始弹出所有待重播的提交日志，并同步执行重播回调
                while let Some((commit_uid, log)) = self.buf.pop() {
                    //执行重播回调
                    if let Err(e) = (self.callback)(commit_uid.clone(), B::from(log)) {
                        //执行重播回调失败，则立即设置错误原因
                        self.result = Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, commit_uid: {:?}, reason: {:?}", commit_uid, e)));
                    }
                }

                //重置当前正在加载的日志文件路径到提交日志加载器
                self.log_file = Some(log_file.clone());

                //已重播完成当前的日志文件，则将下一个需要重播的提交日志，设置为可写检查点
                //保证下一个加载的日志文件，在追加重播的提交日志时，使用对应的可写检查点
                next_check_point(&self.logger);
            }

            //将加载的日志写入提交日志加载器的日志缓冲区
            let uid = u128::from_le_bytes(key.try_into().unwrap());
            let commit_uid = Guid(uid);
            if let Some(log) = value {
                //更新加载结果
                if let Ok((log_count, bytes_count)) = self.result {
                    self.result = Ok((log_count + 1, bytes_count + 16 + log.len()));
                }

                self.buf.push((commit_uid, log));
            }
        }
    }
}

// 为重播提交日志，将下一个需要重播的提交日志，设置为可写检查点
// 设置上一个可写检查点是否已完成确认，并将上一个可写检查点追加到只读检查点的文件路径列表
fn next_check_point(logger: &CommitLogger) {
    {
        //将上一个可写检查点的日志文件追加到只读检查点的文件路径列表，等待这个检查点的所有重播事务的提交确认
        let (_, last_writable_path) = &*logger.0.writable.lock();
        let only_read_path = last_writable_path.as_ref().clone();
        logger.0.only_reads.lock().push_back((only_read_path, false));
    }

    if let Some(path) = logger.0.replay_only_reads.lock().pop_front() {
        //设置新的可写检查点
        let check_point_counter = Arc::new(AtomicU64::new(0)); //初始化可写检查点的计数器
        let check_point_path = Arc::new(path); //设置下一个需要重播的提交日志的只读日志文件为可写检查点的文件路径
        *logger.0.writable.lock() = (check_point_counter, check_point_path);
    } else {
        //已经重播完提交日志的所有只读日志文件，则将提交日志的可写日志文件，并设置为新的可写检查点
        let check_point_counter = Arc::new(AtomicU64::new(0)); //初始化可写检查点的计数器
        let check_point_path = Arc::new(logger.0.file.writable_path().unwrap()); //获取可写检查点的文件路径
        *logger.0.writable.lock() = (check_point_counter, check_point_path);
    }
}

impl<
    B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
    F: Fn(Guid, B) -> Result<()> + Send + 'static,
> CommitLoggerLoader<B, F> {
    //获取加载结果
    pub fn result(mut self) -> Result<(usize, usize)> {
        if self.buf.len() > 0 {
            //加载缓冲区未清空，则表示只加载了一个提交日志的日志文件
            //则从提交日志加载器的日志缓冲区的栈顶开始弹出所有待重播的提交日志，并同步执行重播回调
            while let Some((commit_uid, log)) = self.buf.pop() {
                //执行重播回调
                if let Err(e) = (self.callback)(commit_uid.clone(), B::from(log)) {
                    //执行重播回调失败，则立即设置错误原因
                    self.result = Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, commit_uid: {:?}, reason: {:?}", commit_uid, e)));
                }
            }

            //所有的需要重播的日志文件已重播完成，则将提交日志的当前可写文件，设置为新的可写检查点
            //也保证了所有被重播的日志文件，成为提交日志的只读日志文件
            next_check_point(&self.logger);
        }

        self.result
    }
}

#[cfg(test)]
mod checkpoint_rotation_tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64 as TestAtomicU64, AtomicUsize as TestAtomicUsize},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use crossbeam_channel::bounded;
    use pi_async_rt::rt::{
        multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
        startup_global_time_loop,
        AsyncRuntime,
    };

    use super::*;

    const PHYSICAL_FILE_LIMIT: usize = 1024 * 1024;
    const CURRENT_BLOCK_LIMIT: usize = 2 * 1024 * 1024;
    const TEST_TIMEOUT: Duration = Duration::from_secs(30);

    /// 这是局部实现分支测试，只验证构建器公开参数的边界归一化；它不代替真实文件系统专项。
    #[test]
    fn test_commit_logger_builder_parameter_boundaries() {
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(1)
            .build();
        let path = PathBuf::from("unused-builder-boundary-path");

        let defaults = CommitLoggerBuilder::new(rt.clone(), &path);
        assert_eq!(defaults.path, path);
        assert_eq!(defaults.log_block_limit, DEFAULT_COMMIT_LOG_BLOCK_SIZE);
        assert_eq!(defaults.delay_timeout, DEFAULT_DELAY_COMMIT_TIMEOUT);
        assert_eq!(defaults.log_file_limit, DEFAULT_COMMIT_LOG_FILE_MAX_LIMIT);
        assert_eq!(defaults.collect_interval, DEFAULT_COMMIT_LOG_COLLECT_INTERVAL);

        for (input, expected) in [
            (2 * 1024 - 1, DEFAULT_COMMIT_LOG_BLOCK_SIZE),
            (2 * 1024, 2 * 1024),
            (32 * 1024 * 1024, 32 * 1024 * 1024),
            (32 * 1024 * 1024 + 1, DEFAULT_COMMIT_LOG_BLOCK_SIZE),
        ] {
            assert_eq!(
                CommitLoggerBuilder::new(rt.clone(), &path)
                    .log_block_limit(input)
                    .log_block_limit,
                expected,
                "unexpected block-limit normalization for {input}",
            );
        }

        for (input, expected) in [
            (0, DEFAULT_DELAY_COMMIT_TIMEOUT),
            (1, 1),
            (10, 10),
            (11, DEFAULT_DELAY_COMMIT_TIMEOUT),
        ] {
            assert_eq!(
                CommitLoggerBuilder::new(rt.clone(), &path)
                    .delay_timeout(input)
                    .delay_timeout,
                expected,
                "unexpected delay-timeout normalization for {input}",
            );
        }

        for (input, expected) in [
            (2 * 1024 * 1024 - 1, DEFAULT_COMMIT_LOG_FILE_MAX_LIMIT),
            (2 * 1024 * 1024, 2 * 1024 * 1024),
            (2 * 1024 * 1024 * 1024, 2 * 1024 * 1024 * 1024),
            (2 * 1024 * 1024 * 1024 + 1, DEFAULT_COMMIT_LOG_FILE_MAX_LIMIT),
        ] {
            assert_eq!(
                CommitLoggerBuilder::new(rt.clone(), &path)
                    .log_file_limit(input)
                    .log_file_limit,
                expected,
                "unexpected file-limit normalization for {input}",
            );
        }

        for (input, expected) in [
            (5 * 1000 - 1, DEFAULT_COMMIT_LOG_COLLECT_INTERVAL),
            (5 * 1000, 5 * 1000),
            (5 * 60 * 1000, 5 * 60 * 1000),
            (5 * 60 * 1000 + 1, DEFAULT_COMMIT_LOG_COLLECT_INTERVAL),
        ] {
            assert_eq!(
                CommitLoggerBuilder::new(rt.clone(), &path)
                    .collect_interval(input)
                    .collect_interval,
                expected,
                "unexpected collect-interval normalization for {input}",
            );
        }
    }

    #[test]
    fn test_commit_logger_checkpoint_crosses_logfile_limit_once() {
        let _time_loop = startup_global_time_loop(1);
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(1)
            .build();
        let root = unique_test_root();
        fs::create_dir_all(&root)
            .expect("creating checkpoint size-boundary test root must succeed");

        let (sender, receiver) = bounded(1);
        let test_rt = rt.clone();
        let test_root = root.clone();
        rt.spawn(async move {
            let result = async {
                verify_size_boundary(test_rt.clone(), test_root.join("commit-logger")).await?;
                verify_public_delay_commit_boundary(test_rt, test_root.join("public-log-file")).await
            }
            .await;
            let _ = sender.send(result);
        })
        .expect("spawning checkpoint size-boundary test must succeed");

        receiver
            .recv_timeout(TEST_TIMEOUT)
            .expect("checkpoint size-boundary test must finish within 30 seconds")
            .unwrap_or_else(|error| panic!("checkpoint size-boundary test failed: {error}"));
        fs::remove_dir_all(&root)
            .expect("cleaning checkpoint size-boundary test root must succeed");
    }

    async fn verify_size_boundary(
        rt: MultiTaskRuntime<()>,
        root: PathBuf,
    ) -> std::result::Result<(), String> {
        let logger = build_small_physical_file_logger(rt, root.clone()).await?;
        let old_path = logger
            .0
            .file
            .writable_path()
            .ok_or_else(|| "small-limit logger omitted initial writable file".to_owned())?;
        let first_uid = Guid(0x7101);
        let second_uid = Guid(0x7102);

        let first_handle = logger
            .append(first_uid.clone(), vec![0x61; 768 * 1024])
            .await
            .map_err(|error| format!("appending first WAL failed: {error}"))?;
        logger
            .flush(first_handle)
            .await
            .map_err(|error| format!("flushing first WAL failed: {error}"))?;
        let old_len_before = logger.0.file.writable_size();
        if old_len_before == 0 || old_len_before >= PHYSICAL_FILE_LIMIT {
            return Err(format!(
                "first WAL must leave the physical file below its limit: len={old_len_before}, limit={PHYSICAL_FILE_LIMIT}",
            ));
        }
        if logger.0.file.writable_path().as_ref() != Some(&old_path) {
            return Err("CommitLogger flush must not auto-split the physical WAL".to_owned());
        }

        let crossing_payload_len = PHYSICAL_FILE_LIMIT - old_len_before + 64 * 1024;
        let second_handle = logger
            .append(second_uid.clone(), vec![0x71; crossing_payload_len])
            .await
            .map_err(|error| format!("appending threshold-crossing WAL failed: {error}"))?;
        let new_checkpoint = logger
            .append_check_point()
            .await
            .map_err(|error| format!("rotating threshold-crossing checkpoint failed: {error}"))?;
        let new_path = logger
            .0
            .file
            .writable_path()
            .ok_or_else(|| "checkpoint rotation omitted new writable file".to_owned())?;

        if new_path == old_path {
            return Err("checkpoint rotation must replace the physical writable file".to_owned());
        }
        if logger.0.file.readable_amount() != 1 {
            return Err(format!(
                "threshold-crossing checkpoint must split exactly once: readable_amount={}",
                logger.0.file.readable_amount(),
            ));
        }
        let parsed_checkpoint = new_path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(log_file_name_to_usize)
            .ok_or_else(|| format!("new checkpoint path is invalid: {new_path:?}"))?;
        if parsed_checkpoint != new_checkpoint {
            return Err(format!(
                "returned checkpoint must identify the actual writable file: returned={new_checkpoint}, actual={parsed_checkpoint}",
            ));
        }
        let old_len_after = old_path
            .metadata()
            .map_err(|error| format!("reading old WAL metadata failed: {error}"))?
            .len() as usize;
        if old_len_after <= PHYSICAL_FILE_LIMIT {
            return Err(format!(
                "pending WAL must be written to the old file before its single split: len={old_len_after}",
            ));
        }
        let new_len = new_path
            .metadata()
            .map_err(|error| format!("reading new WAL metadata failed: {error}"))?
            .len();
        if new_len != 0 {
            return Err(format!(
                "new checkpoint must not contain pre-rotation WAL: len={new_len}",
            ));
        }

        logger
            .flush(second_handle)
            .await
            .map_err(|error| format!("flushing helper-committed WAL failed: {error}"))?;
        logger
            .confirm(second_uid)
            .await
            .map_err(|error| format!("confirming second WAL failed: {error}"))?;
        if !old_path.exists() {
            return Err("old WAL must remain active until every registered transaction confirms".to_owned());
        }
        logger
            .confirm(first_uid)
            .await
            .map_err(|error| format!("confirming first WAL failed: {error}"))?;

        let mut backup_path = old_path.clone();
        if !backup_path.set_extension("bak") {
            return Err(format!("old WAL path cannot form a backup path: {old_path:?}"));
        }
        if old_path.exists() {
            return Err("fully confirmed old WAL must no longer remain active".to_owned());
        }
        let backup_len = backup_path
            .metadata()
            .map_err(|error| format!("confirmed old WAL backup is missing: {error}"))?
            .len() as usize;
        if backup_len != old_len_after {
            return Err(format!(
                "confirmed backup must preserve the exact old WAL bytes: active={old_len_after}, backup={backup_len}",
            ));
        }
        if logger.waiting_confirm_count().await != 0 ||
            logger.append_total_count() != 2 ||
            logger.confirm_total_count() != 2 {
            return Err(format!(
                "checkpoint accounting did not close: waiting={}, appended={}, confirmed={}",
                logger.waiting_confirm_count().await,
                logger.append_total_count(),
                logger.confirm_total_count(),
            ));
        }

        Ok(())
    }

    // 生产修复只允许 CommitLogger 的私有 flush 关闭底层自动分裂；公开 LogFile 路径必须
    // 保留原有物理阈值语义。同时，重复刷新已提交句柄不得占住延迟提交所有权（delay
    // ownership），否则下一笔未达块阈值的 WAL 只会登记等待者（waiter），而旧句柄定时任务
    // 命中已提交快路后无人唤醒它。
    async fn verify_public_delay_commit_boundary(
        rt: MultiTaskRuntime<()>,
        path: PathBuf,
    ) -> std::result::Result<(), String> {
        let file = LogFile::open(rt.clone(),
                                 path,
                                 CURRENT_BLOCK_LIMIT,
                                 PHYSICAL_FILE_LIMIT,
                                 None)
            .await
            .map_err(|error| format!("opening public LogFile boundary fixture failed: {error}"))?;
        let old_path = file
            .writable_path()
            .ok_or_else(|| "public LogFile omitted initial writable path".to_owned())?;
        let first_value = vec![0x81; PHYSICAL_FILE_LIMIT + 64 * 1024];
        let first_handle = file.append(LogMethod::PlainAppend, b"first", &first_value);
        if first_handle == 0 {
            return Err("non-empty public LogFile append returned handle 0".to_owned());
        }
        file.delay_commit(first_handle, false, 1)
            .await
            .map_err(|error| format!("public threshold delay commit failed: {error}"))?;

        // delay_commit 的等待者（waiter）在 WAL 写入成功后、所有者（owner）完成自动分裂
        // 前被唤醒；这里按公开既有语义只做有界观察，不能把返回值误当成文件切换的同步屏障。
        let mut new_path = file
            .writable_path()
            .ok_or_else(|| "public threshold split omitted writable path".to_owned())?;
        for _ in 0..100 {
            if new_path != old_path && file.readable_amount() == 1 {
                break;
            }
            rt.timeout(1).await;
            new_path = file
                .writable_path()
                .ok_or_else(|| "public threshold split lost writable path".to_owned())?;
        }
        if new_path == old_path || file.readable_amount() != 1 {
            return Err(format!(
                "public delay commit must auto-split exactly once: old={old_path:?}, new={new_path:?}, readable={}",
                file.readable_amount(),
            ));
        }
        let old_len = old_path
            .metadata()
            .map_err(|error| format!("reading public old WAL metadata failed: {error}"))?
            .len() as usize;
        if old_len <= PHYSICAL_FILE_LIMIT || file.writable_size() != 0 {
            return Err(format!(
                "public auto-split file sizes are invalid: old={old_len}, new={}",
                file.writable_size(),
            ));
        }

        file.delay_commit(first_handle, false, 10)
            .await
            .map_err(|error| format!("repeating committed public handle failed: {error}"))?;
        let second_handle = file.append(LogMethod::PlainAppend, b"second", b"value");

        // `0` 是类型系统可表达但不属于 LogFile 正常协议的输入。它只应命中初始已提交水位并
        // 幂等返回，绝不能恢复 v0.11.1 的“绕过水位并提交调用时 current”特殊权限。
        file.delay_commit(0, false, 1)
            .await
            .map_err(|error| format!("delaying invalid zero handle failed: {error}"))?;
        file.commit(0, true, false, None)
            .await
            .map_err(|error| format!("committing invalid zero handle failed: {error}"))?;
        if file.commited_uid() != first_handle || file.writable_size() != 0 {
            return Err(format!(
                "zero handle must not commit the successor block: committed={}, expected={}, writable_len={}",
                file.commited_uid(),
                first_handle,
                file.writable_size(),
            ));
        }

        file.delay_commit(second_handle, false, 10)
            .await
            .map_err(|error| format!("public successor delay commit failed: {error}"))?;
        if file.commited_uid() != second_handle || file.writable_path().as_ref() != Some(&new_path) {
            return Err(format!(
                "public successor did not close on the same writable file: committed={}, expected={}, writable={:?}, expected_path={new_path:?}",
                file.commited_uid(),
                second_handle,
                file.writable_path(),
            ));
        }
        if file.writable_size() == 0 || file.readable_amount() != 1 {
            return Err(format!(
                "public successor produced an unexpected split or empty write: writable_len={}, readable={}",
                file.writable_size(),
                file.readable_amount(),
            ));
        }

        // 公开 commit 的显式分裂标志与大小阈值自动分裂是两条独立语义。这里用一个远低于
        // 阈值的真实句柄要求同步并分裂：调用返回时旧 writable 必须已经成为第二个 readable，
        // 新文件必须为空，且提交水位只能推进到第三个真实句柄。
        let explicit_old_path = file
            .writable_path()
            .ok_or_else(|| "public explicit-split fixture lost writable path".to_owned())?;
        let third_handle = file.append(LogMethod::PlainAppend, b"third", b"explicit-split");
        file.commit(third_handle, true, true, None)
            .await
            .map_err(|error| format!("public explicit-split commit failed: {error}"))?;
        let explicit_new_path = file
            .writable_path()
            .ok_or_else(|| "public explicit split omitted new writable path".to_owned())?;
        if explicit_new_path == explicit_old_path ||
            file.readable_amount() != 2 ||
            file.writable_size() != 0 ||
            file.commited_uid() != third_handle {
            return Err(format!(
                "public explicit split state mismatch: old={explicit_old_path:?}, new={explicit_new_path:?}, readable={}, writable_len={}, committed={}, expected={third_handle}",
                file.readable_amount(),
                file.writable_size(),
                file.commited_uid(),
            ));
        }
        if explicit_old_path
            .metadata()
            .map_err(|error| format!("reading explicit-split old WAL failed: {error}"))?
            .len() == 0 {
            return Err("public explicit split did not persist its target WAL".to_owned());
        }

        // `split()` 本身只切换物理文件，不提交 current。先追加第四条、直接 split，再提交该
        // 真实句柄：被缓冲的数据必须写入 split 后的新文件，证明公开 API 的两步组合边界，
        // 同时避免把 split 错写成隐式持久化屏障。
        let buffered_before_split = explicit_new_path;
        let fourth_handle = file.append(LogMethod::PlainAppend, b"fourth", b"after-raw-split");
        let raw_split_index = file
            .split()
            .await
            .map_err(|error| format!("public raw split failed: {error}"))?;
        let buffered_after_split = file
            .writable_path()
            .ok_or_else(|| "public raw split omitted new writable path".to_owned())?;
        let parsed_raw_split_index = buffered_after_split
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(log_file_name_to_usize)
            .ok_or_else(|| format!("public raw split returned an invalid path: {buffered_after_split:?}"))?;
        if buffered_after_split == buffered_before_split ||
            file.readable_amount() != 3 ||
            file.commited_uid() != third_handle ||
            parsed_raw_split_index != raw_split_index ||
            buffered_before_split
                .metadata()
                .map_err(|error| format!("reading raw-split old WAL failed: {error}"))?
                .len() != 0 {
            return Err(format!(
                "raw split unexpectedly persisted current: returned={raw_split_index}, old={buffered_before_split:?}, new={buffered_after_split:?}, readable={}, committed={}, expected={third_handle}",
                file.readable_amount(),
                file.commited_uid(),
            ));
        }
        file.commit(fourth_handle, true, false, None)
            .await
            .map_err(|error| format!("committing after public raw split failed: {error}"))?;
        if file.commited_uid() != fourth_handle ||
            file.writable_path().as_ref() != Some(&buffered_after_split) ||
            file.writable_size() == 0 {
            return Err(format!(
                "buffered WAL did not persist to the post-split file: committed={}, expected={fourth_handle}, writable={:?}, expected_path={buffered_after_split:?}, len={}",
                file.commited_uid(),
                file.writable_path(),
                file.writable_size(),
            ));
        }

        Ok(())
    }

    async fn build_small_physical_file_logger(
        rt: MultiTaskRuntime<()>,
        path: PathBuf,
    ) -> std::result::Result<CommitLogger, String> {
        let file = LogFile::open(rt.clone(),
                                 path,
                                 CURRENT_BLOCK_LIMIT,
                                 PHYSICAL_FILE_LIMIT,
                                 None)
            .await
            .map_err(|error| format!("opening small-limit LogFile failed: {error}"))?;
        let check_point_path = Arc::new(
            file.writable_path()
                .ok_or_else(|| "small-limit LogFile omitted writable path".to_owned())?,
        );
        let check_point_counter = Arc::new(TestAtomicU64::new(0));

        Ok(CommitLogger(Arc::new(InnerCommitLogger {
            #[cfg(feature = "wal-trace")]
            trace: std::sync::OnceLock::new(),
            rt,
            file,
            delay_timeout: 1,
            log_file_limit: u64::MAX,
            writed_size: TestAtomicU64::new(0),
            writable: SpinLock::new((check_point_counter, check_point_path)),
            only_reads: SpinLock::new(VecDeque::new()),
            check_points: Mutex::new(XHashMap::default()),
            is_replaying: AtomicBool::new(false),
            replay_only_reads: SpinLock::new(VecDeque::new()),
            replay_confirm_buf: SpinLock::new(VecDeque::new()),
            commit_log_count: TestAtomicUsize::new(0),
            confirm_commited_count: TestAtomicUsize::new(0),
        })))
    }

    fn unique_test_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after UNIX_EPOCH")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "pi-store-checkpoint-size-boundary-{}-{nonce}",
            std::process::id(),
        ))
    }
}

// 扩展的提交日志加载器
struct CommitLoggerLoaderExt<
    B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
    F: Fn(Option<(Guid, LogMethod, u64, B)>) -> Result<()> + Send + 'static,
> {
    logger:     CommitLogger,                           //提交日志记录器
    buf:        Vec<(Guid, LogMethod, u64, Vec<u8>)>,   //提交日志缓冲区
    log_file:   Option<PathBuf>,                        //当前正在加载的日志文件路径
    callback:   Arc<F>,                                 //提交日志的重播回调
    result:     Result<(usize, usize)>,                 //加载的结果
    marker:     PhantomData<B>,
}

impl<
    B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
    F: Fn(Option<(Guid, LogMethod, u64, B)>) -> Result<()> + Send + 'static,
> PairLoaderExt for CommitLoggerLoaderExt<B, F> {
    fn is_require(&self,
                  _log_file: Option<&PathBuf>,
                  _payload_time: u64,
                  _key: &Vec<u8>) -> bool {
        //提交日志的所有日志都需要加载
        true
    }

    fn load(&mut self,
            log_file: Option<&PathBuf>,
            method: LogMethod,
            payload_time: u64,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        if self.result.is_err() {
            //如果加载结果已经设置为错误，则忽略后续的所有加载
            return;
        }

        if let Some(log_file) = log_file {
            if self.log_file.is_none() {
                //正在加载首个日志文件的首个键值对，则设置当前正在加载的日志文件路径到提交日志加载器
                self.log_file = Some(log_file.clone());
            }

            if self.log_file.as_ref().unwrap() != log_file {
                //提交日志加载器正在加载的日志文件与正在加载的日志文件不相同
                //则表示已加载完一个日志文件，则从提交日志加载器的日志缓冲区的栈顶开始弹出所有待重播的提交日志，并同步执行重播回调
                while let Some((commit_uid, method, time, log)) = self.buf.pop() {
                    //执行重播回调
                    if let Err(e) = (self.callback)(Some((commit_uid.clone(), method, time, B::from(log)))) {
                        //执行重播回调失败，则立即设置错误原因
                        self.result = Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, commit_uid: {:?}, reason: {:?}", commit_uid, e)));
                    }
                }

                //重置当前正在加载的日志文件路径到提交日志加载器
                self.log_file = Some(log_file.clone());

                //已重播完成当前的日志文件，则将下一个需要重播的提交日志，设置为可写检查点
                //保证下一个加载的日志文件，在追加重播的提交日志时，使用对应的可写检查点
                next_check_point(&self.logger);
            }

            //将加载的日志写入提交日志加载器的日志缓冲区
            let uid = u128::from_le_bytes(key.try_into().unwrap());
            let commit_uid = Guid(uid);
            if let Some(log) = value {
                //更新加载结果
                if let Ok((log_count, bytes_count)) = self.result {
                    self.result = Ok((log_count + 1, bytes_count + 16 + log.len()));
                }

                self.buf.push((commit_uid, method, payload_time, log));
            }
        }
    }
}

impl<
    B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
    F: Fn(Option<(Guid, LogMethod, u64, B)>) -> Result<()> + Send + 'static,
> CommitLoggerLoaderExt<B, F> {
    //获取加载结果
    pub fn result(mut self) -> Result<(usize, usize)> {
        if self.buf.len() > 0 {
            //加载缓冲区未清空，则表示只加载了一个提交日志的日志文件
            //则从提交日志加载器的日志缓冲区的栈顶开始弹出所有待重播的提交日志，并同步执行重播回调
            while let Some((commit_uid, method, time, log)) = self.buf.pop() {
                //执行重播回调
                if let Err(e) = (self.callback)(Some((commit_uid.clone(), method, time, B::from(log)))) {
                    //执行重播回调失败，则立即设置错误原因
                    self.result = Err(Error::new(ErrorKind::Other,
                                                 format!("Replay commit log failed, commit_uid: {:?}, reason: {:?}",
                                                         commit_uid,
                                                         e)));
                }
            }

            //所有的需要重播的日志文件已重播完成，则将提交日志的当前可写文件，设置为新的可写检查点
            //也保证了所有被重播的日志文件，成为提交日志的只读日志文件
            next_check_point(&self.logger);
        }

        //加载已完成
        (self.callback)(None);
        self.result
    }
}
