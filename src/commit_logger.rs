use std::convert::TryInto;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc,
                atomic::{AtomicBool, AtomicU64, Ordering}};

use futures::future::{FutureExt, BoxFuture};
use async_lock::Mutex;
use bytes::BufMut;

use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{AsyncRuntime, multi_thread::MultiTaskRuntime}};
use pi_async_transaction::AsyncCommitLog;

use crate::log_store::log_file::{PairLoader, LogMethod, LogFile};

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

///
/// 基于日志文件的提交日志记录器的构建器
///
pub struct CommitLoggerBuilder {
    rt:                 MultiTaskRuntime<()>,   //异步运行时
    path:               PathBuf,                //提交日志记录器的日志文件所在路径
    log_block_limit:    usize,                  //日志文件的块大小限制，单位字节
    delay_timeout:      usize,                  //延迟刷新提交日志的时间，单位毫秒
    log_file_limit:     u64,                    //日志文件的可写文件大小限制，单位字节
    collect_interval:   usize,                  //提交日志记录器的定时整理间隔时长，单位毫秒
}

unsafe impl Send for CommitLoggerBuilder {}
unsafe impl Sync for CommitLoggerBuilder {}

impl CommitLoggerBuilder {
    /// 构建一个基于日志文件的提交日志记录器的构建器
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

    /// 设置提交日志文件的块大小限制，超过限制以后，会强制刷新可写文件
    pub fn log_block_limit(mut self, mut limit: usize) -> Self {
        if limit < 2048 || limit > 32 * 1024 * 1024 {
            limit = DEFAULT_COMMIT_LOG_BLOCK_SIZE
        }

        self.log_block_limit = limit;
        self
    }

    /// 设置提交日志文件的超时时长，提交日志文件在超时后，会强制刷新可写文件
    pub fn delay_timeout(mut self, mut timeout: usize) -> Self {
        if timeout < 1 || timeout > 10 {
            timeout = DEFAULT_DELAY_COMMIT_TIMEOUT
        }

        self.delay_timeout = timeout;
        self
    }

    /// 设置提交日志文件的大小限制，超过限制以后，会强制生成新的可写文件
    pub fn log_file_limit(mut self, mut limit: u64) -> Self {
        if limit < 2 * 1024 * 1024 || limit > 2 * 1024 * 1024 * 1024 {
            limit = DEFAULT_COMMIT_LOG_FILE_MAX_LIMIT;
        }

        self.log_file_limit = limit;
        self
    }

    /// 设置提交日志记录器的定时整理的间隔时长
    pub fn collect_interval(mut self, mut interval: usize) -> Self {
        if interval < 5 * 1000 || interval > 5 * 60 * 1000 {
            interval = DEFAULT_COMMIT_LOG_COLLECT_INTERVAL;
        }

        self.collect_interval = interval;
        self
    }

    /// 异步构建一个基于日志文件的提交日志记录器
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

        let inner = InnerCommitLogger {
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
        };
        let commit_logger = CommitLogger(Arc::new(inner));

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

///
/// 基于日志文件的提交日志记录器
///
#[derive(Clone)]
pub struct CommitLogger(Arc<InnerCommitLogger>);

unsafe impl Send for CommitLogger {}
unsafe impl Sync for CommitLogger {}

impl AsyncCommitLog for CommitLogger {
    type C = usize;
    type Cid = Guid;

    fn append<B>(&self, commit_uid: Self::Cid, log: B) -> BoxFuture<'static, Result<Self::C>>
        where B: BufMut + AsRef<[u8]> + Send + Sized + 'static {
        let logger = self.clone();

        async move {
            if log.as_ref().len() == 0 {
                //无效的提交日志，则忽略
                return Ok(0);
            }

            let mut check_pointes_locked = logger.0.check_points.lock().await;

            //追加指定的提交日志
            let log_handle = logger.0.file.append(LogMethod::PlainAppend,
                                                  commit_uid.0.to_le_bytes().as_ref(),
                                                  log.as_ref());

            //增加已写入当前可写文件的字节数量
            logger.0.writed_size.fetch_add(log.as_ref().len() as u64 + 16, Ordering::Relaxed);

            //注册本次事务到检查点表
            let (counter, path) = &*logger.0.writable.lock();
            counter.fetch_add(1, Ordering::AcqRel); //增加可写检查点未确认事务的计数
            check_pointes_locked.insert(commit_uid, (counter.clone(), path.clone()));

            Ok(log_handle)
        }.boxed()
    }

    fn flush(&self, log_handle: Self::C) -> BoxFuture<'static, Result<()>> {
        let mut logger = self.clone();

        async move {
            logger.0.file.delay_commit(log_handle,
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

        async move {
            let mut check_pointes_locked = logger.0.check_points.lock().await;

            if logger.0.writed_size.load(Ordering::Relaxed) >= logger.0.log_file_limit {
                //提交日志的当前可写检查点对应的可写文件，已写入字节数量已达限制
                //则立即强制生成新的可写检查点，并设置上一个可写检查点的状态为未完成确认
                let _ = new_check_point(&logger, false).await;
            }

            if let Some((counter, check_point_path)) = check_pointes_locked.remove(&commit_uid) {
                //从检查点表中移除已确认的事务，并减少事务对应检查点的计数
                if counter.fetch_sub(1, Ordering::AcqRel) == 1 {
                    println!("!!!!!!check_point_path: {:?}", check_point_path);
                    //当前已确认事务对应的检查点的计数已清空，则表示事务对应检查点的所有事务已完成确认
                    if check_point_path.as_ref() == logger.0.writable.lock().1.as_ref() {
                        //当前已完成确认的检查点是当前可写检查点
                        //则立即强制生成新的可写检查点，并设置上一个可写检查点的状态为已完成确认
                        let _ = new_check_point(&logger, true).await;
                    }

                    //整理只读检查点的文件路径列表中已完成确认且可以移除的只读检查点
                    let mut swap = VecDeque::new();
                    {
                        let only_reads = &mut *logger.0.only_reads.lock();
                        for (path, is_finish_confirm) in only_reads.iter_mut() {
                            println!("!!!!!!only_read_path: {:?}", path);
                            if check_point_path.as_ref() == path {
                                //当前已完成确认的检查点是只读检查点
                                *is_finish_confirm = true; //标记当前只读检查点的状态为已完成确认
                            }
                        }

                        let mut prev = true; //上一个只读检查点是否已完成确认
                        while let Some((path, is_finish_confirm)) = only_reads.pop_front() {
                            println!("!!!!!!prev: {:?}, is_finish_confirm: {:?}, path: {:?}", prev, is_finish_confirm, path);
                            if prev && is_finish_confirm {
                                //上一个只读检查点已完成确认，且当前只读检查点也完成了确认
                                //则将当前只读检查点的日志文件设置为备份的只读文件，并从只读检查点的文件路径列表中移除
                                let _ = logger.0.file.readable_to_back(path).await?;
                            } else if !prev {
                                //上一个只读检查点未完成确认，则需要等待上一个只读检查点完成确认后，再处理当前只读检查点
                                swap.push_back((path, is_finish_confirm));
                            } else {
                                //当前只读检查点未完成确认，则等待完成确认后再处理
                                swap.push_back((path, is_finish_confirm));
                                prev = false; //设置上一个只读检查点未完成确认
                            }
                        }
                    }
                    *logger.0.only_reads.lock() = swap; //更新只读检查点的文件路径列表
                }
            }

            Ok(())
        }.boxed()
    }

    fn start_replay<B, F>(&self, mut callback: Arc<F>) -> BoxFuture<'static, Result<(usize, usize)>>
        where B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
              F: Fn(Self::Cid, B) -> Result<()> + Send + Sync + 'static {
        self.0.is_replaying.store(true, Ordering::SeqCst); //设置为正在重播
        let commit_logger = self.clone();

        async move {
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
            if let Err(e) = commit_logger.0.file.split().await {
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
                    .file.readable_to_back(invalid_only_read_path.clone())
                    .await {
                    //将无效的只读日志文件设置为备份的只读日志文件错误，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other, format!("Replay commit log failed, path: {:?}, reason: {:?}", invalid_only_read_path, e)));
                }
            }

            loader.result()
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

        async move {
            //重播时的确认提交日志，不允许直接确认，需要缓冲确认的提交唯一id，并在完成重播时统一确认
            logger.0.replay_confirm_buf.lock().push_back(commit_uid);
            Ok(())
        }.boxed()
    }

    fn finish_replay(&self) -> BoxFuture<'static, Result<()>> {
        let logger = self.clone();

        async move {
            let check_points_len = logger.0.check_points.lock().await.len();

            loop {
                if logger.0.replay_confirm_buf.lock().len() < check_points_len {
                    //还有未确认已注册到检查点表中的事务，则稍后继续
                    logger.0.rt.timeout(10).await;
                    continue;
                }

                break;
            }

            //设置为已完成重播
            logger.0.is_replaying.store(false, Ordering::SeqCst);

            //执行重播时缓冲的确认提交日志
            let replay_confirms = &mut *logger.0.replay_confirm_buf.lock();
            while let Some(commit_uid) = replay_confirms.pop_front() {
                let _ = logger.confirm(commit_uid).await?;
            }

            Ok(())
        }.boxed()
    }

    fn append_check_point(&self) -> BoxFuture<'static, Result<usize>> {
        let logger = self.clone();
        async move {
            //立即强制生成新的可写检查点，并设置上一个可写检查点的状态为未完成确认
            let _check_pointes_locked = logger.0.check_points.lock().await;
            new_check_point(&logger, false).await
        }.boxed()
    }
}

// 为提交日志文件，异步创建新的可写检查点
// 设置上一个可写检查点是否已完成确认，并将上一个可写检查点追加到只读检查点的文件路径列表
async fn new_check_point(logger: &CommitLogger,
                         is_finish_confirm: bool) -> Result<usize> {
    let log_index = logger.0.file.split().await?; //立即强制生成新的可写文件，并忽略强制生成新的可写文件是否成功

    //设置新的可写检查点
    let check_point_counter = Arc::new(AtomicU64::new(0)); //初始化可写检查点的计数器
    let check_point_path = Arc::new(logger.0.file.writable_path().unwrap()); //获取可写检查点的文件路径
    *logger.0.writable.lock() = (check_point_counter, check_point_path);

    //将上一个可写检查点的日志文件追加到只读检查点的文件路径列表，等待这个检查点的所有事务的提交确认
    let only_read_path = logger.0.file.last_readable_path();
    logger.0.only_reads.lock().push_back((only_read_path, is_finish_confirm));

    //重置新的可写日志文件的已写入字节数量
    logger.0.writed_size.store(0, Ordering::Relaxed);

    Ok(log_index)
}

// 整理提交日志记录器，在重播时不允许整理
async fn collect_commit_logger(logger: &CommitLogger, timeout: usize) {
    //等待指定时长后，开始整理提交日志记录器
    logger.0.rt.timeout(timeout).await;

    if logger.0.is_replaying.load(Ordering::Relaxed) {
        //如果提交日志记录器，当前正在重播，则忽略整理
        return;
    }

    //获取检查点表的异步锁
    let check_pointes_locked = logger.0.check_points.lock().await;

    //检查是否需要生成新的可写检查点
    if logger.0.writed_size.load(Ordering::Relaxed) >= logger.0.log_file_limit {
        //提交日志的当前可写检查点对应的可写文件，已写入字节数量已达限制
        //则立即强制生成新的可写检查点，并设置上一个可写检查点的状态为未完成确认
        new_check_point(&logger, false).await;
    }

    drop(check_pointes_locked); //立即释放检查点表的异步锁
}

// 基于日志文件的内部提交日志记录器
struct InnerCommitLogger {
    rt:                 MultiTaskRuntime<()>,                                   //异步运行时
    file:               LogFile,                                                //日志文件
    delay_timeout:      usize,                                                  //延迟刷新提交日志的时间，单位毫秒
    log_file_limit:     u64,                                                    //日志文件的可写文件的最大限制
    writed_size:        AtomicU64,                                              //已写入当前可写文件的字节数量
    writable:           SpinLock<(Arc<AtomicU64>, Arc<PathBuf>)>,               //提交日志记录器的可写检查点
    only_reads:         SpinLock<VecDeque<(PathBuf, bool)>>,                    //提交日志记录器的只读检查点的文件路径列表
    check_points:       Mutex<XHashMap<Guid, (Arc<AtomicU64>, Arc<PathBuf>)>>,  //提交日志记录器的检查点表
    is_replaying:       AtomicBool,                                             //是否正在重播
    replay_only_reads:  SpinLock<VecDeque<PathBuf>>,                            //需要重播的提交日志的只读日志文件路径列表
    replay_confirm_buf: SpinLock<VecDeque<Guid>>,                               //已确认的重播事务的提交唯一id缓冲区
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