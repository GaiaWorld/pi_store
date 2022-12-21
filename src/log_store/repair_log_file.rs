use std::collections::LinkedList;
use std::fmt::Debug;
use std::io::{Cursor, Error, ErrorKind, Result};
use std::path::{Path, PathBuf};

use bytes::Buf;
use crc32fast::Hasher;
use log::debug;

use pi_async::rt::multi_thread::MultiTaskRuntime;
use pi_async_file::file::{rename, AsyncFile, AsyncFileOptions, WriteOptions};

use crate::log_store::log_file::now_unix_epoch;

use super::log_file::{open_logs, LogMethod};

// 默认的起始时间'2020-01-01 01:00:00'
pub const DEFAULT_START_TIME: u64 = 1577808000000;

/*
* 默认的日志文件块头长度
*/
pub const DEFAULT_LOG_BLOCK_HEADER_LEN: usize = 16;

/*
* 默认的临时日志文件扩展名
*/
const DEFAULT_TMP_LOG_FILE_EXT: &str = "tmp";

/*
* 默认的整理后的只读日志文件的初始扩展名
*/
const DEFAULT_COLLECTED_LOG_FILE_INIT_EXT: &str = "0";

pub async fn repair_log<P: AsRef<Path> + Debug>(
    rt: &MultiTaskRuntime<()>,
    path: P,
    len: u64,
) -> Result<()> {
    match open_logs(&rt, &path).await {
        Ok((log_files, _last_log_name)) => {
            if log_files.len() > 0 {
                for (path, file) in log_files {
                    let file_size = file.get_size();
                    if file_size > 0 {
                        match load_file(file.clone(), path.clone(), None, len, true).await {
                            Err(e) => {
                                return Err(Error::new(
                                    ErrorKind::Other,
                                    format!(
                                        "load_file error, path: {:?}, reason: {:?}",
                                        path.to_path_buf(),
                                        e
                                    ),
                                ));
                            }
                            Ok(blocks) => {
                                match create_tmp_log(rt.clone(), path.clone()).await {
                                    Err(e) => {
                                        return Err(Error::new(
                                            ErrorKind::Other,
                                            format!(
                                                "create_tmp_log failed, path: {:?}, reason: {:?}",
                                                path.to_path_buf(),
                                                e
                                            ),
                                        ));
                                    }
                                    Ok((tmp_path, tmp_file, mut new_path)) => {
                                        let mut write_pos = 0;
                                        for (start, end) in blocks {
                                            match file.read(start, (end - start) as usize).await {
                                                Err(e) => {
                                                    return Err(Error::new(
                                                        ErrorKind::Other,
                                                        format!(
                                                            "read file failed, reason: {:?}",
                                                            e
                                                        ),
                                                    ));
                                                }
                                                Ok(bin) => {
                                                    match tmp_file
                                                        .write(write_pos, bin, WriteOptions::None)
                                                        .await
                                                    {
                                                        Err(e) => {
                                                            return Err(Error::new(ErrorKind::Other, format!("Write tmp log block failed, path: {:?}, reason: {:?}", tmp_path.to_path_buf(), e)));
                                                        }
                                                        Ok(size) => {
                                                            write_pos += size as u64;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        new_path.set_extension("repair");
                                        // 重命名临时文件
                                        if let Err(e) =
                                            rename(rt.clone(), tmp_path.clone(), new_path.clone())
                                                .await
                                        {
                                            return Err(Error::new(ErrorKind::Other, format!("Rename tmp log failed, from: {:?}, to: {:?}, reason: {:?}", tmp_path, new_path, e)));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn read_log_file_block(
    file_path: PathBuf,
    bin: &Vec<u8>,
    file_offset: u64,
    read_len: u64,
    is_checksum: bool,
    blocks: &mut Vec<(u64, u64)>,
) -> Result<(u64, u64, LinkedList<(LogMethod, Vec<u8>, Option<Vec<u8>>)>)> {
    let mut result = LinkedList::new();
    if bin.len() == 0 {
        //缓冲区长度为0，则立即退出
        return Ok((0, 0, result));
    }

    //从缓冲区中读取所有完整的日志块，默认情况下缓冲区长度至少等于日志块头的长度
    let header_len = DEFAULT_LOG_BLOCK_HEADER_LEN as u64; //日志块头的长度
    let mut bin_top = bin.len() as u64; //初始化缓冲区的剩余长度
    while bin_top >= header_len {
        let header_offset = bin_top - header_len; //获取缓冲区的当前头偏移
        match read_block_header(bin, file_offset, read_len, header_offset) {
            (Some((0, 0)), _, _, _, _) => {
                // 当前header不正确，往前偏移
                bin_top -= 1;
            }
            (Some((next_file_offset, next_read_len)), _, _, _, _) => {
                //读当前缓冲区中，当前二进制数据未包括完整的日志块负载，则立即返回需要读取的日志文件偏移和长度，以保证可以继续读日志块
                return Ok((next_file_offset, next_read_len, result));
            }
            (None, payload_offset, payload_time, payload_checksum, payload_len) => {
                // println!("payload_offset:{}", payload_offset);
                // println!("payload_time:{}", payload_time);
                // println!("payload_checksum:{}", payload_checksum);
                // println!("payload_len:{}", payload_len);

                if let Err(_e) = read_block_payload(
                    &mut result,
                    &file_path,
                    bin,
                    payload_offset,
                    payload_time,
                    payload_checksum,
                    payload_len,
                    is_checksum,
                ) {
                    // 当前header不正确，往前偏移
                    bin_top -= 1;
                    continue;
                }

                //读日志块头成功
                bin_top -= header_len; //从缓冲区的剩余长度中减去日志块头长度

                bin_top -= payload_len as u64; //读日志块负载成功，从缓冲区的剩余长度中减去日志块负载长度
                blocks.push((
                    file_offset + payload_offset,
                    file_offset
                        + payload_offset
                        + payload_len as u64
                        + DEFAULT_LOG_BLOCK_HEADER_LEN as u64,
                ));
                debug!(
                    "=======> file_offset:{}, payload_offset:{}, payload_len:{}",
                    file_offset, payload_offset, payload_len
                );
                debug!(
                    "=====>block start: {}, end: {}",
                    file_offset + payload_offset,
                    file_offset
                        + payload_offset
                        + payload_len as u64
                        + DEFAULT_LOG_BLOCK_HEADER_LEN as u64
                );
                if file_offset == 0 && bin_top == 0 {
                    //已读取当前日志文件的所有日志块，则立即退出
                    return Ok((0, 0, result));
                }
            }
        }
    }

    //返回下次需要读取的日志文件偏移和这次从所有的完整日志块中读取的日志
    let unread_len = file_offset + bin_top; //获取文件剩余未读长度和缓冲区未读长度
    let next_file_offset = unread_len.checked_sub(read_len).unwrap_or(0);
    let next_read_len = if unread_len < read_len {
        //文件剩余未读长度小于当前读取长度
        unread_len
    } else {
        //文件剩余未读长度大于等于当前读取长度
        read_len
    };
    if file_offset == 0 {
        Ok((0, 0, result))
    } else {
        Ok((next_file_offset, next_read_len, result))
    }
}

async fn load_file(
    file: AsyncFile<()>,
    file_path: PathBuf,
    mut offset: Option<u64>,
    mut len: u64,
    is_checksum: bool,
) -> Result<Vec<(u64, u64)>> {
    if len < DEFAULT_LOG_BLOCK_HEADER_LEN as u64 {
        return Err(Error::new(
            ErrorKind::Other,
            format!(
                "Load file failed, log offset: {:?}, len: {:?}, checksum: {:?}, reason: {:?}",
                offset, len, is_checksum, "Invalid len"
            ),
        ));
    }
    // 块记录
    let mut blocks: Vec<(u64, u64)> = Vec::new();
    loop {
        match read_log_file(file_path.clone(), file.clone(), offset, len).await {
            Err(e) => return Err(e),
            Ok((file_offset, bin)) => {
                if file_offset == 0 && len == 0 {
                    //已读到日志文件头，则立即返回
                    break;
                };
                match read_log_file_block(
                    file_path.clone(),
                    &bin,
                    file_offset,
                    len,
                    is_checksum,
                    &mut blocks,
                ) {
                    Err(e) => return Err(e),
                    Ok((next_file_offset, next_len, _logs)) => {
                        if next_file_offset == 0 && next_len == 0 {
                            //已读到日志文件头，则立即返回
                            break;
                        } else {
                            //更新日志文件位置
                            offset = Some(next_file_offset);
                            len = next_len;
                        }
                    }
                }
            }
        }
    }
    blocks.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(blocks)
}

pub async fn read_log_file(
    file_path: PathBuf,
    file: AsyncFile<()>,
    offset: Option<u64>,
    len: u64,
) -> Result<(u64, Vec<u8>)> {
    let file_size = file.get_size();
    if file_size == 0 {
        //当前日志文件没有日志
        return Ok((0, vec![]));
    }
    let mut real_len = len;

    if let Some(off) = offset {
        //从当前日志文件的指定位置开始读指定长度的二进制
        debug!(
            "=====>read file, real_offset: {}, real_len: {}",
            off, real_len
        );
        match file.read(off, real_len as usize).await {
            Err(e) => {
            Err(Error::new(ErrorKind::Other, format!("Read log file failed, path: {:?}, file size: {:?}, offset: {:?}, len: {:?}, reason: {:?}", file_path, file_size, off, real_len, e)))
            },
            Ok(bin) => {
            Ok((off, bin))
            },
        }
    } else {
        //从当前日志文件的尾部开始读指定长度的二进制
        if file_size < len {
            //如果读超过文件的大小，则设置为文件的大小
            real_len = file_size;
        }

        let offset = file_size - real_len;
        match file.read(offset, real_len as usize).await {
            Err(e) => {
                Err(Error::new(ErrorKind::Other, format!("Read log file failed, path: {:?}, file size: {:?}, offset: {:?}, len: {:?}, reason: {:?}", file_path, file_size, offset, real_len, e)))
            },
            Ok(bin) => {
                Ok((offset, bin))
            },
            }
    }
}

//读二进制块负载
fn read_block_payload<P: AsRef<Path>>(
    list: &mut LinkedList<(LogMethod, Vec<u8>, Option<Vec<u8>>)>,
    path: P,
    bin: &Vec<u8>,
    payload_offset: u64,
    payload_time: u64,
    payload_checksum: u32,
    payload_len: u32,
    is_checksum: bool,
) -> Result<()> {
    let bytes = &bin[payload_offset as usize..payload_offset as usize + payload_len as usize];
    let mut hasher = Hasher::new();
    let mut payload = Cursor::new(bytes).copy_to_bytes(bytes.len());
    hasher.update(payload.as_ref());
    hasher.update(&payload_time.to_le_bytes());
    //将当前块的日志写入缓冲栈中
    let mut stack = Vec::new();
    while payload.len() > 7 {
        //解析日志方法和关键字
        let tag = payload.get_u8();
        if !(tag == 0 || tag == 1) {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Read log block payload tag error tag:{:?}", tag),
            ));
        }
        let tag = LogMethod::with_tag(tag);
        let key_len = payload.get_u16_le() as usize;
        if key_len > payload.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Read log block payload key_len error"),
            ));
        }
        let mut swap = payload.split_off(key_len);
        let key = payload.to_vec();
        // println!("read_block_payload tag:{:?}, key:{:?}, ", tag, key);
        payload = swap;

        //解析值
        if let LogMethod::Remove = tag {
            //移除方法的日志，则忽略值解析，并继续解析下一个日志
            stack.push((tag, key, None));
            continue;
        }
        let value_len = payload.get_u32_le() as usize;
        if value_len > payload.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Read log block payload value_len error"),
            ));
        }
        swap = payload.split_off(value_len);
        let value = payload.to_vec();
        payload = swap;
        stack.push((tag, key, Some(value)));
    }

    //将日志从临时缓冲栈中弹出，并写入链表尾部
    while let Some(log) = stack.pop() {
        list.push_back(log);
    }

    if is_checksum {
        //需要校验块负载
        let hash = hasher.finalize();
        if payload_checksum != hash {
            //校验尾块负载失败，则立即返回错误
            return Err(Error::new(ErrorKind::Other, format!("Read log block payload failed, path: {:?}, offset: {:?}, len: {:?}, checksum: {:?}, real: {:?}, reason: Valid checksum error", path.as_ref(), payload_offset, payload_len, payload_checksum, hash)));
        }
    }
    Ok(())
}

fn read_block_header(
    bin: &Vec<u8>,
    file_offset: u64,
    read_len: u64,
    header_offset: u64,
) -> (Option<(u64, u64)>, u64, u64, u32, u32) {
    debug!(
        "=====>bin len: {}, file_offset: {}, read_len: {}, header_offset: {}",
        bin.len(),
        file_offset,
        read_len,
        header_offset
    );
    let head_bin_len = bin[0..header_offset as usize].len();

    //从缓冲区中获取块头
    let bytes = &bin[header_offset as usize..header_offset as usize + DEFAULT_LOG_BLOCK_HEADER_LEN];
    debug!("=====>bytes: {:?}", bytes);

    //从块从中获取相关负载摘要
    let mut header = Cursor::new(bytes);
    let payload_time = header.get_u64_le(); //读取块同步时间
    let payload_checksum = header.get_u32_le(); //读取块校验码
    let payload_len = header.get_u32_le(); //读取块负载长度

    // 检测时间是否合理
    if payload_time < DEFAULT_START_TIME || payload_time > now_unix_epoch() {
        // time异常，尝试下一个字节
        return (Some((0, 0)), 0, 0, 0, 0);
    }
    if head_bin_len < payload_len as usize {
        debug!(
            "=====>, head_bin_len: {}, payload_len: {}",
            head_bin_len, payload_len
        );
        //当前日志块的负载长度超过了当前剩余缓冲区的长度，则获取下次需要读取的日志块头偏移和下次需要读取的文件长度
        let block_len = payload_len as u64 + DEFAULT_LOG_BLOCK_HEADER_LEN as u64; //日志块的长度
        if block_len > read_len {
            //当前日志块的长度超过了当前的文件读长度，则更新包括了当前缓冲区中未读数据的下次需要读取的日志块头偏移和下次需要读取的精确的文件长度
            let next_file_offset = file_offset
                .checked_sub(
                    block_len - (head_bin_len as u64 + DEFAULT_LOG_BLOCK_HEADER_LEN as u64),
                )
                .unwrap_or(0);
            debug!(
                "=====>, file_header_offset: {}, next_read_len: {}",
                next_file_offset, block_len
            );
            (Some((next_file_offset, block_len)), 0, 0, 0, 0)
        } else {
            //当前日志块的长度未超过当前的文件读长度，则更新包括了当前缓冲区中未读数据的下次需要读取的日志块头偏移和下次需要读取的文件长度
            let next_read_len =
                if file_offset + head_bin_len as u64 + DEFAULT_LOG_BLOCK_HEADER_LEN as u64
                    >= read_len
                {
                    //文件剩余未读长度和缓冲区剩余未读长度之和大于等于当前文件读长度
                    read_len
                } else {
                    //文件剩余未读长度和缓冲区剩余未读长度之和小于当前文件读长度
                    file_offset + head_bin_len as u64 + DEFAULT_LOG_BLOCK_HEADER_LEN as u64
                };
            let next_file_offset = file_offset
                .checked_sub(
                    next_read_len - (head_bin_len as u64 + DEFAULT_LOG_BLOCK_HEADER_LEN as u64),
                )
                .unwrap_or(0);
            debug!(
                "=====>, file_header_offset: {}, next_read_len: {}",
                next_file_offset, next_read_len
            );
            (Some((next_file_offset, next_read_len)), 0, 0, 0, 0)
        }
    } else {
        //当前日志块的负载长度未超过当前剩余缓冲区的长度，则返回当前日志块的负载摘要
        let payload_offset = header_offset - payload_len as u64;
        debug!(
            "=====>payload_offset: {}, payload_len: {}",
            payload_offset, payload_len
        );
        (
            None,
            payload_offset,
            payload_time,
            payload_checksum,
            payload_len,
        )
    }
}

//增加指定的临时整理日志文件，返回临时整理日志文件的路径、文件和整理完成后只读日志文件的扩展名
async fn create_tmp_log<P: AsRef<Path>>(
    rt: MultiTaskRuntime<()>,
    path: P,
) -> Result<(PathBuf, AsyncFile<()>, PathBuf)> {
    let mut tmp_log = path.as_ref().to_path_buf();
    if let Some(ext_name) = tmp_log.extension() {
        if let Some(ext_name_str) = ext_name.to_str() {
            if let Ok(current) = ext_name_str.parse::<usize>() {
                //临时整理日志文件的基础只读日志文件名，有扩展名且扩展名是整数，则替换扩展名
                let ok_log = tmp_log.clone();
                tmp_log = tmp_log.with_extension(DEFAULT_TMP_LOG_FILE_EXT);
                match AsyncFile::open(rt, tmp_log.clone(), AsyncFileOptions::OnlyAppend).await {
                    Err(e) => Err(e),
                    Ok(file) => Ok((
                        tmp_log,
                        file,
                        ok_log.with_extension((current + 1).to_string()),
                    )),
                }
            } else {
                return Err(Error::new(ErrorKind::Other, format!("Create tmp log failed, last readable path: {:?}, reason: invalid last readable log extension", path.as_ref())));
            }
        } else {
            return Err(Error::new(ErrorKind::Other, format!("Create tmp log failed, last readable path: {:?}, reason: invalid last readable log", path.as_ref())));
        }
    } else {
        //临时整理日志文件的基础只读日志文件名没有扩展名，则增加扩展名
        let mut ok_log = tmp_log.clone();
        tmp_log.set_extension(DEFAULT_TMP_LOG_FILE_EXT);
        match AsyncFile::open(rt, tmp_log.clone(), AsyncFileOptions::OnlyAppend).await {
            Err(e) => Err(e),
            Ok(file) => {
                ok_log.set_extension(DEFAULT_COLLECTED_LOG_FILE_INIT_EXT.to_string());
                Ok((tmp_log, file, ok_log))
            }
        }
    }
}
