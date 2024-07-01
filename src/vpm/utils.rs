use std::io::Result;

use bytes::{Buf, BufMut};
use lzzzz::lz4::{ACC_LEVEL_DEFAULT, compress, decompress, max_compressed_size};

use crate::vpm::{VirtualPageEncoding, VirtualPageEncodingType,
                 page_manager::PAGE_HEADER_SIZE};

// 最小源数据大小，单位字节
const MIN_SOURCE_SIZE: usize = 128;

// 编码头长度，单位字节
const ENCODING_HEADER_LEN: usize = 8;

///
/// LZ4块编码器构建器
///
pub struct LZ4EncoderBuilder {
    level:      i32,    //压缩等级
    threshold:  usize,  //压缩阈值
}

impl LZ4EncoderBuilder {
    /// 构建一个LZ4块编码器构建器
    pub fn new() -> Self {
        LZ4EncoderBuilder {
            level: ACC_LEVEL_DEFAULT,
            threshold: MIN_SOURCE_SIZE,
        }
    }

    /// 设置编码级别，数字越高压缩率越高，压缩和解压时间越长
    pub fn set_level(mut self, level: u8) -> Self {
        self.level = level as i32;
        self
    }

    /// 设置压缩阈值，必须高于阈值才会编码
    pub fn set_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// 构建一个LZ4编码器
    pub fn build(self) -> LZ4Encoder {
        LZ4Encoder {
            level: self.level,
            threshold: self.threshold,
        }
    }
}

///
/// LZ4编码器
///
#[derive(Debug, Clone)]
pub struct LZ4Encoder {
    level:      i32,    //压缩等级
    threshold:  usize,  //压缩阈值
}

impl VirtualPageEncoding for LZ4Encoder {
    type Raw = Vec<u8>;
    type Encoded = Vec<u8>;

    fn encoding_type(&self) -> VirtualPageEncodingType {
        VirtualPageEncodingType::LZ4(self.level as u8)
    }

    fn encode(&self, raw: Self::Raw) -> Result<Self::Encoded> {
        if raw.len() <= self.threshold {
            //无需压缩
            let mut uncompressed = vec![0; ENCODING_HEADER_LEN];
            uncompressed.extend_from_slice(raw.as_slice());
            Ok(uncompressed)
        } else {
            //需要压缩
            let max_compressed_size = max_compressed_size(raw.len());
            let capacity = if max_compressed_size == 0 {
                raw.len() + ENCODING_HEADER_LEN
            } else {
                max_compressed_size + ENCODING_HEADER_LEN
            };
            let mut compressed = Vec::with_capacity(capacity);
            compressed.put_u64_le(raw.len() as u64);
            compressed.resize(capacity, 0);
            let len = compress(raw.as_slice(),
                             &mut compressed[ENCODING_HEADER_LEN..capacity],
                             self.level)?;
            compressed.truncate(len + ENCODING_HEADER_LEN);

            Ok(compressed)
        }
    }

    fn decode(&self, encoded: Self::Encoded, page_len: usize) -> Result<Self::Raw> {
        let mut slice = &encoded[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + page_len]; //忽略页头

        match slice.get_u64_le() {
            0 => {
                //无需解压
                Ok(slice.to_vec())
            },
            raw_len => {
                //需要解压
                let mut decompressed = vec![0; raw_len as usize];
                let len = decompress(slice, decompressed.as_mut())?;
                decompressed.truncate(len);

                Ok(decompressed)
            },
        }
    }
}

impl LZ4Encoder {
    /// 获取压缩级别
    pub fn level(&self) -> u8 {
        self.level as u8
    }

    /// 获取当前压缩阈值
    pub fn threshold(&self) -> usize {
        self.threshold
    }
}

