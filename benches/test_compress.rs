#![feature(test)]

extern crate test;

use std::io::Write;
use test::Bencher;

use lz4::{EncoderBuilder, Decoder};
use lzzzz::lz4::{ACC_LEVEL_DEFAULT, compress_to_vec, decompress};

#[bench]
fn test_lz4_encoding(b: &mut Bencher) {
    let mut buf = Vec::with_capacity(0xffff);
    let bytes = include_bytes!("./input.txt").as_slice();
    let bytes_len = bytes.len();

    let mut result = Vec::with_capacity(300000);
    let r = &mut result;
    b.iter(move || {
        buf.clear();
        let mut encoder = EncoderBuilder::new()
            .level(1)
            .build(&mut buf)
            .unwrap();
        if let Ok(_size) = encoder.write(bytes) {
            if let (compressed, Ok(_)) = encoder.finish() {
                r.push(compressed.len() as f64 / bytes_len as f64);
            }
        }
    });

    let count = result.len();
    println!("compress count: {:?}, compress rate: {:?}", count, result.iter().sum::<f64>() / count as f64);
}

#[bench]
fn test_lz4_decoding(b: &mut Bencher) {
    let buf = Vec::with_capacity(0xffff);
    let mut encoder = EncoderBuilder::new()
        .level(1)
        .build(buf)
        .unwrap();
    let bytes = include_bytes!("./input.txt").as_slice();

    if let Ok(_size) = encoder.write(bytes) {
        if let (compressed, Ok(_)) = encoder.finish() {
            b.iter(move || {
                if let Ok(decoder) = Decoder::new(compressed.as_slice()) {
                    if let (decompressed, Ok(_)) = decoder.finish() {
                        if bytes == decompressed {
                            return;
                        } else {
                            panic!("decompressed failed, bytes: {:?}, decompressed: {:?}", bytes, decompressed);
                        }
                    }
                }
            });
        } else {
            panic!("decompressed failed");
        }
    } else {
        panic!("compress failed");
    }
}

#[bench]
fn test_lzzzz_encoding(b: &mut Bencher) {
    let mut buf = Vec::with_capacity(0xffff);
    let bytes = include_bytes!("./input.txt").as_slice();
    let bytes_len = bytes.len();

    let mut result = Vec::with_capacity(300000);
    let r = &mut result;
    b.iter(move || {
        buf.clear();
        if let Ok(size) = compress_to_vec(bytes, &mut buf, ACC_LEVEL_DEFAULT) {
            r.push(size as f64 / bytes_len as f64);
        }
    });

    let count = result.len();
    println!("compress count: {:?}, compress rate: {:?}", count, result.iter().sum::<f64>() / count as f64);
}

#[bench]
fn test_lzzzz_decoding(b: &mut Bencher) {
    let mut buf = Vec::with_capacity(0xffff);
    let bytes = include_bytes!("./input.txt").as_slice();

    if let Ok(_size) = compress_to_vec(bytes, &mut buf, ACC_LEVEL_DEFAULT) {
        let mut decompressed = Vec::with_capacity(0xffff);
        b.iter(move || {
            decompressed.resize(bytes.len(), 0);
            match decompress(buf.as_slice(), decompressed.as_mut()) {
                Err(e) => {
                    panic!("decompressed failed, reason: {:?}", e);
                },
                Ok(len) => {
                    if bytes == &decompressed[0..len]{
                        return;
                    } else {
                        panic!("decompressed failed, bytes: {:?}, decompressed: {:?}", bytes, decompressed);
                    }
                },
            }
        });
    } else {
        panic!("compress failed");
    }
}


