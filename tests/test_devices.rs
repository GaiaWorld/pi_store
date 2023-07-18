use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::unbounded;

use pi_async_rt::rt::multi_thread::MultiTaskRuntimeBuilder;

use pi_store::devices::{BlockDevice, BlockLocation, simple_device::{Binary, SimpleDevice}};

#[test]
fn test_simple_device() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match SimpleDevice::open(rt_copy, "./device/0.simple", None).await {
            Err(e) => panic!("Open simple device failed, reason: {:?}", e),
            Ok(device) => {
                println!("!!!!!!device: {:?}", device.get_status());

                let location = device.alloc_block(50).await;
                println!("!!!!!!location: {:?}", location);
                match device.write(&location,
                                   &Binary::new("Hello Simple device".to_string().into_bytes())).await {
                    Err(e) => panic!("Write block failed, reason: {:?}", e),
                    Ok(len) => {
                        println!("!!!!!!write ok, len: {}", len);
                    }
                }

                match device.read(&location).await {
                    Err(e) => panic!("Read block failed, reason: {:?}", e),
                    Ok(block) => {
                        println!("read writed data: {:?}", String::from_utf8_lossy(block.as_ref()))
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 先执行一次test_simple_device，再执行
#[test]
fn test_simple_device_collect() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match SimpleDevice::open(rt_copy, "./device/0.simple", None).await {
            Err(e) => panic!("Open simple device failed, reason: {:?}", e),
            Ok(device) => {
                println!("!!!!!!device: {:?}", device.get_status());


                match device.collect_alloced_blocks(&[BlockLocation::from(1)]).await {
                    Err(e) => println!("Collect alloced failed, reason: {:?}", e),
                    Ok(size) => {
                        println!("collect alloced ok, size: {}", size);
                    }
                }

                let location = device.alloc_block(50).await;
                println!("!!!!!!location: {:?}", location);
                match device.write(&location,
                                   &Binary::new("Hello Simple device".to_string().into_bytes())).await {
                    Err(e) => panic!("Write block failed, reason: {:?}", e),
                    Ok(len) => {
                        println!("!!!!!!write ok, len: {}", len);
                    }
                }

                match device.read(&location).await {
                    Err(e) => panic!("Read block failed, reason: {:?}", e),
                    Ok(block) => {
                        println!("read writed data: {:?}", String::from_utf8_lossy(block.as_ref()))
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}