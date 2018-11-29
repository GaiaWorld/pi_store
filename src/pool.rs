use std::thread;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{ Sender, Receiver, channel };

use lmdb::{
    Environment, Database, WriteFlags, Error, Transaction, EnvironmentFlags,
    DatabaseFlags, RwTransaction, RoTransaction, RoCursor, Cursor, RwCursor,

    mdb_set_compare, MDB_txn, MDB_dbi, MDB_val, MDB_cmp_func
};

use lmdb_file::{
    MDB_SET, MDB_PREV, MDB_NEXT, MDB_FIRST, MDB_LAST
};

use pi_db::db::{
    Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult,
    NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn,
    Tab, OpenTab, Ware, WareSnapshot, TxState, Iter, CommitResult,
    RwLog, TabMeta
};

pub enum LmdbMessage {
    NewTxn(Arc<Environment>, String, bool),
    Query(Arc<Environment>, String, Arc<Vec<TabKV>>, TxQueryCallback),
    IterItems(Arc<Environment>, String, bool, Option<Bin>, Arc<Fn(IterResult)>),
    IterKeys(Arc<Environment>, String, bool, Option<Bin>, Arc<Fn(KeyIterResult)>),
    Modify(Arc<Environment>, String, Arc<Vec<TabKV>>, TxCallback),
    Commit(Arc<Environment>, String, TxCallback),
    Rollback(Arc<Environment>, String, TxCallback),
}

unsafe impl Send for LmdbMessage {}

pub struct ThreadPool {
    senders: Vec<Sender<LmdbMessage>>,
    total: usize,
    idle: usize
}

impl ThreadPool {
    pub fn with_capacity(cap: usize) -> Self {
        let mut senders = Vec::new();

        for i in 0..cap {
            let (tx, rx) = channel();
            thread::spawn(move || {
                let mut rw_txn_ptr: usize = 0;
                let mut ro_txn_ptr: usize = 0;
                loop {
                    match rx.recv() {
                        // This is the very first message should be sent before any database operation, or will be crashed.
                        Ok(LmdbMessage::NewTxn(db_env, db_name, writable)) => {
                            if writable {
                                rw_txn_ptr = unsafe {
                                    Box::into_raw(Box::new(db_env.begin_rw_txn().unwrap())) as usize
                                };
                                println!("rw_txn_ptr: {}", rw_txn_ptr);
                            } else {
                                ro_txn_ptr = unsafe {
                                    Box::into_raw(Box::new(db_env.begin_ro_txn().unwrap())) as usize
                                };
                                println!("ro_txn_ptr: {}", ro_txn_ptr);
                            }
                            println!("{:?}", thread::current().id());
                        },

                        Ok(LmdbMessage::Query(db_env, db_name, keys, cb)) => {
                            let mut values = Vec::new();
                            // let db = db_env.open_db(Some(&db_name.to_string())).unwrap();
                            let db = db_env.create_db(Some(&db_name.to_string()), DatabaseFlags::empty()).unwrap();

                            let ro_txn = unsafe {
                                Box::from_raw(ro_txn_ptr as *mut RoTransaction)
                            };

                            for kv in keys.iter() {
                                match ro_txn.get(db, b"hello") {
                                    Ok(v) => {
                                        values.push(TabKV {
                                            ware: kv.ware.clone(),
                                            tab: kv.tab.clone(),
                                            key: kv.key.clone(),
                                            index: kv.index,
                                            value: Some(Arc::new(Vec::from(v)))
                                        })
                                    },
                                    Err(e) => {
                                        println!("query failed {:?}", e);
                                        cb(Err(e.to_string()));
                                    }
                                }
                            }
                            println!("query value: {:?}", values);
                            cb(Ok(values));
                        },

                        Ok(LmdbMessage::IterItems(db_env, db_name, descending, key, cb)) => {
                            let db = db_env.open_db(Some(&db_name.to_string())).unwrap();
                            let ro_txn = unsafe {
                                Box::from_raw(ro_txn_ptr as *mut RoTransaction)
                            };
                            let cursor = ro_txn.open_ro_cursor(db).unwrap();

                            if let Some(k) = key.clone() {
                                cursor.get(Some(k.as_ref()), None, MDB_SET);
                            } else {
                                if descending {
                                    cursor.get(None, None, MDB_FIRST);
                                } else {
                                    cursor.get(None, None, MDB_LAST);
                                }
                            }

                            println!("iter items");
                        },

                        Ok(LmdbMessage::IterKeys(db_env, db_name, descending, key, cb)) => {
                            println!("iter keys");
                        },

                        Ok(LmdbMessage::Modify(db_env, db_name, keys, cb)) => {
                            // let db = db_env.create_db(Some(&db_name.to_string()), DatabaseFlags::empty()).unwrap();
                            // let mut txn = db_env.begin_rw_txn().unwrap();
                            // txn.put(db, b"key1", b"val1", WriteFlags::empty()).is_err();
                            // println!("value: {:?}", txn.get(db, b"key1"));
                            let db = db_env.create_db(Some(&db_name.to_string()), DatabaseFlags::empty()).unwrap();

                            let mut rw_txn = unsafe {
                                Box::from_raw(rw_txn_ptr as *mut RwTransaction)
                            };

                            for kv in keys.iter() {
                                if let Some(_) = kv.value {
                                    println!("insert");
                                    // match rw_txn.put(db, kv.key.as_ref(), kv.clone().value.unwrap().as_ref(), WriteFlags::empty()) {
                                    match rw_txn.put(db, b"key1", b"val1", WriteFlags::empty()) {
                                        Ok(_) => {
                                            println!("insert {:?} success", kv.clone().key.as_ref());
                                        }
                                        Err(e) => {
                                            println!("modify error {:?}", e);
                                            return cb(Err("insert failed".to_string()))
                                        }
                                    };
                                } else {
                                    println!("del");
                                    match rw_txn.del(db, kv.key.as_ref(), None) {
                                        Ok(_) => {
                                            println!("delete {:?} success", kv.clone().key.as_ref());
                                        }
                                        Err(e) => return cb(Err("delete failed".to_string()))
                                    };
                                }
                            }
                            println!("in modify");
                        },

                        // only commit rw txn
                        Ok(LmdbMessage::Commit(db_env, db_name, cb)) => {
                            let mut rw_txn = unsafe {
                                Box::from_raw(rw_txn_ptr as *mut RwTransaction)
                            };
                            match rw_txn.commit() {
                                Ok(_) => cb(Ok(())),
                                Err(e) => cb(Err(e.to_string()))
                            }
                            println!("receive commit");
                        },

                        // only abort tw txn
                        Ok(LmdbMessage::Rollback(db_env, db_name, cb)) => {
                            let mut rw_txn = unsafe {
                                Box::from_raw(rw_txn_ptr as *mut RwTransaction)
                            };
                            rw_txn.abort();
                            println!("receive rollback");
                        },

                        Err(e) => {
                            // unexpected message, do nothing
                        },
                    }
                }
                println!("create thread {}", i);
            });
            senders.push(tx);
        }

        ThreadPool {
            senders,
            total: cap,
            idle: cap
        }
    }

    pub fn pop(&mut self) -> Option<Sender<LmdbMessage>> {
        self.idle = self.idle - 1;
        self.senders.pop()
    }

    pub fn push(&mut self, sender: Sender<LmdbMessage>) {
        self.idle = self.idle + 1;
        self.senders.push(sender);
    }

    pub fn total_threads(&self) -> usize {
        self.total
    }

    pub fn idle_threads(&self) -> usize {
        self.idle
    }
}