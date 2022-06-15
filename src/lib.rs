#![feature(map_first_last)]
#[allow(dead_code,unused_variables,non_snake_case,unused_parens,unused_assignments,unused_unsafe,unused_imports)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate core;

pub mod free_lock;
pub mod log_store;
pub mod vpm;
pub mod devices;
pub mod commit_logger;