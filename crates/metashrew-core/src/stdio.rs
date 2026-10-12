use std::sync::Arc;
//use std::io::{Write, Result};
use crate::imports::__log;
use metashrew_support::compat::{to_arraybuffer_layout, to_passback_ptr};
pub use std::fmt::{Error, Write};

pub struct Stdout(());

impl Write for Stdout {
    fn write_str(&mut self, s: &str) -> Result<(), Error> {
        let data = Arc::new(s.to_string().as_bytes().to_vec());
        log(data.clone());
        return Ok(());
    }
}

pub fn stdout() -> Stdout {
    Stdout(())
}

// `println!` / `print!` here route through the host `__log` import (see
// `log()` below). On the indexer hot path that's a wasmi→host call plus
// stdout I/O per line — across thousands of cellpacks per heavy block
// (e.g. mass-mint DIESEL blocks) the cumulative cost dominates per-block
// throughput. The `debug-log` feature toggles the macros: ON keeps the
// original behaviour, OFF compiles them to `()` so call sites pay
// nothing. The `use ...::stdio::{stdout, Write}` imports that nearly
// every consumer pairs with `use metashrew_core::{println, ...}` are
// already `#[allow(unused_imports)]`-annotated, so the no-op variant
// doesn't trigger dead-import warnings downstream.
#[cfg(feature = "debug-log")]
#[macro_export]
macro_rules! println {
  ( $( $x:tt )* ) => {
    {
      writeln!(stdout(), $($x)*).unwrap();
    }
  }
}

#[cfg(not(feature = "debug-log"))]
#[macro_export]
macro_rules! println {
    ( $( $x:tt )* ) => {
        ()
    };
}

#[cfg(feature = "debug-log")]
#[macro_export]
macro_rules! print {
  ( $( $x:tt )* ) => {
    {
      write!(stdout(), $($x)*).unwrap();
    }
  }
}

#[cfg(not(feature = "debug-log"))]
#[macro_export]
macro_rules! print {
    ( $( $x:tt )* ) => {
        ()
    };
}

/*
#[cfg(not(test))]
#[link(wasm_import_module = "env")]
extern "C" {
    fn __log(ptr: i32);
}
*/

#[allow(unused_unsafe)]
pub fn log(v: Arc<Vec<u8>>) -> () {
    unsafe {
        __log(to_passback_ptr(&mut to_arraybuffer_layout(v.as_ref())));
    }
}
