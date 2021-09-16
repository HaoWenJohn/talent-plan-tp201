use std::path::PathBuf;
use crate::err::Result;
use std::fs::{OpenOptions, File};

pub fn open_file(path: impl Into<PathBuf>, write: bool,name:&str) -> Result<File> {
    Ok(OpenOptions::new()
        .append(true)
        .read(true)
        .write(write)
        .create(true)
        .open(path.into().join(name))?)
}