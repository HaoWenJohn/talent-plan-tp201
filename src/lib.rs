#[deny(missing_docs)]
mod kvs;
mod err;
pub mod utils;

pub use crate::kvs::Database as KvStore;
pub use crate::kvs::SledKvsEngine;
pub use err::{Result,Error};


pub trait KvsEngine{
    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}