#[deny(missing_docs)]

mod kvs;
mod err;

pub use crate::kvs::Database;
pub use err::{Result,Error};


