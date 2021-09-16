use failure::Fail;
use std::string::FromUtf8Error;

#[derive(Debug,Fail)]
pub enum Error{
    #[fail(display="open data base log dir err")]
    OpenLogDirError,
    #[fail(display="serializing data to log file meets err")]
    SerializingError,
    #[fail(display="the specified path is not dir")]
    InvalidDirectoryPath,
    #[fail(display="internal err")]
    InternalError,
    #[fail(display="key not found")]
    KeyNotFoundError
}
impl From<std::io::Error> for Error{
    fn from(_: std::io::Error) -> Self {
        Error::OpenLogDirError
    }
}
impl From<serde_json::Error> for Error{
    fn from(_: serde_json::Error) -> Self {
        Error::SerializingError
    }
}
impl From<FromUtf8Error> for Error{
    fn from(_: FromUtf8Error) -> Self {
        Error::InternalError
    }
}
pub type Result<T> = std::result::Result<T,Error>;
