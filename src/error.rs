use std;
use std::fmt;

#[derive(Debug, )]
pub enum Error {
    Read(String),
    Write(String),
    Other(String)
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Read(_)  => "postgis error while reading",
            Error::Write(_) => "postgis error while writing",
            Error::Other(_) => "postgis unknown error"
        }
    }
}
