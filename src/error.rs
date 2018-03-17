//  FileName    : error.rs
//  Author      : ShuYu Wang <andelf@gmail.com>
//  Created     : Wed May 27 01:45:41 2015 by ShuYu Wang
//  Copyright   : Feather Workshop (c) 2015
//  Description : PostGIS helper
//  Time-stamp: <2015-06-13 19:21:08 andelf>

use std;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    Read(String),
    Write(String),
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Read(_) => "postgis error while reading",
            Error::Write(_) => "postgis error while writing",
            Error::Other(_) => "postgis unknown error",
        }
    }
}
