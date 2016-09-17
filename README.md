rust-postgis
============

[![Build Status](https://travis-ci.org/andelf/rust-postgis.svg?branch=master)](https://travis-ci.org/andelf/rust-postgis)
[![Crates.io](https://meritbadge.herokuapp.com/postgis)](https://crates.io/crates/postgis)

[Documentation](http://www.rust-ci.org/andelf/rust-postgis/doc/postgis/)

An extension to rust-postgres, adds support for PostGIS.

- PostGIS type helper
- GCJ02 support (used offically in Mainland China)

## HowTo

```rust
use postgres::{Connection, SslMode};
use postgis::EwkbLineString;

fn main() {
    // conn ....
    for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
        let route: EwkbLineString = row.get("route");
    }
}
```
