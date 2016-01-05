rust-postgis
============

[![Build Status](https://travis-ci.org/andelf/rust-postgis.svg?branch=master)](https://travis-ci.org/andelf/rust-postgis)
[![Crates.io](https://meritbadge.herokuapp.com/postgis)](https://crates.io/crates/postgis)

[Documentation](http://www.rust-ci.org/andelf/rust-postgis/doc/postgis/)

An extension to rust-postgres, adds support for PostGIS.

- PostGIS type helper
- GCJ02 support (used offically in Mainland China)
- Type-safe SRID support

## HowTo

```rust
use postgres::{Connection, SslMode};
use postgis::{Point, LineString, WGS84};

fn main() {
    // conn ....
    let stmt = conn.prepare("SELECT * FROM busline").unwrap();
    for row in stmt.query(&[]).unwrap() {
        println!(">>>>>> {}", row.get::<_, LineString<Point>>("route"));
    }
}
```
