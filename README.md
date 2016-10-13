rust-postgis
============

[![Build Status](https://travis-ci.org/andelf/rust-postgis.svg?branch=master)](https://travis-ci.org/andelf/rust-postgis)
[![Crates.io](https://meritbadge.herokuapp.com/postgis)](https://crates.io/crates/postgis)

[Documentation](http://www.rust-ci.org/andelf/rust-postgis/doc/postgis/)

An extension to rust-postgres, adds support for PostGIS.

- PostGIS type helper
- GCJ02 support (used offically in Mainland China)
- Tiny WKB (TWKB) support

## Usage

```rust
use postgres::{Connection, SslMode};
use postgis::{EwkbLineString, Points};

fn main() {
    // conn ....
    for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
        let route: EwkbLineString = row.get("route");
        let last_stop = route.points().last().unwrap();
        let _ = conn.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop]);
    }
}
```

## Writing other geometry types into PostGIS

rust-postgis supports writing geometry types into PostGIS which implement the following traits:

* `Point`, `LineString`, ...
* `AsEwkbPoint`, `AsEwkbLineString`, ...

See the TWKB implementation as an example.

An example for reading a TWKB geometry and writing it back as EWKB:

```rust
for row in &conn.query("SELECT ST_AsTWKB(route) FROM busline", &[]).unwrap() {
    let route: TwkbLineString = row.get(0);
    let last_stop = route.points().last().unwrap();
    let _ = conn.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop.as_ewkb()]);
}
```


## Unit tests

Unit tests which need a PostgreSQL connection are ignored by default.
To run the database tests, declare the connection in an environment variable `DBCONN`. Example:

    export DBCONN=postgresql://user@localhost/testdb

Run the tests with

    cargo test -- --ignored
