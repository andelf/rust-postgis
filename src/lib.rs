//
// Copyright (c) ShuYu Wang <andelf@gmail.com>, Feather Workshop and Pirmin Kalberer. All rights reserved.
//

//! An extension to rust-postgres, adds support for PostGIS.
//!
//! - PostGIS type helper
//! - GCJ02 support (used offically in Mainland China)
//! - Tiny WKB (TWKB) support
//!
//! ```rust,no_run
//! use postgres::{Connection, TlsMode};
//! use postgis::ewkb;
//! use postgis::LineString;
//!
//! fn main() {
//!     // conn ....
//!     # let conn = Connection::connect("postgresql://postgres@localhost", TlsMode::None).unwrap();
//!     for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
//!         let route: ewkb::LineString = row.get("route");
//!         let last_stop = route.points().last().unwrap();
//!         let _ = conn.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop]);
//!     }
//! }
//! ```
//!
//! Handling NULL values:
//!
//! ```rust,no_run
//! let route = row.get_opt::<_, Option<ewkb::LineString>>("route");
//! match route.unwrap() {
//!     Ok(Some(geom)) => { println!("{:?}", geom) }
//!     Ok(None) => { /* Handle NULL value */ }
//!     Err(err) => { println!("Error: {}", err) }
//! }
//! ```

#[macro_use(to_sql_checked)]
extern crate postgres;
extern crate byteorder;

mod error;
mod types;
pub use types::{Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon};
pub mod ewkb;
pub mod twkb;
mod postgis;
pub mod mars;
