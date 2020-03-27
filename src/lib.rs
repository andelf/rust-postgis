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
//! use postgres::{Client, NoTls};
//! use postgis::{ewkb, LineString};
//!
//! fn main() {
//!     let mut client = Client::connect("host=localhost user=postgres", NoTls).unwrap();
//!     for row in &client.query("SELECT * FROM busline", &[]).unwrap() {
//!         let route: ewkb::LineString = row.get("route");
//!         let last_stop = route.points().last().unwrap();
//!         let _ = client.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop]);
//!     }
//! }
//! ```
//!
//! Handling NULL values:
//!
//! ```rust,no_run
//! # use postgres::{Client, NoTls};
//! # use postgis::{ewkb, LineString};
//! # let mut client = Client::connect("host=localhost user=postgres", NoTls).unwrap();
//! # let rows = client.query("SELECT * FROM busline", &[]).unwrap();
//! # let row = rows.first().unwrap();
//! let route = row.try_get::<_, Option<ewkb::LineString>>("route");
//! match route {
//!     Ok(Some(geom)) => { println!("{:?}", geom) }
//!     Ok(None) => { /* Handle NULL value */ }
//!     Err(err) => { println!("Error: {}", err) }
//! }
//! ```

pub mod error;
mod types;
pub use types::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
pub mod ewkb;
pub mod mars;
mod postgis;
pub mod twkb;
