//
// Copyright (c) Pirmin Kalberer. All rights reserved.
//

//! Read geometries in [Tiny WKB](https://github.com/TWKB/Specification/blob/master/twkb.md) format.
//!
//! ```rust,no_run
//! use postgis::twkb;
//! use postgis::LineString;
//!
//! for row in &conn.query("SELECT ST_AsTWKB(route) FROM busline", &[]).unwrap() {
//!     let route: twkb::LineString = row.get(0);
//!     let last_stop = route.points().last().unwrap();
//!     let _ = conn.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop.as_ewkb()]);
//! }
//! ```

use types as postgis;
use ewkb;
use std::io::prelude::*;
use std::mem;
use std::fmt;
use std::u8;
use std::f64;
use std::slice::Iter;
use byteorder::ReadBytesExt;
use error::Error;


#[derive(PartialEq, Clone, Debug)]
pub struct Point {
    pub x: f64,
    pub y: f64
    // TODO: support for z, m
}

#[derive(PartialEq, Clone, Debug)]
pub struct LineString {
    pub points: Vec<Point>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct Polygon {
    pub rings: Vec<LineString>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct MultiPoint {
    pub points: Vec<Point>,
    pub ids: Option<Vec<u64>>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct MultiLineString {
    pub lines: Vec<LineString>,
    pub ids: Option<Vec<u64>>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct MultiPolygon {
    pub polygons: Vec<Polygon>,
    pub ids: Option<Vec<u64>>,
}

#[doc(hidden)]
#[derive(Default,Debug)]
pub struct TwkbInfo {
    geom_type: u8,
    precision: i8,
    has_idlist: bool,
    is_empty_geom: bool,
    size: Option<u64>,
    has_z: bool,
    has_m: bool,
    prec_z: Option<u8>,
    prec_m: Option<u8>,
}

pub trait TwkbGeom: fmt::Debug + Sized {
    fn read_twkb<R: Read>(raw: &mut R) -> Result<Self, Error> {
        let mut twkb_info: TwkbInfo = Default::default();
        // type_and_prec     byte
        // metadata_header   byte
        // [extended_dims]   byte
        // [size]            uvarint
        // [bounds]          bbox
        let type_and_prec = raw.read_u8()?;
        twkb_info.geom_type = type_and_prec & 0x0F;
        twkb_info.precision = decode_zig_zag_64(((type_and_prec & 0xF0) >> 4) as u64) as i8;
        let metadata_header = raw.read_u8()?;
        let has_bbox = (metadata_header & 0b0001) != 0;
        let has_size_attribute = (metadata_header & 0b0010) != 0;
        twkb_info.has_idlist = (metadata_header & 0b0100) != 0;
        let has_ext_prec_info = (metadata_header & 0b1000) != 0;
        twkb_info.is_empty_geom = (metadata_header & 0b10000) != 0;
        if has_ext_prec_info {
            let ext_prec_info = raw.read_u8()?;
            twkb_info.has_z = ext_prec_info & 0b0001 != 0;
            twkb_info.has_m = ext_prec_info & 0b0010 != 0;
            twkb_info.prec_z = Some((ext_prec_info & 0x1C) >> 2);
            twkb_info.prec_m = Some((ext_prec_info & 0xE0) >> 5);
        }
        if has_size_attribute {
            twkb_info.size = Some(read_raw_varint64(raw)?);
        }
        if has_bbox {
            let _xmin = read_int64(raw)?;
            let _deltax = read_int64(raw)?;
            let _ymin = read_int64(raw)?;
            let _deltay = read_int64(raw)?;
            if twkb_info.has_z {
                let _zmin = read_int64(raw)?;
                let _deltaz = read_int64(raw)?;
            }
            if twkb_info.has_m {
                let _mmin = read_int64(raw)?;
                let _deltam = read_int64(raw)?;
            }
        }
        Self::read_twkb_body(raw, &twkb_info)
    }

    #[doc(hidden)]
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error>;

    #[doc(hidden)]
    fn read_relative_point<R: Read>(raw: &mut R, twkb_info: &TwkbInfo, x: f64, y: f64, z: Option<f64>, m: Option<f64>)
        -> Result<(f64, f64, Option<f64>, Option<f64>), Error>
    {
        let x2 = x + read_varint64_as_f64(raw, twkb_info.precision)?;
        let y2 = y + read_varint64_as_f64(raw, twkb_info.precision)?;
        let z2 = if twkb_info.has_z {
            let dz = read_varint64_as_f64(raw, twkb_info.precision)?;
            z.map(|v| v + dz)
        } else {
            None
        };
        let m2 = if twkb_info.has_m {
            let dm = read_varint64_as_f64(raw, twkb_info.precision)?;
            m.map(|v| v + dm)
        } else {
            None
        };
        Ok((x2, y2, z2, m2))
    }

    fn read_idlist<R: Read>(raw: &mut R, size: usize) -> Result<Vec<u64>, Error>
    {
        let mut idlist = Vec::new();
        idlist.reserve(size);
        for _ in 0..size {
            let id = read_raw_varint64(raw)?;
            idlist.push(id);
        }
        Ok(idlist)
    }
}

// --- helper functions for reading ---

fn read_raw_varint64<R: Read>(raw: &mut R) -> Result<u64, Error> {
    // from rust-protobuf
    let mut r: u64 = 0;
    let mut i = 0;
    loop {
        if i == 10 {
            return Err(Error::Read("invalid varint".into()));
        }
        let b = raw.read_u8()?;
        // TODO: may overflow if i == 9
        r = r | (((b & 0x7f) as u64) << (i * 7));
        i += 1;
        if b < 0x80 {
            return Ok(r);
        }
    }
}

fn read_int64<R: Read>(raw: &mut R) -> Result<i64, Error> {
    read_raw_varint64(raw).map(|v| v as i64)
}

fn decode_zig_zag_64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

fn varint64_to_f64(varint: u64, precision: i8) -> f64 {
    if precision >= 0 {
        (decode_zig_zag_64(varint) as f64) / 10u64.pow(precision as u32) as f64
    } else {
        (decode_zig_zag_64(varint) as f64) * 10u64.pow(precision.abs() as u32) as f64
    }
}

fn read_varint64_as_f64<R: Read>(raw: &mut R, precision: i8) -> Result<f64, Error> {
    read_raw_varint64(raw).map(|v| varint64_to_f64(v, precision))
}

// ---

impl Point {
    fn new_from_opt_vals(x: f64, y: f64, _z: Option<f64>, _m: Option<f64>) -> Self {
        Point { x: x, y: y }
    }
}

impl postgis::Point for Point {
    fn x(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self) }
    }
    fn y(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self).offset(1) }
    }
}

impl TwkbGeom for Point {
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        if twkb_info.is_empty_geom {
            return Ok(Point::new_from_opt_vals(f64::NAN, f64::NAN, None, None));
        }
        let x = read_varint64_as_f64(raw, twkb_info.precision)?;
        let y = read_varint64_as_f64(raw, twkb_info.precision)?;
        let z = if twkb_info.has_z {
            Some(read_varint64_as_f64(raw, twkb_info.precision)?)
        } else {
            None
        };
        let m = if twkb_info.has_m {
            Some(read_varint64_as_f64(raw, twkb_info.precision)?)
        } else {
            None
        };
        Ok(Self::new_from_opt_vals(x, y, z, m))
    }    
}

impl<'a> ewkb::AsEwkbPoint<'a> for Point {
    fn as_ewkb(&'a self) -> ewkb::EwkbPoint<'a> {
        ewkb::EwkbPoint { geom: self, srid: None, point_type: ewkb::PointType::Point }
    }
}


impl TwkbGeom for LineString {
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        // npoints           uvarint
        // pointarray        varint[]
        let mut points: Vec<Point> = Vec::new();
        if !twkb_info.is_empty_geom {
            let npoints = read_raw_varint64(raw)?;
            points.reserve(npoints as usize);
            let mut x = 0.0;
            let mut y = 0.0;
            let mut z = if twkb_info.has_z { Some(0.0) } else { None };
            let mut m = if twkb_info.has_m { Some(0.0) } else { None };
            for _ in 0..npoints {
                let (x2, y2, z2, m2) = Self::read_relative_point(raw, twkb_info, x, y, z, m)?;
                points.push(Point::new_from_opt_vals(x2, y2, z2, m2));
                x = x2; y = y2; z = z2; m = m2;
            }
        }
        Ok(LineString {points: points})
    }
}

impl<'a> postgis::LineString<'a> for LineString {
    type ItemType = Point;
    type Iter = Iter<'a, Self::ItemType>;
    fn points(&'a self) -> Self::Iter {
        self.points.iter()
    }
}

impl<'a> ewkb::AsEwkbLineString<'a> for LineString {
    type PointType = Point;
    type Iter = Iter<'a, Point>;
    fn as_ewkb(&'a self) -> ewkb::EwkbLineString<'a, Self::PointType, Self::Iter> {
        ewkb::EwkbLineString { geom: self, srid: None, point_type: ewkb::PointType::Point }
    }
}


impl TwkbGeom for Polygon {
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        // nrings            uvarint
        // npoints[0]        uvarint
        // pointarray[0]     varint[]
        // ...
        // npoints[n]        uvarint
        // pointarray[n]     varint[]
        let mut rings: Vec<LineString> = Vec::new();
        let nrings = read_raw_varint64(raw)?;
        rings.reserve(nrings as usize);
        let mut x = 0.0;
        let mut y = 0.0;
        let mut z = if twkb_info.has_z { Some(0.0) } else { None };
        let mut m = if twkb_info.has_m { Some(0.0) } else { None };
        for _ in 0..nrings {
            let mut points: Vec<Point> = Vec::new();
            let npoints = read_raw_varint64(raw)?;
            points.reserve(npoints as usize);
            let (x0, y0, z0, m0) = (x, y, z, m);
            for _ in 0..npoints {
                let (x2, y2, z2, m2) = Self::read_relative_point(raw, twkb_info, x, y, z, m)?;
                points.push(Point::new_from_opt_vals(x2, y2, z2, m2));
                x = x2; y = y2; z = z2; m = m2;
            }
            // close ring, if necessary
            if x != x0 && y != y0 && z != z0 && m != m0 {
                points.push(Point::new_from_opt_vals(x0, y0, z0, m0));
            }
            rings.push(LineString {points: points});
        }
        Ok(Polygon {rings: rings})
    }
}

impl<'a> postgis::Polygon<'a> for Polygon {
    type ItemType = LineString;
    type Iter = Iter<'a, Self::ItemType>;
    fn rings(&'a self) -> Self::Iter {
        self.rings.iter()
    }
}

impl<'a> ewkb::AsEwkbPolygon<'a> for Polygon {
    type PointType = Point;
    type PointIter = Iter<'a, Point>;
    type ItemType = LineString;
    type Iter = Iter<'a, Self::ItemType>;
    fn as_ewkb(&'a self) -> ewkb::EwkbPolygon<'a, Self::PointType, Self::PointIter, Self::ItemType, Self::Iter> {
        ewkb::EwkbPolygon { geom: self, srid: None, point_type: ewkb::PointType::Point }
    }
}


impl TwkbGeom for MultiPoint {
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        // npoints           uvarint
        // [idlist]          varint[]
        // pointarray        varint[]
        let mut points: Vec<Point> = Vec::new();
        let mut ids: Option<Vec<u64>> = None;
        if !twkb_info.is_empty_geom {
            let npoints = read_raw_varint64(raw)?;
            points.reserve(npoints as usize);

            if twkb_info.has_idlist {
                let idlist = Self::read_idlist(raw, npoints as usize)?;
                ids = Some(idlist);
            }

            let mut x = 0.0;
            let mut y = 0.0;
            let mut z = if twkb_info.has_z { Some(0.0) } else { None };
            let mut m = if twkb_info.has_m { Some(0.0) } else { None };
            for _ in 0..npoints {
                let (x2, y2, z2, m2) = Self::read_relative_point(raw, twkb_info, x, y, z, m)?;
                points.push(Point::new_from_opt_vals(x2, y2, z2, m2));
                x = x2; y = y2; z = z2; m = m2;
            }
        }
        Ok(MultiPoint {points: points, ids: ids})
    }
}

impl<'a> postgis::MultiPoint<'a> for MultiPoint {
    type ItemType = Point;
    type Iter = Iter<'a, Self::ItemType>;
    fn points(&'a self) -> Self::Iter {
        self.points.iter()
    }
}

impl<'a> ewkb::AsEwkbMultiPoint<'a> for MultiPoint {
    type PointType = Point;
    type Iter = Iter<'a, Point>;
    fn as_ewkb(&'a self) -> ewkb::EwkbMultiPoint<'a, Self::PointType, Self::Iter> {
        ewkb::EwkbMultiPoint { geom: self, srid: None, point_type: ewkb::PointType::Point }
    }
}


impl TwkbGeom for MultiLineString {
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        // nlinestrings      uvarint
        // [idlist]          varint[]
        // npoints[0]        uvarint
        // pointarray[0]     varint[]
        // ...
        // npoints[n]        uvarint
        // pointarray[n]     varint[]
        let mut lines: Vec<LineString> = Vec::new();
        let mut ids: Option<Vec<u64>> = None;
        let nlines = read_raw_varint64(raw)?;
        lines.reserve(nlines as usize);

        if twkb_info.has_idlist {
            let idlist = Self::read_idlist(raw, nlines as usize)?;
            ids = Some(idlist);
        }

        let mut x = 0.0;
        let mut y = 0.0;
        let mut z = if twkb_info.has_z { Some(0.0) } else { None };
        let mut m = if twkb_info.has_m { Some(0.0) } else { None };
        for _ in 0..nlines {
            let mut points: Vec<Point> = Vec::new();
            let npoints = read_raw_varint64(raw)?;
            points.reserve(npoints as usize);
            for _ in 0..npoints {
                let (x2, y2, z2, m2) = Self::read_relative_point(raw, twkb_info, x, y, z, m)?;
                points.push(Point::new_from_opt_vals(x2, y2, z2, m2));
                x = x2; y = y2; z = z2; m = m2;
            }
            lines.push(LineString {points: points});
        }
        Ok(MultiLineString {lines: lines, ids: ids})
    }
}

impl<'a> postgis::MultiLineString<'a> for MultiLineString {
    type ItemType = LineString;
    type Iter = Iter<'a, Self::ItemType>;
    fn lines(&'a self) -> Self::Iter {
        self.lines.iter()
    }
}

impl<'a> ewkb::AsEwkbMultiLineString<'a> for MultiLineString {
    type PointType = Point;
    type PointIter = Iter<'a, Point>;
    type ItemType = LineString;
    type Iter = Iter<'a, Self::ItemType>;
    fn as_ewkb(&'a self) -> ewkb::EwkbMultiLineString<'a, Self::PointType, Self::PointIter, Self::ItemType, Self::Iter> {
        ewkb::EwkbMultiLineString { geom: self, srid: None, point_type: ewkb::PointType::Point }
    }
}


impl TwkbGeom for MultiPolygon {
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        // npolygons         uvarint
        // [idlist]          varint[]
        // nrings[0]         uvarint
        // npoints[0][0]     uvarint
        // pointarray[0][0]  varint[]
        // ...
        // nrings[n]         uvarint
        // npoints[n][m]     uvarint
        // pointarray[n][m]  varint[]
        let mut polygons: Vec<Polygon> = Vec::new();
        let mut ids: Option<Vec<u64>> = None;
        let npolygons = read_raw_varint64(raw)?;
        polygons.reserve(npolygons as usize);

        if twkb_info.has_idlist {
            let idlist = Self::read_idlist(raw, npolygons as usize)?;
            ids = Some(idlist);
        }

        let mut x = 0.0;
        let mut y = 0.0;
        let mut z = if twkb_info.has_z { Some(0.0) } else { None };
        let mut m = if twkb_info.has_m { Some(0.0) } else { None };
        for _ in 0..npolygons {
            let mut rings: Vec<LineString> = Vec::new();
            let nrings = read_raw_varint64(raw)?;
            rings.reserve(nrings as usize);
            for _ in 0..nrings {
                let mut points: Vec<Point> = Vec::new();
                let npoints = read_raw_varint64(raw)?;
                points.reserve(npoints as usize);
                let (x0, y0, z0, m0) = (x, y, z, m);
                for _ in 0..npoints {
                    let (x2, y2, z2, m2) = Self::read_relative_point(raw, twkb_info, x, y, z, m)?;
                    points.push(Point::new_from_opt_vals(x2, y2, z2, m2));
                    x = x2; y = y2; z = z2; m = m2;
                }
                // close ring, if necessary
                if x != x0 && y != y0 && z != z0 && m != m0 {
                    points.push(Point::new_from_opt_vals(x0, y0, z0, m0));
                }
                rings.push(LineString {points: points});
            }
            polygons.push(Polygon {rings: rings});
        }
        Ok(MultiPolygon {polygons: polygons, ids: ids})
    }
}

impl<'a> postgis::MultiPolygon<'a> for MultiPolygon {
    type ItemType = Polygon;
    type Iter = Iter<'a, Self::ItemType>;
    fn polygons(&'a self) -> Self::Iter {
        self.polygons.iter()
    }
}

impl<'a> ewkb::AsEwkbMultiPolygon<'a> for MultiPolygon {
    type PointType = Point;
    type PointIter = Iter<'a, Point>;
    type LineType = LineString;
    type LineIter = Iter<'a, Self::LineType>;
    type ItemType = Polygon;
    type Iter = Iter<'a, Self::ItemType>;
    fn as_ewkb(&'a self) -> ewkb::EwkbMultiPolygon<'a, Self::PointType, Self::PointIter, Self::LineType, Self::LineIter, Self::ItemType, Self::Iter> {
        ewkb::EwkbMultiPolygon { geom: self, srid: None, point_type: ewkb::PointType::Point }
    }
}


#[cfg(test)]
use ewkb::{EwkbWrite, AsEwkbPoint, AsEwkbLineString, AsEwkbPolygon, AsEwkbMultiPoint, AsEwkbMultiLineString, AsEwkbMultiPolygon};

#[cfg(test)]
fn hex_to_vec(hexstr: &str) -> Vec<u8> {
    hexstr.as_bytes().chunks(2).map(|chars| {
        let hb = if chars[0] <= 57 { chars[0] - 48 } else { chars[0] - 87 };
        let lb = if chars[1] <= 57 { chars[1] - 48 } else { chars[1] - 87 };
        hb * 16 + lb
    }).collect::<Vec<_>>()
}

#[test]
fn test_read_point() {
    let twkb = hex_to_vec("01001427"); // SELECT encode(ST_AsTWKB('POINT(10 -20)'::geometry), 'hex')
    let point = Point::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "Point { x: 10, y: -20 }");

    let twkb = hex_to_vec("0108011427c601"); // SELECT encode(ST_AsTWKB('POINT(10 -20 99)'::geometry), 'hex')
    let point = Point::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "Point { x: 10, y: -20 }");

    let twkb = hex_to_vec("2100ca019503"); // SELECT encode(ST_AsTWKB('POINT(10.12 -20.34)'::geometry, 1), 'hex')
    let point = Point::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "Point { x: 10.1, y: -20.3 }");

    let twkb = hex_to_vec("11000203"); // SELECT encode(ST_AsTWKB('POINT(11.12 -22.34)'::geometry, -1), 'hex')
    let point = Point::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "Point { x: 10, y: -20 }");

    let twkb = hex_to_vec("0110"); // SELECT encode(ST_AsTWKB('POINT EMPTY'::geometry), 'hex')
    let point = Point::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "Point { x: NaN, y: NaN }");

    let twkb = hex_to_vec("a10080897aff91f401"); // SELECT encode(ST_AsTWKB('SRID=4326;POINT(10 -20)'::geometry), 'hex')
    let point = Point::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "Point { x: 10, y: -20 }");
}

#[test]
fn test_read_line() {
    let twkb = hex_to_vec("02000214271326"); // SELECT encode(ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry), 'hex')
    let line = LineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", line), "LineString { points: [Point { x: 10, y: -20 }, Point { x: 0, y: -1 }] }");

    let twkb = hex_to_vec("220002c8018f03c7018603"); // SELECT encode(ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry, 1), 'hex')
    let line = LineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", line), "LineString { points: [Point { x: 10, y: -20 }, Point { x: 0, y: -0.5 }] }");

    let twkb = hex_to_vec("0210"); // SELECT encode(ST_AsTWKB('LINESTRING EMPTY'::geometry), 'hex')
    let line = LineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", line), "LineString { points: [] }");
}

#[test]
fn test_read_polygon() {
    let twkb = hex_to_vec("03000205000004000004030000030514141700001718000018"); // SELECT encode(ST_AsTWKB('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0),(10 10, -2 10, -2 -2, 10 -2, 10 10))'::geometry), 'hex')
    let poly = Polygon::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", poly), "Polygon { rings: [LineString { points: [Point { x: 0, y: 0 }, Point { x: 2, y: 0 }, Point { x: 2, y: 2 }, Point { x: 0, y: 2 }, Point { x: 0, y: 0 }] }, LineString { points: [Point { x: 10, y: 10 }, Point { x: -2, y: 10 }, Point { x: -2, y: -2 }, Point { x: 10, y: -2 }, Point { x: 10, y: 10 }] }] }");
}

#[test]
fn test_read_multipoint() {
    let twkb = hex_to_vec("04000214271326"); // SELECT encode(ST_AsTWKB('MULTIPOINT ((10 -20), (0 -0.5))'::geometry), 'hex')
    let points = MultiPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", points), "MultiPoint { points: [Point { x: 10, y: -20 }, Point { x: 0, y: -1 }], ids: None }");
}

#[test]
fn test_read_multiline() {
    let twkb = hex_to_vec("05000202142713260200020400"); // SELECT encode(ST_AsTWKB('MULTILINESTRING ((10 -20, 0 -0.5), (0 0, 2 0))'::geometry), 'hex')
    let lines = MultiLineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", lines), "MultiLineString { lines: [LineString { points: [Point { x: 10, y: -20 }, Point { x: 0, y: -1 }] }, LineString { points: [Point { x: 0, y: 0 }, Point { x: 2, y: 0 }] }], ids: None }");
}

#[test]
fn test_read_multipolygon() {
    let twkb = hex_to_vec("060002010500000400000403000003010514141700001718000018"); // SELECT encode(ST_AsTWKB('MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((10 10, -2 10, -2 -2, 10 -2, 10 10)))'::geometry), 'hex')
    let polys = MultiPolygon::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", polys), "MultiPolygon { polygons: [Polygon { rings: [LineString { points: [Point { x: 0, y: 0 }, Point { x: 2, y: 0 }, Point { x: 2, y: 2 }, Point { x: 0, y: 2 }, Point { x: 0, y: 0 }] }] }, Polygon { rings: [LineString { points: [Point { x: 10, y: 10 }, Point { x: -2, y: 10 }, Point { x: -2, y: -2 }, Point { x: 10, y: -2 }, Point { x: 10, y: 10 }] }] }], ids: None }");
}

#[test]
fn test_write_point() {
    let twkb = hex_to_vec("01001427"); // SELECT encode(ST_AsTWKB('POINT(10 -20)'::geometry), 'hex')
    let point = Point::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point.as_ewkb()), "EwkbPoint");
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000000000000000000244000000000000034C0");
}

#[test]
fn test_write_line() {
    let twkb = hex_to_vec("220002c8018f03c7018603"); // SELECT encode(ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry, 1), 'hex')
    let line = LineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", line.as_ewkb()), "EwkbLineString");
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
}

#[test]
fn test_write_polygon() {
    let twkb = hex_to_vec("03000205000004000004030000030514141700001718000018"); // SELECT encode(ST_AsTWKB('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0),(10 10, -2 10, -2 -2, 10 -2, 10 10))'::geometry), 'hex')
    let polygon = Polygon::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", polygon.as_ewkb()), "EwkbPolygon");
    assert_eq!(polygon.as_ewkb().to_hex_ewkb(), "010300000002000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000050000000000000000002440000000000000244000000000000000C0000000000000244000000000000000C000000000000000C0000000000000244000000000000000C000000000000024400000000000002440");
}

#[test]
fn test_write_multipoint() {
    let twkb = hex_to_vec("04000214271326"); // SELECT encode(ST_AsTWKB('MULTIPOINT ((10 -20), (0 -0.5))'::geometry), 'hex')
    let multipoint = MultiPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", multipoint.as_ewkb()), "EwkbMultiPoint");
    //assert_eq!(multipoint.as_ewkb().to_hex_ewkb(), "0104000000020000000101000000000000000000244000000000000034C001010000000000000000000000000000000000E0BF");
    // "MULTIPOINT(10 -20,0 -1)"
    assert_eq!(multipoint.as_ewkb().to_hex_ewkb(), "0104000000020000000101000000000000000000244000000000000034C001010000000000000000000000000000000000F0BF");
}

#[test]
fn test_write_multiline() {
    let twkb = hex_to_vec("05000202142713260200020400"); // SELECT encode(ST_AsTWKB('MULTILINESTRING ((10 -20, 0 -0.5), (0 0, 2 0))'::geometry), 'hex')
    let multiline = MultiLineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", multiline.as_ewkb()), "EwkbMultiLineString");
    //assert_eq!(multiline.as_ewkb().to_hex_ewkb(), "010500000002000000010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF0102000000020000000000000000000000000000000000000000000000000000400000000000000000");
    // "MULTILINESTRING((10 -20,0 -1),(0 0,2 0))"
    assert_eq!(multiline.as_ewkb().to_hex_ewkb(), "010500000002000000010200000002000000000000000000244000000000000034C00000000000000000000000000000F0BF0102000000020000000000000000000000000000000000000000000000000000400000000000000000");
}

#[test]
fn test_write_multipoly() {
    let twkb = hex_to_vec("060002010500000400000403000003010514141700001718000018"); // SELECT encode(ST_AsTWKB('MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((10 10, -2 10, -2 -2, 10 -2, 10 10)))'::geometry), 'hex')
    let multipoly = MultiPolygon::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", multipoly.as_ewkb()), "EwkbMultiPolygon");
    assert_eq!(multipoly.as_ewkb().to_hex_ewkb(), "010600000002000000010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000010300000001000000050000000000000000002440000000000000244000000000000000C0000000000000244000000000000000C000000000000000C0000000000000244000000000000000C000000000000024400000000000002440");
}
