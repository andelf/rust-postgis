use types::{Point, Points, LineString};
use std::io::prelude::*;
use std::mem;
use std::fmt;
use std::u8;
use std::f64;
use byteorder::ReadBytesExt;
use error::Error;

#[derive(Debug)]
pub struct TwkbPoint {
    x: f64,
    y: f64
}

/*
impl geo::ToGeo<f64> for TwkbPoint {
    fn to_geo(&self) -> geo::Geometry<f64> {
        geo::Geometry::Point(self.geom)
    }
}
*/

#[derive(Debug)]
pub struct TwkbLineString {
    pub points: Vec<TwkbPoint>,
}

/*
impl geo::ToGeo<f64> for TwkbLineString {
    fn to_geo(&self) -> geo::Geometry<f64> {
        geo::Geometry::LineString(self.geom.clone())
    }
}
*/

#[derive(Default,Debug)]
pub struct TwkbInfo {
    geom_type: u8,
    precision: i8,
    has_id_list: bool,
    is_empty_geom: bool,
    size: Option<u64>,
    has_z: bool,
    has_m: bool,
    prec_z: Option<u8>,
    prec_m: Option<u8>,
}

pub trait TwkbGeom: fmt::Debug + Sized {
    type PointType: Point;

    fn read_twkb<R: Read>(raw: &mut R) -> Result<Self, Error> {
        // https://github.com/TWKB/Specification/blob/master/twkb.md
        let mut twkb_info: TwkbInfo = Default::default();
        // type_and_prec     byte
        // metadata_header   byte
        // [extended_dims]   byte
        // [size]            uvarint
        // [bounds]          bbox
        let type_and_prec = try!(raw.read_u8());
        twkb_info.geom_type = type_and_prec & 0x0F;
        twkb_info.precision = decode_zig_zag_64(((type_and_prec & 0xF0) >> 4) as u64) as i8;
        let metadata_header = try!(raw.read_u8());
        let has_bbox = (metadata_header & 0b0001) != 0;
        let has_size_attribute = (metadata_header & 0b0010) != 0;
        twkb_info.has_id_list = (metadata_header & 0b0100) != 0;
        let has_ext_prec_info = (metadata_header & 0b1000) != 0;
        twkb_info.is_empty_geom = (metadata_header & 0b10000) != 0;
        if has_ext_prec_info {
            let ext_prec_info = try!(raw.read_u8());
            twkb_info.has_z = ext_prec_info & 0b0001 != 0;
            twkb_info.has_m = ext_prec_info & 0b0010 != 0;
            twkb_info.prec_z = Some((ext_prec_info & 0x1C) >> 2);
            twkb_info.prec_m = Some((ext_prec_info & 0xE0) >> 5);
        }
        if has_size_attribute {
            twkb_info.size = Some(try!(read_raw_varint64(raw)));
        }
        if has_bbox {
            let _xmin = try!(read_int64(raw));
            let _deltax = try!(read_int64(raw));
            let _ymin = try!(read_int64(raw));
            let _deltay = try!(read_int64(raw));
            if twkb_info.has_z {
                let _zmin = try!(read_int64(raw));
                let _deltaz = try!(read_int64(raw));
            }
            if twkb_info.has_m {
                let _mmin = try!(read_int64(raw));
                let _deltam = try!(read_int64(raw));
            }
        }
        Self::read_twkb_body(raw, &twkb_info)
    }

    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error>;
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
        let b = try!(raw.read_u8());
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

impl TwkbPoint {
    fn has_z() -> bool { false }
    fn has_m() -> bool { false }
    fn new_from_opt_vals(x: f64, y: f64, _z: Option<f64>, _m: Option<f64>) -> Self {
        TwkbPoint { x: x, y: y }
    }
}

impl Point for TwkbPoint {
    fn x(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self) }
    }
    fn y(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self).offset(1) }
    }
}

impl TwkbGeom for TwkbPoint {
    type PointType = TwkbPoint;

    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        if twkb_info.is_empty_geom {
            return Ok(TwkbPoint::new_from_opt_vals(f64::NAN, f64::NAN, None, None));
        }
        let x = try!(read_varint64_as_f64(raw, twkb_info.precision));
        let y = try!(read_varint64_as_f64(raw, twkb_info.precision));
        let z = if twkb_info.has_z {
            Some(try!(read_varint64_as_f64(raw, twkb_info.precision)))
        } else {
            None
        };
        let m = if twkb_info.has_m {
            Some(try!(read_varint64_as_f64(raw, twkb_info.precision)))
        } else {
            None
        };
        Ok(Self::new_from_opt_vals(x, y, z, m))
    }    
}


impl TwkbGeom for TwkbLineString {
    type PointType = TwkbPoint;

    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: &TwkbInfo) -> Result<Self, Error> {
        // npoints           uvarint
        // pointarray        varint[]
        let mut points: Vec<TwkbPoint> = vec![];
        if !twkb_info.is_empty_geom {
            let npoints = try!(read_raw_varint64(raw));
            let mut x = 0.0;
            let mut y = 0.0;
            let mut z = if twkb_info.has_z { Some(0.0) } else { None };
            let mut m = if twkb_info.has_m { Some(0.0) } else { None };
            for _ in 0..npoints {
                let dx = try!(read_varint64_as_f64(raw, twkb_info.precision));
                x += dx;
                let dy = try!(read_varint64_as_f64(raw, twkb_info.precision));
                y += dy;
                if twkb_info.has_z {
                    let dz = try!(read_varint64_as_f64(raw, twkb_info.precision));
                    z = z.map(|v| v + dz);
                };
                if twkb_info.has_m {
                    let dm = try!(read_varint64_as_f64(raw, twkb_info.precision));
                    m = m.map(|v| v + dm);
                };

                points.push(TwkbPoint::new_from_opt_vals(x, y, z, m));
            }
        }
        Ok(TwkbLineString {points: points})
    }
}

impl<'a> LineString<'a> for TwkbLineString {
    type PointType = TwkbPoint;
    fn points(&'a self) -> Points<'a, Self::PointType> {
        Points { iter: self.points.iter() }
    }
}


#[cfg(test)]
fn hex_to_vec(hexstr: &str) -> Vec<u8> {
    hexstr.as_bytes().chunks(2).map(|chars| {
        let hb = if chars[0] <= 57 { chars[0] - 48 } else { chars[0] - 87 };
        let lb = if chars[1] <= 57 { chars[1] - 48 } else { chars[1] - 87 };
        hb * 16 + lb
    }).collect::<Vec<_>>()
}

#[test]
fn test_twkb_to_geom() {
    let twkb = hex_to_vec("01001427"); // SELECT encode(ST_AsTWKB('POINT(10 -20)'::geometry), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "TwkbPoint { x: 10, y: -20 }");

    let twkb = hex_to_vec("0108011427c601"); // SELECT encode(ST_AsTWKB('POINT(10 -20 99)'::geometry), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "TwkbPoint { x: 10, y: -20 }");

    let twkb = hex_to_vec("2100ca019503"); // SELECT encode(ST_AsTWKB('POINT(10.12 -20.34)'::geometry, 1), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "TwkbPoint { x: 10.1, y: -20.3 }");

    let twkb = hex_to_vec("11000203"); // SELECT encode(ST_AsTWKB('POINT(11.12 -22.34)'::geometry, -1), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "TwkbPoint { x: 10, y: -20 }");

    let twkb = hex_to_vec("0110"); // SELECT encode(ST_AsTWKB('POINT EMPTY'::geometry), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "TwkbPoint { x: NaN, y: NaN }");

    let twkb = hex_to_vec("a10080897aff91f401"); // SELECT encode(ST_AsTWKB('SRID=4326;POINT(10 -20)'::geometry), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point), "TwkbPoint { x: 10, y: -20 }");

    let twkb = hex_to_vec("02000214271326"); // SELECT encode(ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry), 'hex')
    let line = TwkbLineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", line), "TwkbLineString { points: [TwkbPoint { x: 10, y: -20 }, TwkbPoint { x: 0, y: -1 }] }");

    let twkb = hex_to_vec("220002c8018f03c7018603"); // SELECT encode(ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry, 1), 'hex')
    let line = TwkbLineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", line), "TwkbLineString { points: [TwkbPoint { x: 10, y: -20 }, TwkbPoint { x: 0, y: -0.5 }] }");

    let twkb = hex_to_vec("0210"); // SELECT encode(ST_AsTWKB('LINESTRING EMPTY'::geometry), 'hex')
    let line = TwkbLineString::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", line), "TwkbLineString { points: [] }");
}
