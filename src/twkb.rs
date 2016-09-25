use geo;
use std::io::prelude::*;
use std::fmt;
use std::u8;
use std::f64;
use byteorder::ReadBytesExt;
use error::Error;

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
        twkb_info.precision = Self::decode_zig_zag_64(((type_and_prec & 0xF0) >> 4) as u64) as i8;
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
            twkb_info.size = Some(try!(Self::read_raw_varint64(raw)));
        }
        if has_bbox {
            let _xmin = try!(Self::read_int64(raw));
            let _deltax = try!(Self::read_int64(raw));
            let _ymin = try!(Self::read_int64(raw));
            let _deltay = try!(Self::read_int64(raw));
            if twkb_info.has_z {
                let _zmin = try!(Self::read_int64(raw));
                let _deltaz = try!(Self::read_int64(raw));
            }
            if twkb_info.has_m {
                let _mmin = try!(Self::read_int64(raw));
                let _deltam = try!(Self::read_int64(raw));
            }
        }
        Self::read_twkb_body(raw, twkb_info)
    }

    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: TwkbInfo) -> Result<Self, Error>;

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
        Self::read_raw_varint64(raw).map(|v| v as i64)
    }

    fn decode_zig_zag_64(n: u64) -> i64 {
        ((n >> 1) as i64) ^ (-((n & 1) as i64))
    }

    fn varint64_to_f64(varint: u64, precision: i8) -> f64 {
        if precision >= 0 {
            (Self::decode_zig_zag_64(varint) as f64) / 10u8.pow(precision as u32) as f64
        } else {
            (Self::decode_zig_zag_64(varint) as f64) * 10u8.pow(precision.abs() as u32) as f64            
        }
    }

    fn read_varint64_as_f64<R: Read>(raw: &mut R, precision: i8) -> Result<f64, Error> {
        Self::read_raw_varint64(raw).map(|v| Self::varint64_to_f64(v, precision))
    }
}

#[derive(Debug)]
pub struct TwkbPoint {
    pub geom: geo::Point<f64>,
}

impl TwkbGeom for TwkbPoint {
    fn read_twkb_body<R: Read>(raw: &mut R, twkb_info: TwkbInfo) -> Result<Self, Error> {
        if twkb_info.is_empty_geom {
            return Ok(TwkbPoint {geom: geo::Point::new(f64::NAN, f64::NAN)});
        }
        let x = try!(Self::read_varint64_as_f64(raw, twkb_info.precision));
        let y = try!(Self::read_varint64_as_f64(raw, twkb_info.precision));
        if twkb_info.has_z {
            let _z = try!(Self::read_varint64_as_f64(raw, twkb_info.precision));
        }
        if twkb_info.has_m {
            let _m = try!(Self::read_varint64_as_f64(raw, twkb_info.precision));
        }
        Ok(TwkbPoint {geom: geo::Point::new(x, y)}) // Self::new_from_opt_vals(x, y, z, m)
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
    assert_eq!(format!("{:?}", point.geom), "Point(Coordinate { x: 10, y: -20 })");

    let twkb = hex_to_vec("0108011427c601"); // SELECT encode(ST_AsTWKB('POINT(10 -20 99)'::geometry), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point.geom), "Point(Coordinate { x: 10, y: -20 })");

    let twkb = hex_to_vec("2100ca019503"); // SELECT encode(ST_AsTWKB('POINT(10.12 -20.34)'::geometry, 1), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point.geom), "Point(Coordinate { x: 10.1, y: -20.3 })");

    let twkb = hex_to_vec("11000203"); // SELECT encode(ST_AsTWKB('POINT(11.12 -22.34)'::geometry, -1), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point.geom), "Point(Coordinate { x: 10, y: -20 })");

    let twkb = hex_to_vec("0110"); // SELECT encode(ST_AsTWKB('POINT EMPTY'::geometry), 'hex')
    let point = TwkbPoint::read_twkb(&mut twkb.as_slice()).unwrap();
    assert_eq!(format!("{:?}", point.geom), "Point(Coordinate { x: NaN, y: NaN })");
}
