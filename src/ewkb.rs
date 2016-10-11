use types::{Point};
use std::io::prelude::*;
use std::mem;
use std::fmt;
use byteorder::{self,ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
use error::Error;


#[derive(PartialEq, Clone, Debug)]
pub struct EwkbPoint {
    pub x: f64,
    pub y: f64,
    pub srid: Option<i32>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct EwkbLineString {
    pub points: Vec<EwkbPoint>,
    pub srid: Option<i32>,
}

pub trait EwkbGeometryType: fmt::Debug + Sized {
    type PointType: Point;

    fn opt_srid(&self) -> Option<i32> {
        None
    }
    fn set_srid(&mut self, _srid: Option<i32>) {
    }

    fn type_id(&self) -> u32;

    fn read_ewkb<R: Read>(raw: &mut R) -> Result<Self, Error> {
        let byte_order = try!(raw.read_i8());
        let is_be = byte_order == 0i8;

        let type_id = try!(read_u32(raw, is_be));
        let mut srid: Option<i32> = None;
        if type_id & 0x20000000 == 0x20000000 {
           srid = Some(try!(read_i32(raw, is_be)));
        }
        let ewkb = Self::read_ewkb_body(raw, is_be);
        ewkb.map(|mut val| { val.set_srid(srid); val } )
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error>;

    fn write_ewkb<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        // use LE
        try!(w.write_u8(0x01));
        let type_id = self.type_id();
        try!(w.write_u32::<LittleEndian>(type_id));
        self.opt_srid().map(|srid| w.write_i32::<LittleEndian>(srid));
        try!(self.write_ewkb_body(w));
        Ok(())
    }
    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error>;

    fn to_hex_ewkb(&self) -> String {
        let mut buf: Vec<u8> = Vec::new();
        let _ = self.write_ewkb(&mut buf).unwrap();
        let hex: String = buf.iter().fold(String::new(), |s, &b| s + &format!("{:02X}", b));
        hex
    }
}


impl From<byteorder::Error> for Error {
    fn from(e: byteorder::Error) -> Error {
        Error::Read(format!("error while reading: {:?}", e))
    }
}

fn read_u32<R: Read>(raw: &mut R, is_be: bool) -> Result<u32, Error> {
    Ok(try!(
        if is_be { raw.read_u32::<BigEndian>() }
        else { raw.read_u32::<LittleEndian>() }
        ))
}

fn read_i32<R: Read>(raw: &mut R, is_be: bool) -> Result<i32, Error> {
    Ok(try!(
        if is_be { raw.read_i32::<BigEndian>() }
        else { raw.read_i32::<LittleEndian>() }
        ))
}

fn read_f64<R: Read>(raw: &mut R, is_be: bool) -> Result<f64, Error> {
    Ok(try!(
        if is_be { raw.read_f64::<BigEndian>() }
        else { raw.read_f64::<LittleEndian>() }
        ))
}


impl EwkbPoint {
    fn has_z() -> bool { false }
    fn has_m() -> bool { false }
    fn wkb_type_id(has_srid: bool) -> u32 {
        let mut type_ = 0x0000_0001_u32;
        if has_srid {
            type_ |= 0x20000000;
        }
        if Self::has_z() {
            type_ |= 0x80000000;
        }
        if Self::has_m() {
            type_ != 0x40000000;
        }
        type_
    }
    fn new_from_opt_vals(x: f64, y: f64, _z: Option<f64>, _m: Option<f64>) -> Self {
        EwkbPoint { x: x, y: y, srid: None }
    }
}

impl Point for EwkbPoint {
    fn x(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self) }
    }
    fn y(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self).offset(1) }
    }
}

impl EwkbGeometryType for EwkbPoint {
    type PointType = EwkbPoint;

    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }
    fn set_srid(&mut self, srid: Option<i32>) {
        self.srid = srid;
    }
    fn type_id(&self) -> u32 {
        Self::wkb_type_id(self.opt_srid().is_some())
    }
    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error> {
        let x = try!(read_f64(raw, is_be));
        let y = try!(read_f64(raw, is_be));
        let z = if Self::has_z() {
            Some(try!(read_f64(raw, is_be)))
        } else {
            None
        };
        let m = if Self::has_m() {
            Some(try!(read_f64(raw, is_be)))
        } else {
            None
        };
        Ok(Self::new_from_opt_vals(x, y, z, m))
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        // lol
        let x = unsafe { *mem::transmute::<_, *const f64>(self) };
        let y = unsafe { *mem::transmute::<_, *const f64>(self).offset(1) };
        try!(w.write_f64::<LittleEndian>(x));
        try!(w.write_f64::<LittleEndian>(y));
        self.opt_z().map(|z| w.write_f64::<LittleEndian>(z));
        self.opt_m().map(|m| w.write_f64::<LittleEndian>(m));
        Ok(())
    }
}


impl EwkbGeometryType for EwkbLineString {
    type PointType = EwkbPoint;

    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }
    fn set_srid(&mut self, srid: Option<i32>) {
        self.srid = srid;
    }

    fn type_id(&self) -> u32 {
        let type_id = EwkbPoint::wkb_type_id(self.opt_srid().is_some());
        (type_id & 0xffff_ff00) | 0x02
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error> {
        let mut points: Vec<EwkbPoint> = vec![];
        let size = try!(read_u32(raw, is_be)) as usize;
        for _ in 0..size {
            points.push(EwkbPoint::read_ewkb_body(raw, is_be).unwrap());
        }
        Ok(EwkbLineString {points: points, srid: None})
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        try!(w.write_u32::<LittleEndian>(self.points.len() as u32));
        for point in self.points.iter() {
            let wkb = EwkbPoint { x: point.x, y: point.y, srid: None };
            try!(wkb.write_ewkb_body(w));
        }
        Ok(())
    }
}


#[test]
fn test_geom_to_wkb() {
    // 'POINT (10 -20)'
    let point = EwkbPoint { x: 10.0, y: -20.0, srid: None };
    assert_eq!(point.to_hex_ewkb(), "0101000000000000000000244000000000000034C0");

    // 'SRID=4326;POINT (10 -20)'
    let point = EwkbPoint { x: 10.0, y: -20.0, srid: Some(4326) };
    assert_eq!(point.to_hex_ewkb(), "0101000020E6100000000000000000244000000000000034C0");

    let p = |x, y| EwkbPoint { x: x, y: y, srid: None };
    // 'LINESTRING (10 -20, -0 -0.5)'
    let line = EwkbLineString {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.to_hex_ewkb(), "010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");

    // 'SRID=4326;LINESTRING (10 -20, -0 -0.5)'
    let line = EwkbLineString {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.to_hex_ewkb(), "0102000020E610000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
}

#[test]
fn test_wkb_to_geom() {
    // 'POINT (10 -20)'
    let mut point_ewkb: &[u8] = &[1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 52, 192];
    let point = EwkbPoint::read_ewkb(&mut point_ewkb).unwrap();
    assert_eq!(point.to_hex_ewkb(), "0101000000000000000000244000000000000034C0");

    // 'LINESTRING (10 -20, -0 -0.5)'
    let mut line_ewkb: &[u8] = &[1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 52, 192, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 224, 191];
    let line = EwkbLineString::read_ewkb(&mut line_ewkb).unwrap();
    assert_eq!(line.to_hex_ewkb(), "010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
}
