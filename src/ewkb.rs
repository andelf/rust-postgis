use types::{Point, LineString, Points, EwkbPointGeom, AsEwkbPoint, EwkbLineStringGeom, AsEwkbLineString};
use std::io::prelude::*;
use std::mem;
use std::fmt;
use std::slice::Iter;
use byteorder::{self,ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
use error::Error;


// --- Structs for reading PostGIS geometries into

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

// --- Traits

pub trait EwkbGeometry: fmt::Debug {
    type PointType: Point;

    fn opt_srid(&self) -> Option<i32> {
        None
    }
    fn set_srid(&mut self, _srid: Option<i32>) {
    }
}

pub trait EwkbRead: EwkbGeometry + Sized {
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
}

pub trait EwkbWrite: EwkbGeometry {
    fn type_id(&self) -> u32;

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

// --- Point

impl<'a> EwkbGeometry for EwkbPointGeom<'a> {
    type PointType = EwkbPoint;
    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }
    fn set_srid(&mut self, srid: Option<i32>) {
        self.srid = srid;
    }
}

impl<'a> fmt::Debug for EwkbPointGeom<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "EwkbPointGeom")); //TODO
        Ok(())
    }
}

impl<'a> EwkbPointGeom<'a> {
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
}

impl<'a> EwkbWrite for EwkbPointGeom<'a> {
    fn type_id(&self) -> u32 {
        Self::wkb_type_id(self.opt_srid().is_some())
    }
    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        try!(w.write_f64::<LittleEndian>(self.geom.x()));
        try!(w.write_f64::<LittleEndian>(self.geom.y()));
        self.geom.opt_z().map(|z| w.write_f64::<LittleEndian>(z));
        self.geom.opt_m().map(|m| w.write_f64::<LittleEndian>(m));
        Ok(())
    }
}

impl<'a> AsEwkbPoint<'a> for EwkbPoint {
    fn as_ewkb(&'a self) -> EwkbPointGeom<'a> {
        EwkbPointGeom { geom: self, srid: self.srid }
    }
}

/*
impl EwkbWrite for EwkbPoint {
    fn type_id(&self) -> u32 {
        EwkbPointGeom::wkb_type_id(self.opt_srid().is_some())
    }
    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        //lol
        let x = unsafe { *mem::transmute::<_, *const f64>(self) };
        let y = unsafe { *mem::transmute::<_, *const f64>(self).offset(1) };
        try!(w.write_f64::<LittleEndian>(x));
        try!(w.write_f64::<LittleEndian>(y));
        self.opt_z().map(|z| w.write_f64::<LittleEndian>(z));
        self.opt_m().map(|m| w.write_f64::<LittleEndian>(m));
        Ok(())
     }
}
*/

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

impl EwkbGeometry for EwkbPoint {
    type PointType = EwkbPoint;
    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }
    fn set_srid(&mut self, srid: Option<i32>) {
        self.srid = srid;
    }
}

impl EwkbRead for EwkbPoint {
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
}

// --- LineString

impl EwkbGeometry for EwkbLineString {
    type PointType = EwkbPoint;
    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }
    fn set_srid(&mut self, srid: Option<i32>) {
        self.srid = srid;
    }
}

impl EwkbRead for EwkbLineString {
    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error> {
        let mut points: Vec<EwkbPoint> = vec![];
        let size = try!(read_u32(raw, is_be)) as usize;
        for _ in 0..size {
            points.push(EwkbPoint::read_ewkb_body(raw, is_be).unwrap());
        }
        Ok(EwkbLineString {points: points, srid: None})
    }
}

impl<'a> Points<'a> for EwkbLineString {
    type ItemType = EwkbPoint;
    type Iter = Iter<'a, Self::ItemType>;
    fn points(&'a self) -> Self::Iter {
        self.points.iter()
    }
}

impl<'a> LineString<'a> for EwkbLineString {}

impl<'a, T, I> EwkbGeometry for EwkbLineStringGeom<'a, T, I>
    where T: 'a + Point,
          I: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
{
    type PointType = EwkbPoint;
    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }
    fn set_srid(&mut self, srid: Option<i32>) {
        self.srid = srid;
    }
}

impl<'a, T, I> fmt::Debug for EwkbLineStringGeom<'a, T, I>
    where T: 'a + Point,
          I: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "EwkbLineStringGeom")); //TODO
        Ok(())
    }
}

impl<'a, T, I> EwkbWrite for EwkbLineStringGeom<'a, T, I>
    where T: 'a + Point,
          I: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
{
    fn type_id(&self) -> u32 {
        let type_id = EwkbPointGeom::wkb_type_id(self.opt_srid().is_some());
        (type_id & 0xffff_ff00) | 0x02
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        try!(w.write_u32::<LittleEndian>(self.geom.points().len() as u32));
        for point in self.geom.points() {
            let wkb = EwkbPointGeom { geom: point, srid: self.srid };
            try!(wkb.write_ewkb_body(w));
        }
        Ok(())
    }
}

impl<'a> AsEwkbLineString<'a> for EwkbLineString {
    type PointType = EwkbPoint;
    type Iter = Iter<'a, EwkbPoint>;
    fn as_ewkb(&'a self) -> EwkbLineStringGeom<'a, Self::PointType, Self::Iter> {
        EwkbLineStringGeom { geom: self, srid: self.srid }
    }
}

/*
impl EwkbWrite for EwkbLineString {
    fn type_id(&self) -> u32 {
        let type_id = EwkbPointGeom::wkb_type_id(self.opt_srid().is_some());
        (type_id & 0xffff_ff00) | 0x02
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        try!(w.write_u32::<LittleEndian>(self.points.len() as u32));
        for point in self.points.iter() {
            try!(point.as_ewkb().write_ewkb_body(w));
        }
        Ok(())
    }
}
*/


#[test]
fn test_ewkb_write() {
    // 'POINT (10 -20)'
    let point = EwkbPoint { x: 10.0, y: -20.0, srid: None };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000000000000000000244000000000000034C0");

    // 'SRID=4326;POINT (10 -20)'
    let point = EwkbPoint { x: 10.0, y: -20.0, srid: Some(4326) };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000020E6100000000000000000244000000000000034C0");

    let p = |x, y| EwkbPoint { x: x, y: y, srid: None };
    // 'LINESTRING (10 -20, -0 -0.5)'
    let line = EwkbLineString {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");

    // 'SRID=4326;LINESTRING (10 -20, -0 -0.5)'
    let line = EwkbLineString {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "0102000020E610000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
}

#[test]
fn test_ewkb_adapters() {
    let point = EwkbPoint { x: 10.0, y: -20.0, srid: Some(4326) };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000020E6100000000000000000244000000000000034C0");

    let p = |x, y| EwkbPoint { x: x, y: y, srid: None };
    let line = EwkbLineString {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "0102000020E610000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
}

#[cfg(test)]
fn hex_to_vec(hexstr: &str) -> Vec<u8> {
    hexstr.as_bytes().chunks(2).map(|chars| {
        let hb = if chars[0] <= 57 { chars[0] - 48 } else { chars[0] - 55 };
        let lb = if chars[1] <= 57 { chars[1] - 48 } else { chars[1] - 55 };
        hb * 16 + lb
    }).collect::<Vec<_>>()
}

#[test]
fn test_ewkb_read() {
    // SELECT 'POINT(10 -20)'::geometry
    let ewkb = hex_to_vec("0101000000000000000000244000000000000034C0");
    assert_eq!(ewkb, &[1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 52, 192]);
    let point = EwkbPoint::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000000000000000000244000000000000034C0");

    // SELECT 'LINESTRING (10 -20, -0 -0.5)'::geometry
    let ewkb = hex_to_vec("010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
    let line = EwkbLineString::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
}

#[test]
fn test_iterators() {
    let p = |x, y| EwkbPoint { x: x, y: y, srid: None };
    let line = EwkbLineString {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.points().last(), Some(&EwkbPoint { x: 0., y: -0.5, srid: None }));
}
