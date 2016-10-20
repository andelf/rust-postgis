use types as postgis;
use std::io::prelude::*;
use std::mem;
use std::fmt;
use std::slice::Iter;
use std::iter::FromIterator;
use byteorder::{self,ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
use error::Error;

// OGC WKB specification: http://www.opengeospatial.org/standards/sfa
// PostGIS EWKB extensions: https://svn.osgeo.org/postgis/trunk/doc/ZMSgeoms.txt


// --- Structs for reading PostGIS geometries into

#[derive(PartialEq, Clone, Debug)]
pub struct Point {
    pub x: f64,
    pub y: f64,
    pub srid: Option<i32>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct PointZ {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub srid: Option<i32>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct PointM {
    pub x: f64,
    pub y: f64,
    pub m: f64,
    pub srid: Option<i32>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct PointZM {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub m: f64,
    pub srid: Option<i32>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct PolygonT<P: postgis::Point + EwkbRead> {
    pub rings: Vec<LineStringT<P>>,
    pub srid: Option<i32>
}


// --- Traits

pub trait EwkbRead: fmt::Debug + Sized {
    fn point_type() -> postgis::PointType;

    fn set_srid(&mut self, _srid: Option<i32>) {
    }

    fn read_ewkb<R: Read>(raw: &mut R) -> Result<Self, Error> {
        let byte_order = try!(raw.read_i8());
        let is_be = byte_order == 0i8;

        let type_id = try!(read_u32(raw, is_be));
        let mut srid: Option<i32> = None;
        if type_id & 0x20000000 == 0x20000000 {
           srid = Some(try!(read_i32(raw, is_be)));
        }
        Self::read_ewkb_body(raw, is_be, srid)
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool, srid: Option<i32>) -> Result<Self, Error>;
}

pub trait EwkbWrite: fmt::Debug + Sized {
    fn opt_srid(&self) -> Option<i32> {
        None
    }

    fn wkb_type_id(point_type: &postgis::PointType, srid: Option<i32>) -> u32 {
        let mut type_ = 0;
        if srid.is_some() {
            type_ |= 0x20000000;
        }
        if *point_type == postgis::PointType::PointZ ||
           *point_type == postgis::PointType::PointZM {
            type_ |= 0x80000000;
        }
        if *point_type == postgis::PointType::PointM ||
           *point_type == postgis::PointType::PointZM {
            type_ |= 0x40000000;
        }
        type_
    }

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

// --- helpers

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


// --- Point

impl Point {
    fn has_z() -> bool { false }
    fn has_m() -> bool { false }
    fn new_from_opt_vals(x: f64, y: f64, _z: Option<f64>, _m: Option<f64>, srid: Option<i32>) -> Self {
        Point { x: x, y: y, srid: srid }
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

impl PointZ {
    fn has_z() -> bool { true }
    fn has_m() -> bool { false }
    fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, _m: Option<f64>, srid: Option<i32>) -> Self {
        PointZ { x: x, y: y, z: z.unwrap(), srid: srid }
    }
}

impl postgis::Point for PointZ {
    fn x(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self) }
    }
    fn y(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self).offset(1) }
    }
    fn opt_z(&self) -> Option<f64> {
        Some(unsafe { *mem::transmute::<_, *const f64>(self).offset(2) })
    }
}

impl PointM {
    fn has_z() -> bool { false }
    fn has_m() -> bool { true }
    fn new_from_opt_vals(x: f64, y: f64, _z: Option<f64>, m: Option<f64>, srid: Option<i32>) -> Self {
        PointM { x: x, y: y, m: m.unwrap(), srid: srid }
    }
}

impl postgis::Point for PointM {
    fn x(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self) }
    }
    fn y(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self).offset(1) }
    }
    fn opt_m(&self) -> Option<f64> {
        Some(unsafe { *mem::transmute::<_, *const f64>(self).offset(2) })
    }
}

impl PointZM {
    fn has_z() -> bool { true }
    fn has_m() -> bool { true }
    fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, m: Option<f64>, srid: Option<i32>) -> Self {
        PointZM { x: x, y: y, z: z.unwrap(), m: m.unwrap(), srid: srid }
    }
}

impl postgis::Point for PointZM {
    fn x(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self) }
    }
    fn y(&self) -> f64 {
        unsafe { *mem::transmute::<_, *const f64>(self).offset(1) }
    }
    fn opt_z(&self) -> Option<f64> {
        Some(unsafe { *mem::transmute::<_, *const f64>(self).offset(2) })
    }
    fn opt_m(&self) -> Option<f64> {
        Some(unsafe { *mem::transmute::<_, *const f64>(self).offset(3) })
    }
}


macro_rules! impl_point_read_traits {
    ($ptype:ident) => (
        impl EwkbRead for $ptype {
            fn point_type() -> postgis::PointType {
                postgis::PointType::$ptype
            }
            fn set_srid(&mut self, srid: Option<i32>) {
                self.srid = srid;
            }
            fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool, srid: Option<i32>) -> Result<Self, Error> {
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
                Ok(Self::new_from_opt_vals(x, y, z, m, srid))
            }
        }

        impl<'a> AsEwkbPoint<'a> for $ptype {
            fn as_ewkb(&'a self) -> EwkbPoint<'a> {
                EwkbPoint { geom: self, srid: self.srid, point_type: postgis::PointType::$ptype }
            }
        }
    )
}

impl_point_read_traits!(Point);
impl_point_read_traits!(PointZ);
impl_point_read_traits!(PointM);
impl_point_read_traits!(PointZM);


pub struct EwkbPoint<'a> {
    pub geom: &'a postgis::Point,
    pub srid: Option<i32>,
    pub point_type: postgis::PointType,
}

pub trait AsEwkbPoint<'a> {
    fn as_ewkb(&'a self) -> EwkbPoint<'a>;
}

impl<'a> fmt::Debug for EwkbPoint<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "EwkbPoint")); //TODO
        Ok(())
    }
}

impl<'a> EwkbWrite for EwkbPoint<'a> {
    fn type_id(&self) -> u32 {
        0x01 | Self::wkb_type_id(&self.point_type, self.srid)
    }
    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }
    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        try!(w.write_f64::<LittleEndian>(self.geom.x()));
        try!(w.write_f64::<LittleEndian>(self.geom.y()));
        self.geom.opt_z().map(|z| w.write_f64::<LittleEndian>(z));
        self.geom.opt_m().map(|m| w.write_f64::<LittleEndian>(m));
        Ok(())
    }
}

/*
impl EwkbWrite for Point {
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


macro_rules! point_container_type {
    // points container
    ($geotypetrait:ident for $geotype:ident) => (

        #[derive(PartialEq, Clone, Debug)]
        pub struct $geotype<P: postgis::Point + EwkbRead> {
            pub points: Vec<P>,
            pub srid: Option<i32>,
        }

        impl<P: postgis::Point + EwkbRead> $geotype<P> {
            pub fn new() -> $geotype<P> {
                $geotype { points: Vec::new(), srid: None }
            }
        }

        impl<P> FromIterator<P> for $geotype<P>
            where P: postgis::Point + EwkbRead
        {
            #[inline]
            fn from_iter<I: IntoIterator<Item=P>>(iterable: I) -> $geotype<P> {
                let iterator = iterable.into_iter();
                let (lower, _) = iterator.size_hint();
                let mut ret = $geotype::new();
                ret.points.reserve(lower);
                for item in iterator {
                    ret.points.push(item);
                }
                ret
            }
        }

        impl<'a, P> postgis::$geotypetrait<'a> for $geotype<P>
            where P: 'a + postgis::Point + EwkbRead
        {
            type ItemType = P;
            type Iter = Iter<'a, Self::ItemType>;
            fn points(&'a self) -> Self::Iter {
                self.points.iter()
            }
        }
    )
}

macro_rules! impl_read_for_point_container_type {
    (singletype $geotype:ident) => (

        impl<P> EwkbRead for $geotype<P>
            where P: postgis::Point + EwkbRead
        {
            fn point_type() -> postgis::PointType {
                P::point_type()
            }
            fn set_srid(&mut self, srid: Option<i32>) {
                self.srid = srid;
            }
            fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool, srid: Option<i32>) -> Result<Self, Error> {
                let mut points: Vec<P> = vec![];
                let size = try!(read_u32(raw, is_be)) as usize;
                for _ in 0..size {
                    points.push(P::read_ewkb_body(raw, is_be, srid).unwrap());
                    //points.push($itemtype::read_ewkb(raw).unwrap());
                }
                Ok($geotype::<P> {points: points, srid: srid})
            }
        }
    )
}

macro_rules! point_container_write {
    ($geotypetrait:ident and $asewkbtype:ident for $geotype:ident to $ewkbtype:ident with type code $typecode:expr, command $writecmd:ident) => (

        pub struct $ewkbtype<'a, P, I>
            where P: 'a + postgis::Point,
                  I: 'a + Iterator<Item=&'a P> + ExactSizeIterator<Item=&'a P>
        {
            pub geom: &'a postgis::$geotypetrait<'a, ItemType=P, Iter=I>,
            pub srid: Option<i32>,
            pub point_type: postgis::PointType,
        }

        pub trait $asewkbtype<'a> {
            type PointType: 'a + postgis::Point;
            type Iter: Iterator<Item=&'a Self::PointType>+ExactSizeIterator<Item=&'a Self::PointType>;
            fn as_ewkb(&'a self) -> $ewkbtype<'a, Self::PointType, Self::Iter>;
        }

        impl<'a, T, I> fmt::Debug for $ewkbtype<'a, T, I>
            where T: 'a + postgis::Point,
                  I: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                try!(write!(f, "$ewkbtype")); //TODO
                Ok(())
            }
        }

        impl<'a, T, I> EwkbWrite for $ewkbtype<'a, T, I>
            where T: 'a + postgis::Point,
                  I: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            fn opt_srid(&self) -> Option<i32> {
                self.srid
            }

            fn type_id(&self) -> u32 {
                $typecode | Self::wkb_type_id(&self.point_type, self.srid)
            }

            fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
                try!(w.write_u32::<LittleEndian>(self.geom.points().len() as u32));
                for geom in self.geom.points() {
                    let wkb = EwkbPoint { geom: geom, srid: self.srid, point_type: self.point_type.clone() };
                    try!(wkb.$writecmd(w));
                }
                Ok(())
            }
        }

        impl<'a, P> $asewkbtype<'a> for $geotype<P>
            where P: 'a + postgis::Point + EwkbRead
        {
            type PointType = P;
            type Iter = Iter<'a, P>;
            fn as_ewkb(&'a self) -> $ewkbtype<'a, Self::PointType, Self::Iter> {
                $ewkbtype { geom: self, srid: self.srid, point_type: Self::PointType::point_type() }
            }
        }
    )
}

macro_rules! geometry_container_type {
    // line and polygon containers
    ($geotypetrait:ident for $geotype:ident contains $itemtype:ident named $itemname:ident) => (
        #[derive(PartialEq, Clone, Debug)]
        pub struct $geotype<P: postgis::Point + EwkbRead> {
            pub $itemname: Vec<$itemtype<P>>,
            pub srid: Option<i32>,
        }

        impl<P> $geotype<P>
            where P: postgis::Point + EwkbRead
        {
            pub fn new() -> $geotype<P> {
                $geotype { $itemname: Vec::new(), srid: None }
            }
        }

        impl<P> FromIterator<$itemtype<P>> for $geotype<P>
            where P: postgis::Point + EwkbRead
        {
            #[inline]
            fn from_iter<I: IntoIterator<Item=$itemtype<P>>>(iterable: I) -> $geotype<P> {
                let iterator = iterable.into_iter();
                let (lower, _) = iterator.size_hint();
                let mut ret = $geotype::new();
                ret.$itemname.reserve(lower);
                for item in iterator {
                    ret.$itemname.push(item);
                }
                ret
            }
        }

        impl<'a, P> postgis::$geotypetrait<'a> for $geotype<P>
            where P: 'a + postgis::Point + EwkbRead
        {
            type ItemType = $itemtype<P>;
            type Iter = Iter<'a, Self::ItemType>;
            fn $itemname(&'a self) -> Self::Iter {
                self.$itemname.iter()
            }
        }
    )
}

macro_rules! impl_read_for_geometry_container_type {
    (multitype $geotype:ident contains $itemtype:ident named $itemname:ident) => (
        impl<P> EwkbRead for $geotype<P>
            where P: postgis::Point + EwkbRead
        {
            fn point_type() -> postgis::PointType {
                P::point_type()
            }
            fn set_srid(&mut self, srid: Option<i32>) {
                self.srid = srid;
            }
            fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool, srid: Option<i32>) -> Result<Self, Error> {
                let mut $itemname: Vec<$itemtype<P>> = vec![];
                let size = try!(read_u32(raw, is_be)) as usize;
                for _ in 0..size {
                    //$itemname.push($itemtype::read_ewkb_body(raw, is_be, srid).unwrap());
                    $itemname.push($itemtype::read_ewkb(raw).unwrap());
                 }
                Ok($geotype::<P> {$itemname: $itemname, srid: srid})
            }
        }
    )
}

macro_rules! geometry_container_write {
    ($geotypetrait:ident and $asewkbtype:ident for $geotype:ident to $ewkbtype:ident with type code $typecode:expr, contains $ewkbitemtype:ident, $itemtype:ident as $itemtypetrait:ident named $itemname:ident, command $writecmd:ident) => (

        pub struct $ewkbtype<'a, P, I, T, J>
            where P: 'a + postgis::Point,
                  I: 'a + Iterator<Item=&'a P> + ExactSizeIterator<Item=&'a P>,
                  T: 'a + postgis::$itemtypetrait<'a, ItemType=P, Iter=I>,
                  J: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            pub geom: &'a postgis::$geotypetrait<'a, ItemType=T, Iter=J>,
            pub srid: Option<i32>,
            pub point_type: postgis::PointType,
        }

        pub trait $asewkbtype<'a> {
            type PointType: 'a + postgis::Point;
            type PointIter: Iterator<Item=&'a Self::PointType>+ExactSizeIterator<Item=&'a Self::PointType>;
            type ItemType: 'a + postgis::$itemtypetrait<'a, ItemType=Self::PointType, Iter=Self::PointIter>;
            type Iter: Iterator<Item=&'a Self::ItemType>+ExactSizeIterator<Item=&'a Self::ItemType>;
            fn as_ewkb(&'a self) -> $ewkbtype<'a, Self::PointType, Self::PointIter, Self::ItemType, Self::Iter>;
        }

        impl<'a, P, I, T, J> fmt::Debug for $ewkbtype<'a, P, I, T, J>
            where P: 'a + postgis::Point,
                  I: 'a + Iterator<Item=&'a P> + ExactSizeIterator<Item=&'a P>,
                  T: 'a + postgis::$itemtypetrait<'a, ItemType=P, Iter=I>,
                  J: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                try!(write!(f, "EwkbMultiLineString")); //TODO
                Ok(())
            }
        }

        impl<'a, P, I, T, J> EwkbWrite for $ewkbtype<'a, P, I, T, J>
            where P: 'a + postgis::Point,
                  I: 'a + Iterator<Item=&'a P> + ExactSizeIterator<Item=&'a P>,
                  T: 'a + postgis::$itemtypetrait<'a, ItemType=P, Iter=I>,
                  J: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            fn opt_srid(&self) -> Option<i32> {
                self.srid
            }

            fn type_id(&self) -> u32 {
                $typecode | Self::wkb_type_id(&self.point_type, self.srid)
            }

            fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
                try!(w.write_u32::<LittleEndian>(self.geom.lines().len() as u32));
                for geom in self.geom.$itemname() {
                    let wkb = $ewkbitemtype { geom: geom, srid: self.srid, point_type: self.point_type.clone() };
                    try!(wkb.$writecmd(w));
                }
                Ok(())
            }
        }

        impl<'a, P> $asewkbtype<'a> for $geotype<P>
            where P: 'a + postgis::Point + EwkbRead
        {
            type PointType = P;
            type PointIter = Iter<'a, P>;
            type ItemType = $itemtype<P>;
            type Iter = Iter<'a, Self::ItemType>;
            fn as_ewkb(&'a self) -> $ewkbtype<'a, Self::PointType, Self::PointIter, Self::ItemType, Self::Iter> {
                $ewkbtype { geom: self, srid: self.srid, point_type: Self::PointType::point_type() }
            }
        }
    )
}


/// LineString
point_container_type!(LineString for LineStringT);
impl_read_for_point_container_type!(singletype LineStringT);
point_container_write!(LineString and AsEwkbLineString for LineStringT
                       to EwkbLineString with type code 0x02,
                       command write_ewkb_body);
/// MultiLineString
geometry_container_type!(MultiLineString for MultiLineStringT contains LineStringT named lines);
impl_read_for_geometry_container_type!(multitype MultiLineStringT contains LineStringT named lines);
geometry_container_write!(MultiLineString and AsEwkbMultiLineString for MultiLineStringT
                          to EwkbMultiLineString with type code 0x05,
                          contains EwkbLineString,LineStringT as LineString named lines,
                          command write_ewkb);


pub type LineString = LineStringT<Point>;
pub type LineStringZ = LineStringT<PointZ>;
pub type LineStringM = LineStringT<PointM>;
pub type LineStringZM = LineStringT<PointZM>;

pub type MultiLineString = MultiLineStringT<Point>;
pub type MultiLineStringZ = MultiLineStringT<PointZ>;
pub type MultiLineStringM = MultiLineStringT<PointM>;
pub type MultiLineStringZM = MultiLineStringT<PointZM>;


// --- Polygon

impl<P> EwkbRead for PolygonT<P>
    where P: postgis::Point + EwkbRead
{
    fn point_type() -> postgis::PointType {
        P::point_type()
    }
    fn set_srid(&mut self, srid: Option<i32>) {
        self.srid = srid;
    }
    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool, srid: Option<i32>) -> Result<Self, Error> {
        let mut rings: Vec<LineStringT<P>> = vec![];
        let size = try!(read_u32(raw, is_be)) as usize;
        for _ in 0..size {
            rings.push(LineStringT::read_ewkb_body(raw, is_be, srid).unwrap());
        }
        Ok(PolygonT::<P> {rings: rings, srid: srid})
    }
}

impl<'a, P> postgis::Polygon<'a> for PolygonT<P>
    where P: 'a + postgis::Point + EwkbRead
{
    type ItemType = LineStringT<P>;
    type Iter = Iter<'a, Self::ItemType>;
    fn rings(&'a self) -> Self::Iter {
        self.rings.iter()
    }
}

pub type Polygon = PolygonT<Point>;
pub type PolygonZ = PolygonT<PointZ>;
pub type PolygonM = PolygonT<PointM>;
pub type PolygonZM = PolygonT<PointZM>;


#[test]
fn test_point_write() {
    // 'POINT (10 -20)'
    let point = Point { x: 10.0, y: -20.0, srid: None };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000000000000000000244000000000000034C0");

    // 'POINT (10 -20 100)'
    let point = PointZ { x: 10.0, y: -20.0, z: 100.0, srid: None };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000080000000000000244000000000000034C00000000000005940");

    // 'POINTM (10 -20 1)'
    let point = PointM { x: 10.0, y: -20.0, m: 1.0, srid: None };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000040000000000000244000000000000034C0000000000000F03F");

    // 'POINT (10 -20 100 1)'
    let point = PointZM { x: 10.0, y: -20.0, z: 100.0, m: 1.0, srid: None };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "01010000C0000000000000244000000000000034C00000000000005940000000000000F03F");

    // 'POINT (-0 -1)'
    let point = Point { x: 0.0, y: -1.0, srid: None };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "01010000000000000000000000000000000000F0BF");
    // TODO: -0 in PostGIS gives 01010000000000000000000080000000000000F0BF

    // 'SRID=4326;POINT (10 -20)'
    let point = Point { x: 10.0, y: -20.0, srid: Some(4326) };
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000020E6100000000000000000244000000000000034C0");
}

#[test]
fn test_line_write() {
    let p = |x, y| Point { x: x, y: y, srid: None };
    // 'LINESTRING (10 -20, 0 -0.5)'
    let line = LineStringT::<Point> {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");

    // 'SRID=4326;LINESTRING (10 -20, 0 -0.5)'
    let line = LineStringT::<Point> {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "0102000020E610000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");

    let p = |x, y, z| PointZ { x: x, y: y, z: z, srid: Some(4326) };
    // 'SRID=4326;LINESTRING (10 -20 100, 0 0.5 101)'
    let line = LineStringT::<PointZ> {srid: Some(4326), points: vec![p(10.0, -20.0, 100.0), p(0., -0.5, 101.0)]};
    assert_eq!(line.as_ewkb().to_hex_ewkb(), "01020000A0E610000002000000000000000000244000000000000034C000000000000059400000000000000000000000000000E0BF0000000000405940");
}

#[test]
fn test_multiline_write() {
    let p = |x, y| Point { x: x, y: y, srid: None };
    // SELECT 'MULTILINESTRING ((10 -20, 0 -0.5), (0 0, 2 0))'::geometry
    let line1 = LineStringT::<Point> {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]};
    let line2 = LineStringT::<Point> {srid: None, points: vec![p(0., 0.), p(2., 0.)]};
    let multiline = MultiLineStringT::<Point> {srid: None,lines: vec![line1, line2]};
    assert_eq!(multiline.as_ewkb().to_hex_ewkb(), "010500000002000000010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF0102000000020000000000000000000000000000000000000000000000000000400000000000000000");
}

#[test]
fn test_ewkb_adapters() {
    let point = Point { x: 10.0, y: -20.0, srid: Some(4326) };
    let ewkb = EwkbPoint { geom: &point, srid: Some(4326), point_type: postgis::PointType::Point };
    assert_eq!(ewkb.to_hex_ewkb(), "0101000020E6100000000000000000244000000000000034C0");
    assert_eq!(point.as_ewkb().to_hex_ewkb(), "0101000020E6100000000000000000244000000000000034C0");
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
fn test_point_read() {
    // SELECT 'POINT(10 -20)'::geometry
    let ewkb = hex_to_vec("0101000000000000000000244000000000000034C0");
    assert_eq!(ewkb, &[1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 52, 192]);
    let point = Point::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(point, Point { x: 10.0, y: -20.0, srid: None });

    // SELECT 'POINT(10 -20 100)'::geometry
    let ewkb = hex_to_vec("0101000080000000000000244000000000000034C00000000000005940");
    let point = PointZ::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(point, PointZ { x: 10.0, y: -20.0, z: 100.0, srid: None });

    let point = Point::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(point, Point { x: 10.0, y: -20.0, srid: None });

    // SELECT 'POINTM(10 -20 1)'::geometry
    let ewkb = hex_to_vec("0101000040000000000000244000000000000034C0000000000000F03F");
    let point = PointM::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(point, PointM { x: 10.0, y: -20.0, m: 1.0, srid: None });

    // SELECT 'POINT(10 -20 100 1)'::geometry
    let ewkb = hex_to_vec("01010000C0000000000000244000000000000034C00000000000005940000000000000F03F");
    let point = PointZM::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(point, PointZM { x: 10.0, y: -20.0, z: 100.0, m: 1.0, srid: None });
}

#[test]
fn test_line_read() {
    let p = |x, y| Point { x: x, y: y, srid: None };
    // SELECT 'LINESTRING (10 -20, 0 -0.5)'::geometry
    let ewkb = hex_to_vec("010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF");
    let line = LineStringT::<Point>::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(line, LineStringT::<Point> {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]});

    let p = |x, y, z| PointZ { x: x, y: y, z: z, srid: Some(4326) };
    // SELECT 'SRID=4326;LINESTRING (10 -20 100, 0 0.5 101)'::geometry
    let ewkb = hex_to_vec("01020000A0E610000002000000000000000000244000000000000034C000000000000059400000000000000000000000000000E0BF0000000000405940");
    let line = LineStringT::<PointZ>::read_ewkb(&mut ewkb.as_slice()).unwrap();
    assert_eq!(line, LineStringT::<PointZ> {srid: Some(4326), points: vec![p(10.0, -20.0, 100.0), p(0., -0.5, 101.0)]});
}

#[test]
fn test_multline_read() {
    let p = |x, y| Point { x: x, y: y, srid: None };
    // SELECT 'MULTILINESTRING ((10 -20, 0 -0.5), (0 0, 2 0))'::geometry
    let ewkb = hex_to_vec("010500000002000000010200000002000000000000000000244000000000000034C00000000000000000000000000000E0BF0102000000020000000000000000000000000000000000000000000000000000400000000000000000");
    let poly = MultiLineStringT::<Point>::read_ewkb(&mut ewkb.as_slice()).unwrap();
    let line1 = LineStringT::<Point> {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]};
    let line2 = LineStringT::<Point> {srid: None, points: vec![p(0., 0.), p(2., 0.)]};
    assert_eq!(poly, MultiLineStringT::<Point> {srid: None, lines: vec![line1, line2]});
}

#[test]
fn test_polygon_read() {
    let p = |x, y| Point { x: x, y: y, srid: None };
    // SELECT 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'::geometry
    let ewkb = hex_to_vec("010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000");
    let poly = PolygonT::<Point>::read_ewkb(&mut ewkb.as_slice()).unwrap();
    let line = LineStringT::<Point> {srid: None, points: vec![p(0., 0.), p(2., 0.), p(2., 2.), p(0., 2.), p(0., 0.)]};
    assert_eq!(poly, PolygonT::<Point> {srid: None, rings: vec![line]});
}

#[test]
fn test_iterators() {
    // Iterator traits:
    use types::LineString;

    let p = |x, y| Point { x: x, y: y, srid: None };
    let line = self::LineStringT::<Point> {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
    assert_eq!(line.points().last(), Some(&Point { x: 0., y: -0.5, srid: None }));
}
