//  FileName    : lib.rs
//  Author      : ShuYu Wang <andelf@gmail.com>
//  Created     : Wed May 27 01:45:41 2015 by ShuYu Wang
//  Copyright   : Feather Workshop (c) 2015
//  Description : PostGIS helper
//  Time-stamp: <2015-06-13 19:21:08 andelf>

#[macro_use(to_sql_checked)]
extern crate postgres;
extern crate byteorder;

use std::io::prelude::*;
use std::fmt;
use std::marker::PhantomData;
use std::iter::FromIterator;
use std::convert::From;
use postgres::types::{Type, IsNull, ToSql, FromSql, SessionInfo};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
pub mod mars;

#[derive(Debug, )]
pub enum Error {
    Read(String),
    Write(String),
    Other(String)
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}


impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Read(_)  => "postgis error while reading",
            Error::Write(_) => "postgis error while writing",
            Error::Other(_) => "postgis unknown error"
        }
    }
}

impl From<byteorder::Error> for Error {
    fn from(e: byteorder::Error) -> Error {
        Error::Read(format!("error while reading: {:?}", e))
    }
}

impl From<Error> for postgres::error::Error {
    fn from(e: Error) -> postgres::error::Error {
        postgres::error::Error::Conversion(Box::new(e))
    }
}

trait Geometry: fmt::Debug + Sized {
    type PointType: ToPoint;
    #[inline(always)]
    fn type_id() -> u32;

    fn read_ewkb<R: Read>(raw: &mut R) -> Result<Self, Error> {
        let byte_order = try!(raw.read_i8());
        let is_be = byte_order == 0i8;

        let type_id = try!(read_u32(raw, is_be));
        if type_id != Self::type_id() {
            return Err(Error::Read("type id not match".into()))
        }

        match Self::PointType::opt_srid() {
            Some(srid) => {
                if try!(read_i32(raw, is_be)) != srid {
                    return Err(Error::Read("srid not match".into()))
                }
            },
            _ => ()
        }
        Self::read_ewkb_body(raw, is_be)
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error>;

    fn write_ewkb<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<(), Error> {
        // use LE
        try!(w.write_u8(0x01));
        let type_id = Self::type_id();
        try!(w.write_u32::<LittleEndian>(type_id));
        Self::PointType::opt_srid().map(|srid| w.write_i32::<LittleEndian>(srid));
        try!(self.write_ewkb_body(w));
        Ok(())
    }
    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error>;
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


pub trait SRID {
    #[inline(always)]
    fn as_srid() -> Option<i32>;
}
/// WGS84 — SRID 4326
#[derive(Debug)]
#[allow(missing_copy_implementations)] pub enum WGS84 {}

/// UTM, Zone 17N, NAD27 — SRID 2029
#[derive(Debug)]
#[allow(missing_copy_implementations)] pub enum NAD27 {}

/// Undefined spheroid (value 0)
#[derive(Debug)]
#[allow(missing_copy_implementations)] pub enum NoSRID {}

impl SRID for WGS84 {
    fn as_srid() -> Option<i32> { Some(4326) }
}

impl SRID for NAD27 {
    fn as_srid() -> Option<i32> { Some(2029) }
}

impl SRID for NoSRID {
    fn as_srid() -> Option<i32> { None }
}

mod detail {
    use std::io::prelude::*;
    use std::mem;
    use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
    use postgres::types::Type;
    use super::{Error, SRID, read_f64, read_u32, read_i32};

    pub trait ToPoint: Sized {
        type SRIDType: SRID;

        fn type_id() -> u32 {
            let mut type_ = 0x0000_0001_u32;
            if Self::opt_srid().is_some() {
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
        fn opt_srid() -> Option<i32> {
            Self::SRIDType::as_srid()
        }
        fn x(&self) -> f64 {
            unsafe { *mem::transmute::<_, *const f64>(self) }
        }
        fn y(&self) -> f64 {
            unsafe { *mem::transmute::<_, *const f64>(self).offset(1) }
        }
        fn opt_z(&self) -> Option<f64> {
            None
        }
        fn opt_m(&self) -> Option<f64> {
            None
        }
        fn has_z() -> bool { false }
        fn has_m() -> bool { false }

        fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Self;

        fn read_ewkb<R: Read>(raw: &mut R) -> Result<Self, Error> {
            let byte_order = try!(raw.read_i8());
            let is_be = byte_order == 0i8;

            let type_id = try!(read_u32(raw, is_be));
            if type_id != Self::type_id() {
                return Err(Error::Read("type id not match".into()))
            }

            if Self::opt_srid().is_some() {
                if Self::opt_srid() != Some(try!(read_i32(raw, is_be))) {
                    return Err(Error::Read("srid not match".into()))
                }
            }

            Self::read_ewkb_body(raw, is_be)
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

        fn write_ewkb<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<(), Error> {
            // use LE
            try!(w.write_u8(0x01));
            try!(w.write_u32::<LittleEndian>(Self::type_id()));
            Self::opt_srid().map(|srid| w.write_i32::<LittleEndian>(srid));
            try!(self.write_ewkb_body(w));
            Ok(())
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

        fn describ(&self) -> String {
            let mut ret = "POINT".to_string();
            self.opt_z().map(|_| ret.push_str("Z"));
            self.opt_m().map(|_| ret.push_str("M"));
            // lol
            let x = unsafe { *mem::transmute::<_, *const f64>(self) };
            let y = unsafe { *mem::transmute::<_, *const f64>(self).offset(1) };
            ret.push_str(&format!("({} {}", x, y));
            self.opt_z().map(|z| ret.push_str(&format!(" {}", z)));
            self.opt_m().map(|m| ret.push_str(&format!(" {}", m)));
            ret.push_str(")");
            ret
        }
    }
}
use detail::ToPoint;

#[derive(Copy, Clone)]
pub struct Point<S: SRID = WGS84> {
    pub x: f64,
    pub y: f64,
    phantom: PhantomData<S>
}

impl<S: SRID> Point<S> {
    pub fn new(x: f64, y: f64) -> Point<S> {
        Point { x: x, y: y, phantom: PhantomData }
    }
}

impl Point<WGS84> {
    pub fn new_wgs84(x: f64, y: f64) -> Point<WGS84> {
            Point::new(x, y)
    }
    pub fn from_gcj02(x: f64, y: f64) -> Point<WGS84> {
        let (x0, y0) = mars::to_wgs84(x, y);
        Point::new(x0, y0)
    }
    pub fn to_gcj02(&self) -> (f64, f64) {
        mars::from_wgs84(self.x, self.y)
    }
}

#[derive(Copy, Clone)]
pub struct PointZ<S: SRID = WGS84> {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    phantom: PhantomData<S>
}

impl<S: SRID> PointZ<S> {
    pub fn new(x: f64, y: f64, z: f64) -> PointZ<S> {
        PointZ { x: x, y: y, z: z, phantom: PhantomData }
    }
}

#[derive(Copy, Clone)]
pub struct PointM<S: SRID = WGS84> {
    pub x: f64,
    pub y: f64,
    pub m: f64,
    phantom: PhantomData<S>
}

impl<S: SRID> PointM<S> {
    pub fn new(x: f64, y: f64, m: f64) -> PointM<S> {
        PointM { x: x, y: y, m: m, phantom: PhantomData }
    }
}

#[derive(Copy, Clone)]
pub struct PointZM<S: SRID = WGS84> {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub m: f64,
    phantom: PhantomData<S>
}

impl<S: SRID> PointZM<S> {
    pub fn new(x: f64, y: f64, z: f64, m: f64) -> PointZM<S> {
        PointZM { x: x, y: y, z: z, m: m, phantom: PhantomData }
    }
}

impl<S: SRID> ToPoint for Point<S> {
    type SRIDType = S;
    fn new_from_opt_vals(x: f64, y: f64, _z: Option<f64>, _m: Option<f64>) -> Self {
        Point { x: x, y: y,  phantom: PhantomData }
    }
}

impl<S: SRID> ToPoint for PointZ<S> {
    type SRIDType = S;
    fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, _m: Option<f64>) -> Self {
        PointZ { x: x, y: y, z: z.unwrap(), phantom: PhantomData }
    }
    fn opt_z(&self) -> Option<f64> {
        Some(self.z)
    }
    fn has_z() -> bool { true }
}
impl<S: SRID> ToPoint for PointM<S> {
    type SRIDType = S;
    fn new_from_opt_vals(x: f64, y: f64, _z: Option<f64>, m: Option<f64>) -> Self {
        PointM { x: x, y: y, m: m.unwrap(), phantom: PhantomData }
    }
    fn opt_m(&self) -> Option<f64> {
        Some(self.m)
    }
    fn has_m() -> bool { true }
}

impl<S: SRID> ToPoint for PointZM<S> {
    type SRIDType = S;
    fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Self {
        PointZM { x: x, y: y, z: z.unwrap(), m: m.unwrap(), phantom: PhantomData }
    }

    fn opt_z(&self) -> Option<f64> {
        Some(self.z)
    }
    fn opt_m(&self) -> Option<f64> {
        Some(self.m)
    }
    fn has_z() -> bool {
        true
    }
    fn has_m() -> bool {
        true
    }
}

macro_rules! accepts_geography {
    () => (
        fn accepts(ty: &Type) -> bool {
            match ty {
                &Type::Other(ref t) if t.name() == "geography" => true,
                &Type::Other(ref t) if t.name() == "geometry"  => true,
                _ => false
            }
        }
    )
}

macro_rules! impl_traits_for_point {
    ($ptype:ident) => (
        impl<S: SRID> FromSql for $ptype<S> {
            accepts_geography!();
            fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<$ptype<S>> {
                <$ptype<S> as ToPoint>::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to ToPoint", ty).into(); postgres::error::Error::Conversion(err)})
            }
        }

        impl<S: SRID> ToSql for $ptype<S> {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, ty: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.write_ewkb(ty, out));
                Ok(IsNull::No)
            }
        }

        impl<S: SRID> fmt::Display for $ptype<S> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                try!(write!(f, "{}", self.describ()));
                Ok(())
            }
        }
        impl<S: SRID> fmt::Debug for $ptype<S> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match <$ptype<S> as ToPoint>::opt_srid() {
                    Some(srid) =>
                        try!(write!(f, "SRID={};{}", srid, self.describ())),
                    None =>
                        try!(write!(f, "{}", self.describ()))
                }
                Ok(())
            }
        }
    )
}


impl_traits_for_point!(Point);
impl_traits_for_point!(PointZ);
impl_traits_for_point!(PointM);
impl_traits_for_point!(PointZM);

// Non-Point type Macro
macro_rules! define_geometry_container_type {
    // points container
    ($geotype:ident of type code $typecode:expr, contains points) => (
        #[derive(Debug)]
        pub struct $geotype<P> {
            pub points: Vec<P>,
        }

        impl<P: ToPoint> $geotype<P> {
            pub fn new() -> $geotype<P> {
                $geotype { points: Vec::new() }
            }
        }

        impl<P: ToPoint> FromIterator<P> for $geotype<P> {
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

        impl<P: ToPoint + fmt::Debug> Geometry for $geotype<P> {
            type PointType = P;
            fn type_id() -> u32 {
                let type_id = P::type_id();
                (type_id & 0xffff_ff00) | $typecode
            }

            fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error> {
                let mut ret = $geotype::new();
                let size = try!(read_u32(raw, is_be)) as usize;
                for _ in 0..size {
                    ret.points.push(P::read_ewkb_body(raw, is_be).unwrap())
                }
                Ok(ret)
            }

            fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
                try!(w.write_u32::<LittleEndian>(self.points.len() as u32));
                for point in self.points.iter() {
                    try!(point.write_ewkb_body(w));
                }
                Ok(())
            }

        }

        impl<P: ToPoint + fmt::Debug> ToSql for $geotype<P> {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.write_ewkb(ty, w));
                Ok(IsNull::No)
            }

        }
        impl<P: ToPoint + fmt::Debug> FromSql for $geotype<P> {
            accepts_geography!();
            fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<$geotype<P>> {
                <Self as Geometry>::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to Geometry", ty).into(); postgres::error::Error::Conversion(err)})
            }
        }
        );
    // common geo type contrainer
    ($geotype:ident of type code $typecode:expr, contains $itemtype:ident named $itemname: ident) => (
        #[derive(Debug)]
        pub struct $geotype<P> {
            pub $itemname: Vec<$itemtype<P>>
        }

        impl<P: ToPoint> $geotype<P> {
            pub fn new() -> $geotype<P> {
                $geotype { $itemname: Vec::new() }
            }
        }

        impl<P: ToPoint> FromIterator<$itemtype<P>> for $geotype<P> {
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

        impl<P: ToPoint + fmt::Debug> Geometry for $geotype<P> {
            type PointType = P;
            fn type_id() -> u32 {
                let type_id = P::type_id();
                (type_id & 0xffff_ff00) | $typecode
            }
            fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error> {
                let mut ret = $geotype::new();
                let size = try!(read_u32(raw, is_be)) as usize;
                for _ in 0..size {
                    ret.$itemname.push($itemtype::read_ewkb_body(raw, is_be).unwrap())
                }
                Ok(ret)
            }
            fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
                try!(w.write_u32::<LittleEndian>(self.$itemname.len() as u32));
                for item in self.$itemname.iter() {
                    try!(item.write_ewkb_body(w));
                }
                Ok(())
            }

        }

        impl<P: ToPoint + fmt::Debug> ToSql for $geotype<P> {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.write_ewkb(ty, w));
                Ok(IsNull::No)
            }

        }

        impl<P: ToPoint + fmt::Debug> FromSql for $geotype<P> {
            accepts_geography!();
            fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<$geotype<P>> {
                <Self as Geometry>::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to Geometry", ty).into(); postgres::error::Error::Conversion(err)})
            }
        }
    )
}


/// LineString
define_geometry_container_type!(LineString of type code 0x02, contains points);
/// Polygon
define_geometry_container_type!(Polygon of type code 0x03, contains LineString named rings);
/// MultiPoint
define_geometry_container_type!(MultiPoint of type code 0x04, contains points);
/// MultiLineString
define_geometry_container_type!(MultiLineString of type code 0x05, contains LineString named lines);
/// MultiPolygon
define_geometry_container_type!(MultiPolygon of type code 0x06, contains Polygon named polygons);


impl<P: ToPoint> fmt::Display for MultiPoint<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "MULTIPOINT"));
        if P::has_z() {
            try!(f.write_str("Z"));
        }
        if P::has_m() {
            try!(f.write_str("M"));
        }
        try!(f.write_str("("));
        for (i, point) in self.points.iter().enumerate() {
            if i >= 1 {
                try!(write!(f, ","));
            }
            try!(write!(f, "{} {}", point.x(), point.y()));
            point.opt_z().map(|z| write!(f, " {}", z));
            point.opt_m().map(|m| write!(f, " {}", m));
        }
        try!(write!(f, ")"));
        Ok(())
    }
}


impl<P: ToPoint> fmt::Display for LineString<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "LINESTRING"));
        if P::has_z() {
            try!(f.write_str("Z"));
        }
        if P::has_m() {
            try!(f.write_str("M"));
        }
        try!(f.write_str("("));
        for (i, point) in self.points.iter().enumerate() {
            if i >= 1 {
                try!(write!(f, ","));
            }
            try!(write!(f, "{} {}", point.x(), point.y()));
            point.opt_z().map(|z| write!(f, " {}", z));
            point.opt_m().map(|m| write!(f, " {}", m));
        }
        try!(write!(f, ")"));
        Ok(())
    }
}

/// Generic Geometry Data Type
#[derive(Debug)]
pub enum GeometryType<P> {
    Point(P),
    LineString(LineString<P>),
    Polygon(Polygon<P>),
    MultiPoint(MultiPoint<P>),
    MultiLineString(MultiLineString<P>),
    MultiPolygon(MultiPolygon<P>),
    GeometryCollection(GeometryCollection<P>)
}

/// GeometryCollection
#[derive(Debug)]
pub struct GeometryCollection<P> {
    pub geometries: Vec<GeometryType<P>>
}

impl<P: ToPoint> GeometryCollection<P> {
    pub fn new() -> GeometryCollection<P> {
        GeometryCollection { geometries: Vec::new() }
    }
}
impl<P: ToPoint + fmt::Debug> Geometry for GeometryCollection<P> {
    type PointType = P;
    fn type_id() -> u32 {
        let type_id = P::type_id();
        (type_id & 0xffff_ff00) | 0x0000_0007
    }

    fn write_ewkb<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<(), Error> {
        // use LE
        try!(w.write_u8(0x01));
        let type_id = Self::type_id();
        try!(w.write_u32::<LittleEndian>(type_id));
        Self::PointType::opt_srid().map(|srid| w.write_i32::<LittleEndian>(srid));
        try!(self.write_ewkb_body(w));
        Ok(())
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<(), Error> {
        try!(w.write_u32::<LittleEndian>(self.geometries.len() as u32));
        for item in self.geometries.iter() {
            let ret = match item {
                // FIXME: fake type
                &GeometryType::Point(ref obj)              => obj.write_ewkb(&Type::Point, w),
                &GeometryType::LineString(ref obj)         => obj.write_ewkb(&Type::Point, w),
                &GeometryType::Polygon(ref obj)            => obj.write_ewkb(&Type::Point, w),
                &GeometryType::MultiPoint(ref obj)         => obj.write_ewkb(&Type::Point, w),
                &GeometryType::MultiLineString(ref obj)    => obj.write_ewkb(&Type::Point, w),
                &GeometryType::MultiPolygon(ref obj)       => obj.write_ewkb(&Type::Point, w),
                &GeometryType::GeometryCollection(ref obj) => obj.write_ewkb(&Type::Point, w),
            };
            try!(ret);
        }
        Ok(())
    }
    fn read_ewkb<R: Read>(raw: &mut R) -> Result<Self, Error> {
        let byte_order = try!(raw.read_i8());
        let is_be = byte_order == 0i8;

        let type_id = try!(read_u32(raw, is_be));
        if type_id != Self::type_id() {
            return Err(Error::Read("type id not match".into()))
        }

        match Self::PointType::opt_srid() {
            Some(srid) => {
                if try!(read_i32(raw, is_be)) != srid {
                    return Err(Error::Read("srid not match".into()))
                }
            },
            _ => ()
        }

        Self::read_ewkb_body(raw, is_be)
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> Result<Self, Error> {
        let mut ret = GeometryCollection::new();
        let size = try!(read_u32(raw, is_be)) as usize;
        for _ in 0..size {
            let is_be = try!(raw.read_i8()) == 0i8;

            let type_id = try!(read_u32(raw, is_be));
            if type_id & 0xffff_ff00 != Self::type_id() & 0xffff_ff00 {
                return Err(Error::Read("type id not match".into()))
            }

            match Self::PointType::opt_srid() {
                Some(srid) => {
                    if try!(read_i32(raw, is_be)) != srid {
                        return Err(Error::Read("srid not match".into()))
                    }
                },
                _ => ()
            }
            match type_id & 0xff {
                0x01 => ret.geometries.push(GeometryType::Point(P::read_ewkb_body(raw, is_be).unwrap())),
                0x02 => ret.geometries.push(GeometryType::LineString(LineString::read_ewkb_body(raw, is_be).unwrap())),
                0x03 => ret.geometries.push(GeometryType::Polygon(Polygon::read_ewkb_body(raw, is_be).unwrap())),
                0x04 => ret.geometries.push(GeometryType::MultiPoint(MultiPoint::read_ewkb_body(raw, is_be).unwrap())),
                0x05 => ret.geometries.push(GeometryType::MultiLineString(MultiLineString::read_ewkb_body(raw, is_be).unwrap())),
                0x06 => ret.geometries.push(GeometryType::MultiPolygon(MultiPolygon::read_ewkb_body(raw, is_be).unwrap())),
                0x07 => ret.geometries.push(GeometryType::GeometryCollection(GeometryCollection::read_ewkb_body(raw, is_be).unwrap())),
                _    => panic!("....")
            }
        }
        Ok(ret)
    }

}

impl<P: ToPoint + fmt::Debug> ToSql for GeometryCollection<P> {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
        try!(self.write_ewkb(ty, w));
        Ok(IsNull::No)
    }

}
impl<P: ToPoint + fmt::Debug> FromSql for GeometryCollection<P> {
    accepts_geography!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<GeometryCollection<P>> {
        <Self as Geometry>::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to Geometry", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

#[test]
fn test_point() {
    // out of China
    let p1 = Point::<WGS84>::new(10.2, 20.3);
    assert_eq!(p1.to_gcj02(), (10.2, 20.3));
    let p2 = Point::<NoSRID>::new(10.2, 20.3);
    assert_eq!(format!("{}", p1), "POINT(10.2 20.3)");
    assert_eq!(format!("{}", p2), "POINT(10.2 20.3)");
    assert_eq!(format!("{:?}", p1), "SRID=4326;POINT(10.2 20.3)");
    assert_eq!(format!("{:?}", p2), "POINT(10.2 20.3)");
}
