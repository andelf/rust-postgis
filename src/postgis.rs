use std::io::prelude::*;
use std::fmt;
use std::mem;
use std::marker::PhantomData;
use postgres::{ToSql, FromSql};
use postgres::types;
use postgres::types::{Type, IsNull};
use postgres::{Result, Error};
use byteorder;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};


trait Geometry: fmt::Debug + Sized {
    type PointType: ToPoint;
    #[inline(always)]
    fn type_id() -> u32;

    fn read_ewkb<R: Read>(raw: &mut R) -> byteorder::Result<Self> {
        let byte_order = try!(raw.read_i8());
        let is_be = byte_order == 0i8;

        let type_id = try!(read_u32(raw, is_be));
        if type_id != Self::type_id() {
            return Err(byteorder::Error::UnexpectedEOF);
        }

        match Self::PointType::opt_srid() {
            Some(srid) => {
                if try!(read_i32(raw, is_be)) != srid {
                    return Err(byteorder::Error::UnexpectedEOF);
                }
            },
            _ => ()
        }
        Self::read_ewkb_body(raw, is_be)
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> byteorder::Result<Self>;

    fn write_ewkb<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<IsNull> {
        // use LE
        try!(w.write_u8(0x01));
        let mut type_id = Self::type_id();
        w.write_u32::<LittleEndian>(type_id);
        Self::PointType::opt_srid().map(|srid| w.write_i32::<LittleEndian>(srid));
        self.write_ewkb_body(w);
        Ok(IsNull::No)
    }
    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<()>;

}

fn read_u32<R: Read>(raw: &mut R, is_be: bool) -> byteorder::Result<u32> {
    if is_be { raw.read_u32::<BigEndian>() }
    else { raw.read_u32::<LittleEndian>() }
}

fn read_i32<R: Read>(raw: &mut R, is_be: bool) -> byteorder::Result<i32> {
    if is_be { raw.read_i32::<BigEndian>() }
    else { raw.read_i32::<LittleEndian>() }
}


fn read_f64<R: Read>(raw: &mut R, is_be: bool) -> byteorder::Result<f64> {
    if is_be { raw.read_f64::<BigEndian>() }
    else { raw.read_f64::<LittleEndian>() }
}



pub trait SRID {
    #[inline(always)]
    fn as_srid() -> Option<i32>;
}

#[derive(Debug)]
#[allow(missing_copy_implementations)] pub enum WGS84 {}
#[derive(Debug)]
#[allow(missing_copy_implementations)] pub enum NAD27 {}
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


trait ToPoint: Sized {
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

    fn read_ewkb<R: Read>(raw: &mut R) -> byteorder::Result<Self> {
        let byte_order = try!(raw.read_i8());
        let is_be = byte_order == 0i8;

        let mut type_ = try!(read_u32(raw, is_be));
        if type_ != Self::type_id() {
            return Err(byteorder::Error::UnexpectedEOF);
        }

        if Self::opt_srid().is_some() {
            if Self::opt_srid() != Some(try!(read_i32(raw, is_be))) {
                println!("error: srid not match");
                // FIXME
                return Err(byteorder::Error::UnexpectedEOF);
            }
        }

        Self::read_ewkb_body(raw, is_be)
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> byteorder::Result<Self> {
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

    fn write_ewkb<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<IsNull> {
        // use LE
        try!(w.write_u8(0x01));
        w.write_u32::<LittleEndian>(Self::type_id());
        Self::opt_srid().map(|srid| w.write_i32::<LittleEndian>(srid));
        self.write_ewkb_body(w);
        Ok(IsNull::No)
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<()> {
        // lol
        let x = unsafe { *mem::transmute::<_, *const f64>(self) };
        let y = unsafe { *mem::transmute::<_, *const f64>(self).offset(1) };
        w.write_f64::<LittleEndian>(x);
        w.write_f64::<LittleEndian>(y);
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

#[derive(Copy, Clone)]
pub struct Point<S: SRID = WGS84> {
    pub x: f64,
    pub y: f64,
    phantom: PhantomData<S>
}

impl Point {
    pub fn new(x: f64, y: f64) -> Point {
        Point { x: x, y: y, phantom: PhantomData }
    }
}

#[derive(Copy, Clone)]
pub struct PointZ<S: SRID = WGS84> {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    phantom: PhantomData<S>
}

impl PointZ {
    pub fn new(x: f64, y: f64, z: f64) -> PointZ {
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

impl PointM {
    pub fn new(x: f64, y: f64, m: f64) -> PointM {
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

impl PointZM {
    pub fn new(x: f64, y: f64, z: f64, m: f64) -> PointZM {
        PointZM { x: x, y: y, z: z, m: z, phantom: PhantomData }
    }
}

impl<S: SRID> ToPoint for Point<S> {
    type SRIDType = S;
    fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Self {
        Point { x: x, y: y,  phantom: PhantomData }
    }
}

impl<S: SRID> ToPoint for PointZ<S> {
    type SRIDType = S;
    fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Self {
        PointZ { x: x, y: y, z: z.unwrap(), phantom: PhantomData }
    }
    fn opt_z(&self) -> Option<f64> {
        Some(self.z)
    }
    fn has_z() -> bool { true }
}
impl<S: SRID> ToPoint for PointM<S> {
    type SRIDType = S;
    fn new_from_opt_vals(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Self {
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
                &Type::Other(ref t) if t.name() == "geography" && t.oid() == 25304 => true,
                _ => false
            }
        }
    )
}


macro_rules! impl_traits_for_point {
    ($ptype:ident) => (
        impl FromSql for $ptype {
            accepts_geography!();
            fn from_sql<R: Read>(ty: &Type, raw: &mut R) -> Result<$ptype> {
                <$ptype as ToPoint>::read_ewkb(raw).map_err(|_| Error::WrongType(ty.clone()))
            }
        }

        impl ToSql for $ptype {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, ty: &Type, out: &mut W) -> Result<IsNull> {
                self.write_ewkb(ty, out)
            }
        }

        impl fmt::Display for $ptype {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                try!(write!(f, "{}", self.describ()));
                Ok(())
            }
        }
        impl fmt::Debug for $ptype {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match <$ptype as ToPoint>::opt_srid() {
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

#[derive(Debug)]
pub struct LineString<P> {
    pub points: Vec<P>,
}

impl<P: ToPoint> LineString<P> {
    pub fn new() -> LineString<P> {
        LineString { points: Vec::new() }
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


impl<P: ToPoint + fmt::Debug> Geometry for LineString<P> {
    type PointType = P;
    fn type_id() -> u32 {
        let type_id = P::type_id();
        (type_id & 0xffff_ff00) | 0x0000_0002
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> byteorder::Result<Self> {
        let mut ret = LineString::new();
        let size = try!(read_u32(raw, is_be)) as usize;
        for _ in 0..size {
            ret.points.push(P::read_ewkb_body(raw, is_be).unwrap())
        }
        Ok(ret)
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<()> {
        try!(w.write_u32::<LittleEndian>(self.points.len() as u32));
        for point in self.points.iter() {
            point.write_ewkb_body(w);
        }
        Ok(())
    }

}

impl<P: ToPoint + fmt::Debug> ToSql for LineString<P> {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W) -> Result<IsNull> {
        self.write_ewkb(ty, w)
    }

}
impl<P: ToPoint + fmt::Debug> FromSql for LineString<P> {
    accepts_geography!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R) -> Result<LineString<P>> {
        <Self as Geometry>::read_ewkb(raw).map_err(|_| Error::WrongType(ty.clone()))
    }
}

#[derive(Debug)]
pub struct Polygon<P> {
    pub rings: Vec<LineString<P>>
}


impl<P: ToPoint> Polygon<P> {
    pub fn new() -> Polygon<P> {
        Polygon { rings: Vec::new() }
    }
}

impl<P: ToPoint + fmt::Debug> Geometry for Polygon<P> {
    type PointType = P;
    fn type_id() -> u32 {
        let type_id = P::type_id();
        (type_id & 0xffff_ff00) | 0x0000_0003
    }

    fn read_ewkb_body<R: Read>(raw: &mut R, is_be: bool) -> byteorder::Result<Self> {
        let mut ret = Polygon::new();
        let size = try!(read_u32(raw, is_be)) as usize;
        for _ in 0..size {
            ret.rings.push(LineString::<P>::read_ewkb_body(raw, is_be).unwrap())
        }
        Ok(ret)
    }

    fn write_ewkb_body<W: Write+?Sized>(&self, w: &mut W) -> Result<()> {
        try!(w.write_u32::<LittleEndian>(self.rings.len() as u32));
        for ring in self.rings.iter() {
            ring.write_ewkb_body(w);
        }
        Ok(())
    }

}

impl<P: ToPoint + fmt::Debug> ToSql for Polygon<P> {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<IsNull> {
        // use LE
        try!(w.write_u8(0x01));
        let mut type_id = P::type_id();
        // clean lower bit
        type_id &= 0xffff_ff00;
        type_id |= 0x0000_0002;
        w.write_u32::<LittleEndian>(type_id);
        P::opt_srid().map(|srid| w.write_i32::<LittleEndian>(srid));
        for point in self.rings.iter() {
            point.write_ewkb_body(w);
        }
        Ok(IsNull::No)
    }

}
impl<P: ToPoint + fmt::Debug> FromSql for Polygon<P> {
    accepts_geography!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R) -> Result<Polygon<P>> {
        let mut ret = Polygon::new();
        let byte_order = try!(raw.read_i8());
        let is_be = byte_order == 0i8;

        let mut type_id = try!(read_u32(raw, is_be));
        println!("type id => {:X}", type_id);
        if type_id & 0xff != 0x02 || type_id & 0xff00_0000 != P::type_id() & 0xff00_0000 {
            return Err(Error::WrongType(ty.clone()));
        }

        match P::opt_srid() {
            Some(srid) => {
                if try!(read_i32(raw, is_be)) != srid {
                    return Err(Error::WrongType(ty.clone()));
                }
            },
            _ => ()
        }
        let size = try!(read_u32(raw, is_be)) as usize;
        println!("size => size {}", size);
        for _ in 0..size {
            println!("");
            ret.rings.push(LineString::<P>::read_ewkb_body(raw, is_be).unwrap())
        }
        Ok(ret)
    }
}
