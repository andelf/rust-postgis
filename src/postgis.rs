//
// Copyright (c) ShuYu Wang <andelf@gmail.com>, Feather Workshop and Pirmin Kalberer. All rights reserved.
//

use types::{Point, LineString, Polygon};
use ewkb::{self, EwkbRead, EwkbWrite, AsEwkbPoint, AsEwkbLineString, AsEwkbPolygon, AsEwkbMultiPoint, AsEwkbMultiLineString, AsEwkbMultiPolygon};
use twkb::{self, TwkbGeom};
use std;
use std::io::prelude::*;
use postgres;
use postgres::types::{Type, IsNull, ToSql, FromSql, SessionInfo};
use error::Error;
use std::convert::From;


impl From<Error> for postgres::error::Error {
    fn from(e: Error) -> postgres::error::Error {
        postgres::error::Error::Conversion(Box::new(e))
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


impl<'a> ToSql for ewkb::EwkbPoint<'a> {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
        try!(self.write_ewkb(out));
        Ok(IsNull::No)
    }
}

macro_rules! impl_sql_for_point_type {
    ($ptype:ident) => (
        impl FromSql for ewkb::$ptype {
            accepts_geography!();
            fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<ewkb::$ptype> {
                ewkb::$ptype::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to POINT", ty).into(); postgres::error::Error::Conversion(err)})
            }
        }

        impl ToSql for ewkb::$ptype {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.as_ewkb().write_ewkb(out));
                Ok(IsNull::No)
            }
        }
    )
}

impl_sql_for_point_type!(Point);
impl_sql_for_point_type!(PointZ);
impl_sql_for_point_type!(PointM);
impl_sql_for_point_type!(PointZM);


macro_rules! impl_sql_for_geom_type {
    ($geotype:ident) => (
        impl<'a, T> FromSql for ewkb::$geotype<T>
            where T: 'a + Point + EwkbRead
        {
            accepts_geography!();
            fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<ewkb::$geotype<T>> {
                ewkb::$geotype::<T>::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to LINESTRING", ty).into(); postgres::error::Error::Conversion(err)})
            }
        }

        impl<'a, T> ToSql for ewkb::$geotype<T>
            where T: 'a + Point + EwkbRead
        {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.as_ewkb().write_ewkb(out));
                Ok(IsNull::No)
            }
        }
    )
}

impl_sql_for_geom_type!(LineStringT);
impl_sql_for_geom_type!(PolygonT);
impl_sql_for_geom_type!(MultiPointT);
impl_sql_for_geom_type!(MultiLineStringT);
impl_sql_for_geom_type!(MultiPolygonT);


macro_rules! impl_sql_for_ewkb_type {
    ($ewkbtype:ident contains points) => (
        impl<'a, T, I> ToSql for ewkb::$ewkbtype<'a, T, I>
            where T: 'a + Point,
                  I: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.write_ewkb(out));
                Ok(IsNull::No)
            }
        }
    );
    ($ewkbtype:ident contains $itemtypetrait:ident) => (
        impl<'a, P, I, T, J> ToSql for ewkb::$ewkbtype<'a, P, I, T, J>
            where P: 'a + Point,
                  I: 'a + Iterator<Item=&'a P> + ExactSizeIterator<Item=&'a P>,
                  T: 'a + $itemtypetrait<'a, ItemType=P, Iter=I>,
                  J: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.write_ewkb(out));
                Ok(IsNull::No)
            }
        }
    );
    (multipoly $ewkbtype:ident contains $itemtypetrait:ident) => (
        impl<'a, P, I, L, K, T, J> ToSql for ewkb::$ewkbtype<'a, P, I, L, K, T, J>
            where P: 'a + Point,
                  I: 'a + Iterator<Item=&'a P> + ExactSizeIterator<Item=&'a P>,
                  L: 'a + LineString<'a, ItemType=P, Iter=I>,
                  K: 'a + Iterator<Item=&'a L> + ExactSizeIterator<Item=&'a L>,
                  T: 'a + $itemtypetrait<'a, ItemType=L, Iter=K>,
                  J: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
        {
            to_sql_checked!();
            accepts_geography!();
            fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
                try!(self.write_ewkb(out));
                Ok(IsNull::No)
            }
        }
    )
}

impl_sql_for_ewkb_type!(EwkbLineString contains points);
impl_sql_for_ewkb_type!(EwkbPolygon contains LineString);
impl_sql_for_ewkb_type!(EwkbMultiPoint contains points);
impl_sql_for_ewkb_type!(EwkbMultiLineString contains LineString);
impl_sql_for_ewkb_type!(multipoly EwkbMultiPolygon contains Polygon);


// --- TWKB ---

macro_rules! accepts_bytea {
    () => (
        fn accepts(ty: &Type) -> bool {
            match ty {
                &Type::Bytea  => true,
                _ => false
            }
        }
    )
}


impl FromSql for twkb::Point {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<twkb::Point> {
        twkb::Point::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to Point", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl FromSql for twkb::LineString {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<twkb::LineString> {
        twkb::LineString::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to LineString", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl FromSql for twkb::Polygon {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<twkb::Polygon> {
        twkb::Polygon::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to Polygon", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl FromSql for twkb::MultiPoint {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<twkb::MultiPoint> {
        twkb::MultiPoint::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to MultiPoint", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl FromSql for twkb::MultiLineString {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<twkb::MultiLineString> {
        twkb::MultiLineString::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to MultiLineString", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl FromSql for twkb::MultiPolygon {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<twkb::MultiPolygon> {
        twkb::MultiPolygon::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to MultiPolygon", ty).into(); postgres::error::Error::Conversion(err)})
    }
}


#[cfg(test)]
mod tests {
    use postgres;
    use postgres::Connection;
    use std::env;
    use std::error::Error;
    use types as postgis;
    use ewkb::{self, AsEwkbPoint, AsEwkbLineString};
    use twkb;

    macro_rules! or_panic {
        ($e:expr) => (
            match $e {
                Ok(ok) => ok,
                Err(err) => panic!("{:#?}", err)
            }
        )
    }

    fn connect() -> Connection {
        match env::var("DBCONN") {
            Result::Ok(val) => Connection::connect(&val as &str, postgres::SslMode::None),
            Result::Err(err) => { panic!("{:#?}", err) }
        }.unwrap()
    }

    #[test]
    #[ignore]
    fn test_insert_point() {
        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(Point))", &[]));

        // 'POINT (10 -20)'
        let point = ewkb::Point { x: 10.0, y: -20.0, srid: None };
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('POINT(10 -20)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        // With SRID
        let point = ewkb::Point { x: 10.0, y: -20.0, srid: Some(4326) };
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;POINT(10 -20)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(Point, 4326))", &[]));

        let point = ewkb::Point { x: 10.0, y: -20.0, srid: Some(4326) };
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;POINT(10 -20)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        // Missing SRID
        let point = ewkb::Point { x: 10.0, y: -20.0, srid: None };
        let result = conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]);
        assert_eq!(result.err().unwrap().description(), "Error reported by Postgres");
    }

    #[test]
    #[ignore]
    fn test_insert_line() {
        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineString))", &[]));

        let p = |x, y| ewkb::Point { x: x, y: y, srid: None };
        // 'LINESTRING (10 -20, -0 -0.5)'
        let line = ewkb::LineString {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&line]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('LINESTRING(10 -20, -0 -0.5)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineString, 4326))", &[]));

        // 'SRID=4326;LINESTRING (10 -20, -0 -0.5)'
        let line = ewkb::LineString {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&line]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;LINESTRING(10 -20, -0 -0.5)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineStringZ, 4326))", &[]));

        let p = |x, y, z| ewkb::PointZ { x: x, y: y, z: z, srid: Some(4326) };
        // 'SRID=4326;LINESTRING (10 -20 100, -0 -0.5 101)'
        let line = ewkb::LineStringZ {srid: Some(4326), points: vec![p(10.0, -20.0, 100.0), p(0., -0.5, 101.0)]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&line]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;LINESTRING (10 -20 100, -0 -0.5 101)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));
    }

    #[test]
    #[ignore]
    fn test_insert_polygon() {
        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(Polygon))", &[]));
        let p = |x, y| ewkb::Point { x: x, y: y, srid: Some(4326) };
        // SELECT 'SRID=4326;POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'::geometry
        let line = ewkb::LineString {srid: Some(4326), points: vec![p(0., 0.), p(2., 0.), p(2., 2.), p(0., 2.), p(0., 0.)]};
        let poly = ewkb::Polygon {srid: Some(4326), rings: vec![line]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&poly]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
    }

    #[test]
    #[ignore]
    fn test_insert_multipoint() {
        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(MultiPointZ))", &[]));
        let p = |x, y, z| ewkb::PointZ { x: x, y: y, z: z, srid: Some(4326) };
        // SELECT 'SRID=4326;MULTIPOINT ((10 -20 100), (0 -0.5 101))'::geometry
        let points = ewkb::MultiPointZ {srid: Some(4326), points: vec![p(10.0, -20.0, 100.0), p(0., -0.5, 101.0)]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&points]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;MULTIPOINT ((10 -20 100), (0 -0.5 101))') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
    }

    #[test]
    #[ignore]
    fn test_insert_multiline() {
        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(MultiLineString))", &[]));
        let p = |x, y| ewkb::Point { x: x, y: y, srid: Some(4326) };
        // SELECT 'SRID=4326;MULTILINESTRING ((10 -20, 0 -0.5), (0 0, 2 0))'::geometry
        let line1 = ewkb::LineString {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
        let line2 = ewkb::LineString {srid: Some(4326), points: vec![p(0., 0.), p(2., 0.)]};
        let multiline = ewkb::MultiLineString {srid: Some(4326),lines: vec![line1, line2]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&multiline]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;MULTILINESTRING ((10 -20, 0 -0.5), (0 0, 2 0))') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
    }

    #[test]
    #[ignore]
    fn test_insert_multipoylgon() {
        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(MultiPolygon))", &[]));
        let p = |x, y| ewkb::Point { x: x, y: y, srid: Some(4326) };
        // SELECT 'SRID=4326;MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((10 10, -2 10, -2 -2, 10 -2, 10 10)))'::geometry
        let line = ewkb::LineString {srid: Some(4326), points: vec![p(0., 0.), p(2., 0.), p(2., 2.), p(0., 2.), p(0., 0.)]};
        let poly1 = ewkb::Polygon {srid: Some(4326), rings: vec![line]};
        let line = ewkb::LineString {srid: Some(4326), points: vec![p(10., 10.), p(-2., 10.), p(-2., -2.), p(10., -2.), p(10., 10.)]};
        let poly2 = ewkb::Polygon {srid: Some(4326), rings: vec![line]};
        let multipoly = ewkb::MultiPolygon {srid: Some(4326), polygons: vec![poly1, poly2]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&multipoly]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((10 10, -2 10, -2 -2, 10 -2, 10 10)))') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
    }

    #[test]
    #[ignore]
    fn test_select_point() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT ('POINT(10 -20)')::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(point, ewkb::Point { x: 10.0, y: -20.0, srid: None });

        let result = or_panic!(conn.query("SELECT 'SRID=4326;POINT(10 -20)'::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(point, ewkb::Point { x: 10.0, y: -20.0, srid: Some(4326) });

        let result = or_panic!(conn.query("SELECT 'SRID=4326;POINT(10 -20 99)'::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, ewkb::PointZ>(0)).last().unwrap();
        assert_eq!(point, ewkb::PointZ { x: 10.0, y: -20.0, z: 99.0, srid: Some(4326) });

        let result = or_panic!(conn.query("SELECT 'POINT EMPTY'::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Point { x: NaN, y: NaN, srid: None }");

        let result = or_panic!(conn.query("SELECT NULL::geometry(Point)", &[]));
        let point = result.iter().map(|r| r.get_opt::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Some(Err(Conversion(WasNull)))");
    }

    #[test]
    #[ignore]
    fn test_select_line() {
        let conn = connect();
        let p = |x, y| ewkb::Point { x: x, y: y, srid: None };
        let result = or_panic!(conn.query("SELECT ('LINESTRING (10 -20, -0 -0.5)')::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, ewkb::LineString>(0)).last().unwrap();
        assert_eq!(line, ewkb::LineString {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]});

        let p = |x, y| ewkb::Point { x: x, y: y, srid: Some(4326) };
        let result = or_panic!(conn.query("SELECT ('SRID=4326;LINESTRING (10 -20, -0 -0.5)')::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, ewkb::LineString>(0)).last().unwrap();
        assert_eq!(line, ewkb::LineString {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]});

        let result = or_panic!(conn.query("SELECT 'LINESTRING EMPTY'::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, ewkb::LineString>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", line), "LineStringT { points: [], srid: None }");
    }

    #[test]
    #[ignore]
    fn test_select_polygon() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT 'SRID=4326;POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'::geometry", &[]));
        let poly = result.iter().map(|r| r.get::<_, ewkb::Polygon>(0)).last().unwrap();
        assert_eq!(format!("{:?}", poly), "PolygonT { rings: [LineStringT { points: [Point { x: 0, y: 0, srid: Some(4326) }, Point { x: 2, y: 0, srid: Some(4326) }, Point { x: 2, y: 2, srid: Some(4326) }, Point { x: 0, y: 2, srid: Some(4326) }, Point { x: 0, y: 0, srid: Some(4326) }], srid: Some(4326) }], srid: Some(4326) }");
    }

    #[test]
    #[ignore]
    fn test_select_multipoint() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT 'SRID=4326;MULTIPOINT ((10 -20 100), (0 -0.5 101))'::geometry", &[]));
        let points = result.iter().map(|r| r.get::<_, ewkb::MultiPointZ>(0)).last().unwrap();
        assert_eq!(format!("{:?}", points), "MultiPointT { points: [PointZ { x: 10, y: -20, z: 100, srid: None }, PointZ { x: 0, y: -0.5, z: 101, srid: None }], srid: Some(4326) }");
    }

    #[test]
    #[ignore]
    fn test_select_multiline() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT 'SRID=4326;MULTILINESTRING ((10 -20, 0 -0.5), (0 0, 2 0))'::geometry", &[]));
        let multiline = result.iter().map(|r| r.get::<_, ewkb::MultiLineString>(0)).last().unwrap();
        assert_eq!(format!("{:?}", multiline), "MultiLineStringT { lines: [LineStringT { points: [Point { x: 10, y: -20, srid: None }, Point { x: 0, y: -0.5, srid: None }], srid: None }, LineStringT { points: [Point { x: 0, y: 0, srid: None }, Point { x: 2, y: 0, srid: None }], srid: None }], srid: Some(4326) }");
    }

    #[test]
    #[ignore]
    fn test_select_multipolygon() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT 'SRID=4326;MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((10 10, -2 10, -2 -2, 10 -2, 10 10)))'::geometry", &[]));
        let multipoly = result.iter().map(|r| r.get::<_, ewkb::MultiPolygon>(0)).last().unwrap();
        assert_eq!(format!("{:?}", multipoly), "MultiPolygonT { polygons: [PolygonT { rings: [LineStringT { points: [Point { x: 0, y: 0, srid: None }, Point { x: 2, y: 0, srid: None }, Point { x: 2, y: 2, srid: None }, Point { x: 0, y: 2, srid: None }, Point { x: 0, y: 0, srid: None }], srid: None }], srid: None }, PolygonT { rings: [LineStringT { points: [Point { x: 10, y: 10, srid: None }, Point { x: -2, y: 10, srid: None }, Point { x: -2, y: -2, srid: None }, Point { x: 10, y: -2, srid: None }, Point { x: 10, y: 10, srid: None }], srid: None }], srid: None }], srid: Some(4326) }");
    }

    #[test]
    #[ignore]
    fn test_twkb() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT ST_AsTWKB('POINT(10 -20)'::geometry)", &[]));
        let point = result.iter().map(|r| r.get::<_, twkb::Point>(0)).last().unwrap();
        assert_eq!(point, twkb::Point {x: 10.0, y: -20.0});

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('SRID=4326;POINT(10 -20)'::geometry)", &[]));
        let point = result.iter().map(|r| r.get::<_, twkb::Point>(0)).last().unwrap();
        assert_eq!(point, twkb::Point {x: 10.0, y: -20.0});

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('POINT EMPTY'::geometry)", &[]));
        let point = result.iter().map(|r| r.get::<_, twkb::Point>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Point { x: NaN, y: NaN }");
        let point = &point as &postgis::Point;
        assert!(point.x().is_nan());

        let result = or_panic!(conn.query("SELECT ST_AsTWKB(NULL::geometry(Point))", &[]));
        let point = result.iter().map(|r| r.get_opt::<_, twkb::Point>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Some(Err(Conversion(WasNull)))");

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry, 1)", &[]));
        let line = result.iter().map(|r| r.get::<_, twkb::LineString>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", line), "LineString { points: [Point { x: 10, y: -20 }, Point { x: 0, y: -0.5 }] }");
    }

    #[test]
    #[ignore]
    fn test_twkb_insert() {
        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(Point))", &[]));

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('POINT(10 -20)'::geometry)", &[]));
        let point = result.iter().map(|r| r.get::<_, twkb::Point>(0)).last().unwrap();

        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point.as_ewkb()]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('POINT(10 -20)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineString))", &[]));

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry, 1)", &[]));
        let line = result.iter().map(|r| r.get::<_, twkb::LineString>(0)).last().unwrap();

        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&line.as_ewkb()]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('LINESTRING (10 -20, -0 -0.5)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));
    }

    #[test]
    #[ignore]
    #[allow(unused_imports,unused_variables)]
    fn test_examples() {
        use postgres::{Connection, SslMode};
        //use postgis::ewkb;
        //use postgis::LineString;

        fn main() {
            //
            use ewkb;
            use types::LineString;
            use twkb;
            let conn = connect();
            or_panic!(conn.execute("CREATE TEMPORARY TABLE busline (route geometry(LineString))", &[]));
            or_panic!(conn.execute("CREATE TEMPORARY TABLE stops (stop geometry(Point))", &[]));
            or_panic!(conn.execute("INSERT INTO busline (route) VALUES ('LINESTRING(10 -20, -0 -0.5)'::geometry)", &[]));
            //

            // conn ....
            for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
                let route: ewkb::LineString = row.get("route");
                let last_stop = route.points().last().unwrap();
                let _ = conn.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop]);
            }

            for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
                let route = row.get_opt::<_, Option<ewkb::LineString>>("route");
                match route.unwrap() {
                    Ok(Some(geom)) => { println!("{:?}", geom) }
                    Ok(None) => { /* Handle NULL value */ }
                    Err(err) => { println!("Error: {}", err) }
                }
            }

        //use postgis::twkb;

            for row in &conn.query("SELECT ST_AsTWKB(route) FROM busline", &[]).unwrap() {
                let route: twkb::LineString = row.get(0);
                let last_stop = route.points().last().unwrap();
                let _ = conn.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop.as_ewkb()]);
            }
        }

        main();
    }
}
