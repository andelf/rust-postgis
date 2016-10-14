use types::{Point, AsEwkbPoint, AsEwkbLineString, EwkbPoint, EwkbLineString};
use ewkb::{self, EwkbRead, EwkbWrite};
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


impl FromSql for ewkb::Point {
    accepts_geography!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<ewkb::Point> {
        ewkb::Point::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to POINT", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl<'a> ToSql for EwkbPoint<'a> {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
        try!(self.write_ewkb(out));
        Ok(IsNull::No)
    }
}

impl ToSql for ewkb::Point {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
        try!(self.as_ewkb().write_ewkb(out));
        Ok(IsNull::No)
    }
}

impl FromSql for ewkb::LineString<ewkb::Point> {
    accepts_geography!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<ewkb::LineString<ewkb::Point>> {
        ewkb::LineString::<ewkb::Point>::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to LINESTRING", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl<'a, T, I> ToSql for EwkbLineString<'a, T, I>
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

impl ToSql for ewkb::LineString<ewkb::Point> {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
        try!(self.as_ewkb().write_ewkb(out));
        Ok(IsNull::No)
    }
}

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
        twkb::Point::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to POINT", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl FromSql for twkb::LineString {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<twkb::LineString> {
        twkb::LineString::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to POINT", ty).into(); postgres::error::Error::Conversion(err)})
    }
}


#[cfg(test)]
mod tests {
    use postgres;
    use postgres::Connection;
    use std::env;
    use std::error::Error;
    use types::{Point, AsEwkbPoint, AsEwkbLineString};
    use types as postgis;
    use ewkb;
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
    fn test_insert() {
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

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineString))", &[]));

        let p = |x, y| ewkb::Point { x: x, y: y, srid: None };
        // 'LINESTRING (10 -20, -0 -0.5)'
        let line = ewkb::LineString::<ewkb::Point> {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&line]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('LINESTRING(10 -20, -0 -0.5)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineString, 4326))", &[]));

        // 'SRID=4326;LINESTRING (10 -20, -0 -0.5)'
        let line = ewkb::LineString::<ewkb::Point> {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&line]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;LINESTRING(10 -20, -0 -0.5)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));
    }

    #[test]
    #[ignore]
    fn test_select() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT ('POINT(10 -20)')::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(point, ewkb::Point { x: 10.0, y: -20.0, srid: None });
        assert_eq!(point.srid, None);

        let result = or_panic!(conn.query("SELECT 'SRID=4326;POINT(10 -20)'::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(point, ewkb::Point { x: 10.0, y: -20.0, srid: Some(4326) });
        assert_eq!(point.srid, Some(4326));

        let result = or_panic!(conn.query("SELECT 'POINT EMPTY'::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Point { x: NaN, y: NaN, srid: None }");
        assert!(point.x().is_nan());

        let result = or_panic!(conn.query("SELECT NULL::geometry(Point)", &[]));
        let point = result.iter().map(|r| r.get_opt::<_, ewkb::Point>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Some(Err(Conversion(WasNull)))");

        let p = |x, y| ewkb::Point { x: x, y: y, srid: None };
        let result = or_panic!(conn.query("SELECT ('LINESTRING (10 -20, -0 -0.5)')::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, ewkb::LineString<ewkb::Point>>(0)).last().unwrap();
        assert_eq!(line, ewkb::LineString::<ewkb::Point> {srid: None, points: vec![p(10.0, -20.0), p(0., -0.5)]});
        assert_eq!(line.srid, None);

        let result = or_panic!(conn.query("SELECT ('SRID=4326;LINESTRING (10 -20, -0 -0.5)')::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, ewkb::LineString<ewkb::Point>>(0)).last().unwrap();
        assert_eq!(line, ewkb::LineString::<ewkb::Point> {srid: Some(4326), points: vec![p(10.0, -20.0), p(0., -0.5)]});
        assert_eq!(line.srid, Some(4326));

        let result = or_panic!(conn.query("SELECT 'LINESTRING EMPTY'::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, ewkb::LineString<ewkb::Point>>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", line), "LineString { points: [], srid: None }");
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
        //use postgis::Points;

        fn main() {
            //
            use ewkb;
            use types::Points;
            use twkb;
            let conn = connect();
            or_panic!(conn.execute("CREATE TEMPORARY TABLE busline (route geometry(LineString))", &[]));
            or_panic!(conn.execute("CREATE TEMPORARY TABLE stops (stop geometry(Point))", &[]));
            or_panic!(conn.execute("INSERT INTO busline (route) VALUES ('LINESTRING(10 -20, -0 -0.5)'::geometry)", &[]));
            //

            // conn ....
            for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
                let route: ewkb::LineString<ewkb::Point> = row.get("route");
                let last_stop = route.points().last().unwrap();
                let _ = conn.execute("INSERT INTO stops (stop) VALUES ($1)", &[&last_stop]);
            }

            for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
                let route = row.get_opt::<_, Option<ewkb::LineString<ewkb::Point>>>("route");
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
