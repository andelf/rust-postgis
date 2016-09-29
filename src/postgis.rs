use ewkb::{EwkbPoint, EwkbLineString, EwkbGeometryType};
use twkb::{TwkbGeom, TwkbPoint, TwkbLineString};
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


impl FromSql for EwkbPoint {
    accepts_geography!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<EwkbPoint> {
        EwkbPoint::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to POINT", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl ToSql for EwkbPoint {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
        try!(self.write_ewkb(out));
        Ok(IsNull::No)
    }
}

impl FromSql for EwkbLineString {
    accepts_geography!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<EwkbLineString> {
        EwkbLineString::read_ewkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to LINESTRING", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl ToSql for EwkbLineString {
    to_sql_checked!();
    accepts_geography!();
    fn to_sql<W: Write+?Sized>(&self, _: &Type, out: &mut W, _ctx: &SessionInfo) -> postgres::Result<IsNull> {
        try!(self.write_ewkb(out));
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


impl FromSql for TwkbPoint {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<TwkbPoint> {
        TwkbPoint::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to POINT", ty).into(); postgres::error::Error::Conversion(err)})
    }
}

impl FromSql for TwkbLineString {
    accepts_bytea!();
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _ctx: &SessionInfo) -> postgres::Result<TwkbLineString> {
        TwkbLineString::read_twkb(raw).map_err(|_| {let err: Box<std::error::Error + Sync + Send> = format!("cannot convert {} to POINT", ty).into(); postgres::error::Error::Conversion(err)})
    }
}


#[cfg(test)]
mod tests {
    use postgres;
    use postgres::Connection;
    use std::env;
    use std::error::Error;
    use geo::{self, Point, LineString};
    use types;
    use ewkb::{EwkbPoint, EwkbLineString};
    use twkb::{TwkbPoint, TwkbLineString};

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
        let point = EwkbPoint { srid: None, geom: Point::new(10.0, -20.0) };
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('POINT(10 -20)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        // With SRID
        let point = EwkbPoint { srid: Some(4326), geom: Point::new(10.0, -20.0) };
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;POINT(10 -20)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(Point, 4326))", &[]));

        let point = EwkbPoint { srid: Some(4326), geom: Point::new(10.0, -20.0) };
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('SRID=4326;POINT(10 -20)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        // Missing SRID
        let point = EwkbPoint { srid: None, geom: Point::new(10.0, -20.0) };
        let result = conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&point]);
        assert_eq!(result.err().unwrap().description(), "Error reported by Postgres");

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineString))", &[]));

        let p = |x, y| Point(geo::Coordinate { x: x, y: y });
        // 'LINESTRING (10 -20, -0 -0.5)'
        let line = EwkbLineString {srid: None, geom: LineString(vec![p(10.0, -20.0), p(0., -0.5)])};
        or_panic!(conn.execute("INSERT INTO geomtests (geom) VALUES ($1)", &[&line]));
        let result = or_panic!(conn.query("SELECT geom=ST_GeomFromEWKT('LINESTRING(10 -20, -0 -0.5)') FROM geomtests", &[]));
        assert!(result.iter().map(|r| r.get::<_, bool>(0)).last().unwrap());
        or_panic!(conn.execute("TRUNCATE geomtests", &[]));

        let conn = connect();
        or_panic!(conn.execute("CREATE TEMPORARY TABLE geomtests (geom geometry(LineString, 4326))", &[]));

        // 'SRID=4326;LINESTRING (10 -20, -0 -0.5)'
        let line = EwkbLineString {srid: Some(4326), geom: LineString(vec![p(10.0, -20.0), p(0., -0.5)])};
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
        let point = result.iter().map(|r| r.get::<_, EwkbPoint>(0)).last().unwrap();
        assert_eq!(point.geom, Point::new(10.0, -20.0));
        assert_eq!(point.srid, None);

        let result = or_panic!(conn.query("SELECT 'SRID=4326;POINT(10 -20)'::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, EwkbPoint>(0)).last().unwrap();
        assert_eq!(point.geom, Point::new(10.0, -20.0));
        assert_eq!(point.srid, Some(4326));

        let result = or_panic!(conn.query("SELECT 'POINT EMPTY'::geometry", &[]));
        let point = result.iter().map(|r| r.get::<_, EwkbPoint>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point.geom), "Point(Coordinate { x: NaN, y: NaN })");
        assert!(point.geom.x().is_nan());

        let result = or_panic!(conn.query("SELECT NULL::geometry(Point)", &[]));
        let point = result.iter().map(|r| r.get_opt::<_, EwkbPoint>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Some(Err(Conversion(WasNull)))");

        let p = |x, y| Point(geo::Coordinate { x: x, y: y });
        let result = or_panic!(conn.query("SELECT ('LINESTRING (10 -20, -0 -0.5)')::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, EwkbLineString>(0)).last().unwrap();
        assert_eq!(line.geom, LineString(vec![p(10.0, -20.0), p(0., -0.5)]));
        assert_eq!(line.srid, None);

        let result = or_panic!(conn.query("SELECT ('SRID=4326;LINESTRING (10 -20, -0 -0.5)')::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, EwkbLineString>(0)).last().unwrap();
        assert_eq!(line.geom, LineString(vec![p(10.0, -20.0), p(0., -0.5)]));
        assert_eq!(line.srid, Some(4326));

        let result = or_panic!(conn.query("SELECT 'LINESTRING EMPTY'::geometry", &[]));
        let line = result.iter().map(|r| r.get::<_, EwkbLineString>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", line.geom), "LineString([])");
    }

    #[test]
    #[ignore]
    fn test_twkb() {
        let conn = connect();
        let result = or_panic!(conn.query("SELECT ST_AsTWKB('POINT(10 -20)'::geometry)", &[]));
        let point = result.iter().map(|r| r.get::<_, TwkbPoint>(0)).last().unwrap();
        assert_eq!(point, TwkbPoint {x: 10.0, y: -20.0});

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('SRID=4326;POINT(10 -20)'::geometry)", &[]));
        let point = result.iter().map(|r| r.get::<_, TwkbPoint>(0)).last().unwrap();
        assert_eq!(point, TwkbPoint {x: 10.0, y: -20.0});

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('POINT EMPTY'::geometry)", &[]));
        let point = result.iter().map(|r| r.get::<_, TwkbPoint>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "TwkbPoint { x: NaN, y: NaN }");
        let point = &point as &types::Point;
        assert!(point.x().is_nan());

        let result = or_panic!(conn.query("SELECT ST_AsTWKB(NULL::geometry(Point))", &[]));
        let point = result.iter().map(|r| r.get_opt::<_, TwkbPoint>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", point), "Some(Err(Conversion(WasNull)))");

        let result = or_panic!(conn.query("SELECT ST_AsTWKB('LINESTRING (10 -20, -0 -0.5)'::geometry, 1)", &[]));
        let line = result.iter().map(|r| r.get::<_, TwkbLineString>(0)).last().unwrap();
        assert_eq!(&format!("{:?}", line), "TwkbLineString { points: [TwkbPoint { x: 10, y: -20 }, TwkbPoint { x: 0, y: -0.5 }] }");
    }

    #[test]
    #[ignore]
    #[allow(unused_imports,unused_variables)]
    fn test_examples() {
        use postgres::{Connection, SslMode};
        //use postgis::EwkbLineString;

        fn main() {
            //
            use ewkb::EwkbLineString;
            let conn = connect();
            or_panic!(conn.execute("CREATE TEMPORARY TABLE busline (route geometry(LineString))", &[]));
            or_panic!(conn.execute("INSERT INTO busline (route) VALUES ('LINESTRING(10 -20, -0 -0.5)'::geometry)", &[]));
            //

            // conn ....
            for row in &conn.query("SELECT * FROM busline", &[]).unwrap() {
                let route: EwkbLineString = row.get("route");
            }
        }

        main();
    }
}
