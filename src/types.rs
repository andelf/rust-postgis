//
// Copyright (c) Pirmin Kalberer. All rights reserved.
//

pub trait Point {
    fn x(&self) -> f64;
    fn y(&self) -> f64;
    fn opt_z(&self) -> Option<f64> {
        None
    }
    fn opt_m(&self) -> Option<f64> {
        None
    }
}

pub trait LineString<'a> {
    type ItemType: 'a + Point;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn points(&'a self) -> Self::Iter;
}

pub trait Polygon<'a> {
    type ItemType: 'a + LineString<'a>;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn rings(&'a self) -> Self::Iter;
}

pub trait MultiPoint<'a> {
    type ItemType: 'a + Point;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn points(&'a self) -> Self::Iter;
}

pub trait MultiLineString<'a> {
    type ItemType: 'a + LineString<'a>;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn lines(&'a self) -> Self::Iter;
}

pub trait MultiPolygon<'a> {
    type ItemType: 'a + Polygon<'a>;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn polygons(&'a self) -> Self::Iter;
}

pub trait Geometry<'a> {
    type Point: 'a + Point;
    type LineString: 'a + LineString<'a>;
    type Polygon: 'a + Polygon<'a>;
    type MultiPoint: 'a + MultiPoint<'a>;
    type MultiLineString: 'a + MultiLineString<'a>;
    type MultiPolygon: 'a + MultiPolygon<'a>;
    type GeometryCollection: 'a + GeometryCollection<'a>;
    fn as_type(&'a self) -> GeometryType<'a, Self::Point, Self::LineString, Self::Polygon, Self::MultiPoint, Self::MultiLineString, Self::MultiPolygon, Self::GeometryCollection>;
}

pub enum GeometryType<'a, P, L, Y, MP, ML, MY, GC>
    where P: 'a + Point,
          L: 'a + LineString<'a>,
          Y: 'a + Polygon<'a>,
          MP: 'a + MultiPoint<'a>,
          ML: 'a + MultiLineString<'a>,
          MY: 'a + MultiPolygon<'a>,
          GC: 'a + GeometryCollection<'a>
{
    Point(&'a P),
    LineString(&'a L),
    Polygon(&'a Y),
    MultiPoint(&'a MP),
    MultiLineString(&'a ML),
    MultiPolygon(&'a MY),
    GeometryCollection(&'a GC),
}

pub trait GeometryCollection<'a>
{
    type ItemType: 'a;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn geometries(&'a self) -> Self::Iter;
}
