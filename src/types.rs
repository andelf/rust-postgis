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

pub trait MultiLineString<'a> {
    type ItemType: 'a + LineString<'a>;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn lines(&'a self) -> Self::Iter;
}

pub trait Polygon<'a> {
    type ItemType: 'a + LineString<'a>;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn rings(&'a self) -> Self::Iter;
}

// --- Adapter structs and traits for EWKB output

#[derive(PartialEq, Clone, Debug)]
pub enum PointType {
    Point,
    PointZ,
    PointM,
    PointZM
}

pub struct EwkbPoint<'a> {
    pub geom: &'a Point,
    pub srid: Option<i32>,
    pub point_type: PointType,
}

pub trait AsEwkbPoint<'a> {
    fn as_ewkb(&'a self) -> EwkbPoint<'a>;
}

pub struct EwkbLineString<'a, T, I>
    where T: 'a + Point,
          I: 'a + Iterator<Item=&'a T> + ExactSizeIterator<Item=&'a T>
{
    pub geom: &'a LineString<'a, ItemType=T, Iter=I>,
    pub srid: Option<i32>,
    pub point_type: PointType,
}

pub trait AsEwkbLineString<'a> {
    type PointType: 'a + Point;
    type Iter: Iterator<Item=&'a Self::PointType>+ExactSizeIterator<Item=&'a Self::PointType>;
    fn as_ewkb(&'a self) -> EwkbLineString<'a, Self::PointType, Self::Iter>;
}
