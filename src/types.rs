use std::slice::Iter;
use geo;

pub trait Point {
    fn x(&self) -> f64;
    fn y(&self) -> f64;
    fn opt_z(&self) -> Option<f64> {
        None
    }
    fn opt_m(&self) -> Option<f64> {
        None
    }
    //fn has_z() -> bool { false }
    //fn has_m() -> bool { false }
}

/// Iterator for points of line or multi-point geometry
pub trait Points<'a> {
    type ItemType: 'a + Point;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn points(&'a self) -> Self::Iter;
}

pub trait LineString<'a>: Points<'a> {
}

/// Iterator for lines of multi-lines
pub trait Lines<'a> {
    type ItemType: 'a + LineString<'a>;
    type Iter: Iterator<Item=&'a Self::ItemType>;
    fn lines(&'a self) -> Self::Iter;
}

pub trait MultiLineString<'a>: Lines<'a> {
}

// --- ToGeo impl

impl geo::ToGeo<f64> for Point {
    fn to_geo(&self) -> geo::Geometry<f64> {
        geo::Geometry::Point(geo::Point::new(self.x(), self.y()))
    }
}
