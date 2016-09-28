use std::slice::Iter;

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

/// Iterator for points of a line
pub struct Points<'a, T: 'a + Point>
{
    pub iter: Iter<'a, T>
}

pub trait LineString<'a> {
    type PointType: Point;

    fn points(&'a self) -> Points<'a, Self::PointType>;
}

// --- Iterator impl

impl<'a, T> Iterator for Points<'a, T> where T: 'a + Point {
    type Item = &'a Point;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|it| it as &Point)
    }
}
