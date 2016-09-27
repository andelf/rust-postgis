use std::mem;

pub trait Point: Sized {
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
}

