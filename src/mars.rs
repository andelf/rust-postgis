//  FileName    : mars.rs
//  Author      : ShuYu Wang <andelf@gmail.com>
//  Created     : Thu May 28 14:55:36 2015 by ShuYu Wang
//  Copyright   : Feather Workshop (c) 2015
//  Description : WGS84 GCJ02 conversion for rust
//  Time-stamp: <2015-06-01 10:45:55 andelf>

//! Conversion between GCJ-02 and WGS-84 coordinates.

use crate::ewkb;

// http://emq.googlecode.com/svn/emq/src/Algorithm/Coords/Converter.java
struct Converter {
    casm_rr: f64,
    casm_t1: f64,
    casm_t2: f64,
    casm_x1: f64,
    casm_y1: f64,
    casm_x2: f64,
    casm_y2: f64,
    casm_f: f64,
}

fn yj_sin2(x: f64) -> f64 {
    let mut x = x;
    let mut ff: i32 = 0;

    if x < 0.0 {
        x = -x;
        ff = 1;
    }

    let cc = (x / 6.28318530717959) as i32;

    let mut tt = x - cc as f64 * 6.28318530717959;
    if tt > 3.1415926535897932 {
        tt = tt - 3.1415926535897932;
        if ff == 1 {
            ff = 0;
        } else if ff == 0 {
            ff = 1;
        }
    }
    x = tt;
    let mut ss = x;
    let mut s2 = x;
    tt = tt * tt;
    s2 = s2 * tt;
    ss = ss - s2 * 0.166666666666667;
    s2 = s2 * tt;
    ss = ss + s2 * 8.33333333333333E-03;
    s2 = s2 * tt;
    ss = ss - s2 * 1.98412698412698E-04;
    s2 = s2 * tt;
    ss = ss + s2 * 2.75573192239859E-06;
    s2 = s2 * tt;
    ss = ss - s2 * 2.50521083854417E-08;
    if ff == 1 {
        ss = -ss;
    }
    ss
}

fn transform_yj5(x: f64, y: f64) -> f64 {
    let mut tt: f64 =
        300.0 + 1.0 * x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * (x * x).sqrt().sqrt();
    tt = tt
        + (20.0 * yj_sin2(18.849555921538764 * x) + 20.0 * yj_sin2(6.283185307179588 * x)) * 0.6667;
    tt = tt
        + (20.0 * yj_sin2(3.141592653589794 * x) + 40.0 * yj_sin2(1.047197551196598 * x)) * 0.6667;
    tt = tt
        + (150.0 * yj_sin2(0.2617993877991495 * x) + 300.0 * yj_sin2(0.1047197551196598 * x))
            * 0.6667;
    tt
}

fn transform_yjy5(x: f64, y: f64) -> f64 {
    let mut tt =
        -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * (x * x).sqrt().sqrt();
    tt = tt
        + (20.0 * yj_sin2(18.849555921538764 * x) + 20.0 * yj_sin2(6.283185307179588 * x)) * 0.6667;
    tt = tt
        + (20.0 * yj_sin2(3.141592653589794 * y) + 40.0 * yj_sin2(1.047197551196598 * y)) * 0.6667;
    tt = tt
        + (160.0 * yj_sin2(0.2617993877991495 * y) + 320.0 * yj_sin2(0.1047197551196598 * y))
            * 0.6667;
    tt
}

fn transform_jy5(x: f64, xx: f64) -> f64 {
    let a: f64 = 6378245.0;
    let e: f64 = 0.00669342;
    let n: f64 =
        (1.0 - e * yj_sin2(x * 0.0174532925199433) * yj_sin2(x * 0.0174532925199433)).sqrt();
    (xx * 180.0) / (a / n * (x * 0.0174532925199433).cos() * 3.1415926)
}

fn transform_jyj5(x: f64, yy: f64) -> f64 {
    let a: f64 = 6378245.0;
    let e: f64 = 0.00669342;
    let mm = 1.0 - e * yj_sin2(x * 0.0174532925199433) * yj_sin2(x * 0.0174532925199433);
    let m = (a * (1.0 - e)) / (mm * mm.sqrt());
    (yy * 180.0) / (m * 3.1415926)
}

impl Converter {
    pub fn new() -> Converter {
        Converter {
            casm_rr: 0.0,
            casm_t1: 0.0,
            casm_t2: 0.0,
            casm_x1: 0.0,
            casm_y1: 0.0,
            casm_x2: 0.0,
            casm_y2: 0.0,
            casm_f: 0.0,
        }
    }

    fn random_yj(&mut self) -> f64 {
        let casm_a: f64 = 314159269.0;
        let casm_c: f64 = 453806245.0;
        self.casm_rr = casm_a * self.casm_rr + casm_c;
        let t = (self.casm_rr / 2.0) as i32 as f64;
        self.casm_rr = self.casm_rr - t * 2.0;
        self.casm_rr = self.casm_rr / 2.0;
        self.casm_rr
    }

    fn init_casm(&mut self, w_time: f64, w_lng: f64, w_lat: f64) {
        self.casm_t1 = w_time;
        self.casm_t2 = w_time;
        let tt = (w_time / 0.357) as i32 as f64;
        self.casm_rr = w_time - tt * 0.357;
        if w_time == 0.0 {
            self.casm_rr = 0.3;
        }
        self.casm_x1 = w_lng;
        self.casm_y1 = w_lat;
        self.casm_x2 = w_lng;
        self.casm_y2 = w_lat;
        self.casm_f = 3.0;
    }
}

fn wgtochina_lb(
    wg_flag: i32,
    wg_lng: i32,
    wg_lat: i32,
    wg_heit: i32,
    _wg_week: i32,
    wg_time: i32,
) -> (f64, f64) {
    let mut point: (f64, f64) = (wg_lng as f64, wg_lat as f64);

    let x1_x2: f64;
    let y1_y2: f64;
    let casm_v: f64;
    let mut x_add: f64;
    let mut y_add: f64;
    let h_add: f64;

    if wg_heit > 5000 {
        return point;
    }
    let mut x_l = wg_lng as f64;
    x_l = x_l / 3686400.0;
    let mut y_l = wg_lat as f64;
    y_l = y_l / 3686400.0;

    if x_l < 72.004 {
        return point;
    }
    if x_l > 137.8347 {
        return point;
    }
    if y_l < 0.8293 {
        return point;
    }
    if y_l > 55.8271 {
        return point;
    }

    let mut me = Converter::new();

    if wg_flag == 0 {
        me.init_casm(wg_time as f64, wg_lng as f64, wg_lat as f64);
        point.0 = wg_lng as f64;
        point.1 = wg_lat as f64;
        return point;
    }
    me.casm_t2 = wg_time as f64;
    let t1_t2: f64 = (me.casm_t2 - me.casm_t1) / 1000.0;
    if t1_t2 <= 0.0 {
        me.casm_t1 = me.casm_t2;
        me.casm_f = me.casm_f + 1.0;
        me.casm_x1 = me.casm_x2;
        me.casm_f = me.casm_f + 1.0;
        me.casm_y1 = me.casm_y2;
        me.casm_f = me.casm_f + 1.0;
    } else {
        if t1_t2 > 120.0 {
            if me.casm_f as i32 == 3 {
                me.casm_f = 0.0;
                me.casm_x2 = wg_lng as f64;
                me.casm_y2 = wg_lat as f64;
                x1_x2 = me.casm_x2 - me.casm_x1;
                y1_y2 = me.casm_y2 - me.casm_y1;
                casm_v = (x1_x2 * x1_x2 + y1_y2 * y1_y2).sqrt() / t1_t2;
                if casm_v > 3185.0 {
                    return point;
                }
            }
            me.casm_t1 = me.casm_t2;
            me.casm_f = me.casm_f + 1.0;
            me.casm_x1 = me.casm_x2;
            me.casm_f = me.casm_f + 1.0;
            me.casm_y1 = me.casm_y2;
            me.casm_f = me.casm_f + 1.0;
        }
    }
    x_add = transform_yj5(x_l - 105.0, y_l - 35.0);
    y_add = transform_yjy5(x_l - 105.0, y_l - 35.0);
    h_add = wg_heit as f64;
    x_add = x_add + h_add * 0.001 + yj_sin2(wg_time as f64 * 0.0174532925199433) + me.random_yj();
    y_add = y_add + h_add * 0.001 + yj_sin2(wg_time as f64 * 0.0174532925199433) + me.random_yj();
    point = (0.0, 0.0);
    point.0 = (x_l + transform_jy5(y_l, x_add)) * 3686400.0;
    point.1 = (y_l + transform_jyj5(y_l, y_add)) * 3686400.0;
    return point;
}

// WGS84 coords to MARS
pub fn from_wgs84(x: f64, y: f64) -> (f64, f64) {
    let x1 = x * 3686400.0;
    let y1 = y * 3686400.0;
    let gps_week = 0;
    let gps_week_time = 0;
    let gps_height = 0;

    let point = wgtochina_lb(
        1,
        x1 as i32,
        y1 as i32,
        gps_height as i32,
        gps_week as i32,
        gps_week_time as i32,
    );
    let mut tempx = point.0;
    let mut tempy = point.1;
    tempx = tempx / 3686400.0;
    tempy = tempy / 3686400.0;

    (tempx, tempy)
}

// MARS coords to WGS84
pub fn to_wgs84(x: f64, y: f64) -> (f64, f64) {
    // TODO: figure out if it is in China
    let epsilon: f64 = 0.00001;
    fn bisection_find_vals(
        x: f64,
        y: f64,
        x0: f64,
        y0: f64,
        x1: f64,
        y1: f64,
        epsilon: f64,
    ) -> (f64, f64) {
        let (mut x0, mut y0, mut x1, mut y1) = (x0, y0, x1, y1);
        let (mut x_, mut y_): (f64, f64);

        loop {
            x_ = (x0 + x1) / 2.0;
            y_ = (y0 + y1) / 2.0;
            let (x_e, y_e) = from_wgs84(x_, y_);

            // println!("x0: {}, y0: {}, x1: {}, y1: {}", x0, y0, x1, y1);
            // println!("target => {:?}         {:?}", (x,y), (x_e, y_e));

            if (x - x_e).abs() <= epsilon && (y - y_e).abs() <= epsilon {
                break;
            }

            let (x_e0, y_e0) = from_wgs84(x0, y0);
            let (x_e1, y_e1) = from_wgs84(x1, y1);

            // if over some bound
            let mut adjusted = true;

            if x < x_e0 {
                //x1 = x0;
                x0 -= x_e0 - x; // instead of 0.5
            } else if x > x_e1 {
                //x0 = x1;
                x1 += x - x_e1;
            } else {
                adjusted = false;
            }

            // ----*---y_e0-------y_e----------y_e1--------*--------
            if y < y_e0 {
                //y1 = y0;
                y0 -= y_e0 - y;
            } else if y > y_e1 {
                //y0 = y1;
                y1 += y - y_e1;
            } else {
                adjusted |= false;
            }

            if adjusted {
                continue;
            }

            if x_e0 <= x && x <= x_e {
                x1 = x_;
            } else if x_e <= x && x <= x_e1 {
                x0 = x_;
            }

            if y_e0 <= y && y <= y_e {
                y1 = y_;
            } else if y_e <= y && y <= y_e1 {
                y0 = y_;
            }

            if x1 - x0 < epsilon * 0.1 {
                x0 = x0 - x0 * 0.01;
                x1 = x1 + x1 * 0.01;
            }
            if y1 - y0 < epsilon * 0.1 {
                y0 = y0 - y0 * 0.01;
                y1 = y1 + y1 * 0.01;
            }
        }
        //        bisection_find_vals(x, y, x_0, y_0, x_1, y_1, epsilon)
        (x_, y_)
    }

    bisection_find_vals(x, y, x - 0.1, y - 0.1, x + 0.1, y + 0.1, epsilon)
}

impl ewkb::Point {
    pub fn new_wgs84(x: f64, y: f64) -> ewkb::Point {
        ewkb::Point {
            x: x,
            y: y,
            srid: Some(4326),
        }
    }
    pub fn from_gcj02(x: f64, y: f64) -> ewkb::Point {
        let (x0, y0) = to_wgs84(x, y);
        ewkb::Point {
            x: x0,
            y: y0,
            srid: Some(4326),
        }
    }
    pub fn to_gcj02(&self) -> (f64, f64) {
        from_wgs84(self.x, self.y)
    }
}

#[test]
fn test_mars_to_wgs84() {
    let (x, y) = to_wgs84(116.501419, 39.99844);
    println!("x = {} y = {}", x, y);
}
