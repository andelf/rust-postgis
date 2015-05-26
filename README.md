# rust-postgis
postgis helper library.


Working in progress.

Not Yet Finished!


## HowTo

```rust
use postgres::{Connection, SslMode};
use postgis::{Point, LineString};

fn main() {
    // conn ....
    let stmt = conn.prepare("SELECT * FROM busline").unwrap();
    for row in stmt.query(&[]).unwrap() {
        println!(">>>>>> {}", row.get::<_, LineString<Point>>("route"));
    }

}
```
