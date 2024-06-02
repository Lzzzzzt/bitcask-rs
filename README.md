# A Fast Key-Value Database based on BitCask written by Rust

## Features

[x] CRUD
[x] Key Expire
[X] Write Batch
[X] Simple MVCC transaction
[x] Backup

## Basic Usage
```Rust
use std::error::Error;

use bitcask_rs_core::{config::Config, db::Engine};

fn main() -> Result<(), Box<dyn Error>> {
    let db = Engine::open(Config {
        db_path: "/tmp/bitcask/".into(),
        ..Default::default()
    })?;

    db.put("foo", "bar")?;

    println!("{}", String::from_utf8(db.get("111")?)?);

    db.del("foo")?;

    Ok(())
}
```
