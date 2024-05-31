use std::error::Error;

use bitcask_rs_core::{config::Config, db::Engine};

fn main() -> Result<(), Box<dyn Error>> {
    let db = Engine::open(Config {
        db_path: "/tmp/222/".into(),
        ..Default::default()
    })?;

    db.put("111", "222")?;

    println!("{}", String::from_utf8(db.get("111")?)?);

    Ok(())
}
