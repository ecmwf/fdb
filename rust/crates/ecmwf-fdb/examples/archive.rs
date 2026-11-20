mod common;

use std::io::Cursor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let (_dir, fdb) = common::open_temporary_fdb()?;

    // [callbacks]
    fdb.on_archive(|field| {
        println!(
            "archived {} bytes at {}",
            field.data.len(),
            field.location_uri.as_deref().unwrap_or("?")
        );
    });
    fdb.on_flush(|| println!("flushed"));
    // [end callbacks]

    // [archive]
    let key = common::sample_key("0");
    let field = common::sample_field(0)?;
    fdb.archive(&key, &field)?;

    fdb.archive_raw(&common::sample_field(6)?)?;

    let stream = Cursor::new(common::sample_field(12)?);
    fdb.archive_reader(stream)?;

    assert!(fdb.dirty());
    fdb.flush()?;
    assert!(!fdb.dirty());
    // [end archive]

    let stats = fdb.stats();
    println!("{} {}", stats.num_archive, stats.num_flush);
    Ok(())
}
