mod common;

use fdb::{ControlAction, ControlIdentifier, DumpOptions, PurgeOptions, WipeOptions};
use metkit::MarsRequest;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let (_dir, fdb) = common::open_temporary_fdb()?;
    fdb.archive(&common::sample_key("0"), &common::sample_field(0)?)?;
    fdb.flush()?;

    let mut request = MarsRequest::new("retrieve");
    request.set("class", "rd");
    request.set("expver", "xxxx");

    // [inspect]
    for entry in fdb.dump(&request, DumpOptions { simple: true })? {
        println!("{}", entry?.content);
    }
    for db in fdb.status(&request)? {
        let db = db?;
        println!("{} {:?}", db.location, db.status);
    }
    for stats in fdb.stats_iter(&request)? {
        let stats = stats?;
        println!(
            "{} fields, {} bytes",
            stats.index_statistics.fields_count, stats.index_statistics.fields_size
        );
    }
    // [end inspect]

    // [wipe]
    for entry in fdb.wipe(&request, WipeOptions::default())? {
        println!("would remove {}", entry?.content);
    }
    for entry in fdb.purge(&request, PurgeOptions::default())? {
        println!("would purge {}", entry?.content);
    }
    // [end wipe]

    // [control]
    for db in fdb.control(
        &request,
        ControlAction::Disable,
        &[ControlIdentifier::Archive],
    )? {
        let db = db?;
        println!("{} {:?}", db.location, db.identifiers);
    }
    assert!(fdb.enabled(ControlIdentifier::Retrieve));
    // [end control]
    Ok(())
}
