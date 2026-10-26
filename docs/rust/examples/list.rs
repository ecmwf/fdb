mod common;

use fdb::ListOptions;
use metkit::MarsRequest;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let (_dir, fdb) = common::open_temporary_fdb()?;
    for step in [0, 6, 12] {
        fdb.archive(
            &common::sample_key(&step.to_string()),
            &common::sample_field(step)?,
        )?;
    }
    fdb.flush()?;

    // [list]
    let mut request = MarsRequest::new("retrieve");
    request.set("class", "rd");
    request.set("expver", "xxxx");

    for item in fdb.list(&request, ListOptions::default())? {
        let item = item?;
        let step = item.datum_key.iter().find(|(k, _)| k == "step");
        println!(
            "{:?} {} bytes at {}+{}",
            step, item.length, item.uri, item.offset
        );
    }
    // [end list]

    // [compact]
    let summary = fdb
        .list(&request, ListOptions::default())?
        .dump_compact(&mut std::io::stdout())?;
    println!("{} fields, {} bytes", summary.fields, summary.total_bytes);
    // [end compact]

    // [axes]
    let axes = fdb.axes(&request, 3)?;
    println!("{:?}", axes["step"]);
    // [end axes]
    Ok(())
}
