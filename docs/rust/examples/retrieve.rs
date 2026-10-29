mod common;

use std::io::Read;

use fdb::ListOptions;
use metkit::{Expanded, MarsRequest};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let (_dir, fdb) = common::open_temporary_fdb()?;
    for step in [0, 6] {
        fdb.archive(
            &common::sample_key(&step.to_string()),
            &common::sample_field(step)?,
        )?;
    }
    fdb.flush()?;

    // [retrieve]
    let request: MarsRequest<Expanded> = "retrieve, class=rd, expver=xxxx, stream=oper, \
        date=20240101, time=1200, type=fc, levtype=sfc, step=0/6, param=167"
        .parse()?;

    let (mut reading, _length) = fdb.retrieve(&request)?.open_for_read()?;
    for message in eckit::MessageReader::new(&mut reading)? {
        let message = message?;
        let step: i64 = message.get("step")?;
        println!("step {step}: {} bytes", message.length());
    }
    // [end retrieve]

    // [read]
    let mut list = MarsRequest::new("retrieve");
    list.set("class", "rd");
    list.set("expver", "xxxx");

    let uris: Vec<String> = fdb
        .list(&list, ListOptions::default())?
        .map(|item| item.map(|item| item.uri))
        .collect::<fdb::Result<_>>()?;
    let (mut reading, _) = fdb.read_uris(&uris, true)?.open_for_read()?;
    let mut bytes = Vec::new();
    reading.read_to_end(&mut bytes)?;

    let items = fdb.list(&list, ListOptions::default())?;
    let (mut reading, _) = fdb.read_from_list(items, false)?.open_for_read()?;
    let mut again = Vec::new();
    reading.read_to_end(&mut again)?;
    assert_eq!(bytes.len(), again.len());
    // [end read]
    Ok(())
}
