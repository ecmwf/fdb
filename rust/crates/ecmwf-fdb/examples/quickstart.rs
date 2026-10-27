use std::io::Read;

use fdb::{Fdb, Key, ListOptions};
use metkit::{CodesHandle, Expanded, MarsRequest};

// [main]
const SCHEMA: &str = "\
param:    Param;
step:     Step;
date:     Date;
levelist: Double;
expver:   Expver;
time:     Time;

[ class, expver, stream, date, time, domain?
    [ type, levtype
        [ step, levelist?, param ]]
]
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();

    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("schema"), SCHEMA)?;
    let config: eckit::Config = format!(
        "type: local\nengine: toc\nschema: {root}/schema\nspaces:\n  - roots:\n      - path: {root}\n",
        root = dir.path().display()
    )
    .parse()?;
    let fdb = Fdb::open(Some(&config), None)?;

    let key = Key::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20240101")
        .with("time", "1200")
        .with("domain", "g")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", "0")
        .with("param", "167");
    let message = CodesHandle::from_sample("GRIB2")?;
    fdb.archive(&key, message.message_data()?)?;
    fdb.flush()?;

    let mut request = MarsRequest::new("retrieve");
    request.set("class", "rd");
    request.set("expver", "xxxx");
    for item in fdb.list(&request, ListOptions::default())? {
        let item = item?;
        println!("{} bytes at {}", item.length, item.uri);
    }

    let field: MarsRequest<Expanded> = "retrieve, class=rd, expver=xxxx, stream=oper, \
        date=20240101, time=1200, type=fc, levtype=sfc, step=0, param=167"
        .parse()?;
    let (mut reading, _length) = fdb.retrieve(&field)?.open_for_read()?;
    let mut data = Vec::new();
    reading.read_to_end(&mut data)?;
    println!("{}", data.len());
    Ok(())
}
// [end main]
