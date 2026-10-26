use std::io::Read;

use fdb::{Fdb, ListOptions};
use metkit::{Expanded, MarsRequest};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let fdb = Fdb::open_default()?;

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
    let (mut reading, length) = fdb.retrieve(&field)?.open_for_read()?;
    let mut data = Vec::with_capacity(usize::try_from(length)?);
    reading.read_to_end(&mut data)?;
    println!("{}", data.len());
    Ok(())
}
