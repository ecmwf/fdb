mod common;

use std::sync::Arc;
use std::thread;

use fdb::{Fdb, ListOptions};
use metkit::MarsRequest;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let (_dir, fdb) = common::open_temporary_fdb()?;
    for step in [0, 6, 12, 18] {
        fdb.archive(
            &common::sample_key(&step.to_string()),
            &common::sample_field(step)?,
        )?;
    }
    fdb.flush()?;

    // [threads]
    let fdb: Arc<Fdb> = Arc::new(fdb);

    let workers: Vec<_> = [0, 6, 12, 18]
        .into_iter()
        .map(|step| {
            let fdb = Arc::clone(&fdb);
            thread::spawn(move || -> fdb::Result<usize> {
                let mut request = MarsRequest::new("retrieve");
                request.set("class", "rd");
                request.set("step", step.to_string());
                Ok(fdb.list(&request, ListOptions::default())?.count())
            })
        })
        .collect();

    for worker in workers {
        assert_eq!(worker.join().expect("thread panicked")?, 1);
    }
    // [end threads]
    Ok(())
}
