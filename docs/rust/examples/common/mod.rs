#![allow(dead_code)]

use std::fs;

use fdb::{Fdb, Key};
use metkit::CodesHandle;

pub const SCHEMA: &str = "\
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

// [config]
pub fn open_temporary_fdb() -> Result<(tempfile::TempDir, Fdb), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    fs::write(dir.path().join("schema"), SCHEMA)?;

    let yaml = format!(
        "type: local\nengine: toc\nschema: {root}/schema\nspaces:\n  - roots:\n      - path: {root}\n",
        root = dir.path().display()
    );
    let config: eckit::Config = yaml.parse()?;
    let fdb = Fdb::open(Some(&config), None)?;
    Ok((dir, fdb))
}
// [end config]

pub fn sample_key(step: &str) -> Key {
    Key::new()
        .with("class", "rd")
        .with("expver", "xxxx")
        .with("stream", "oper")
        .with("date", "20240101")
        .with("time", "1200")
        .with("domain", "g")
        .with("type", "fc")
        .with("levtype", "sfc")
        .with("step", step)
        .with("param", "167")
}

pub fn sample_field(step: i64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut handle = CodesHandle::from_sample("GRIB2")?;
    handle.set("setLocalDefinition", 1_i64)?;
    handle.set("class", "rd")?;
    handle.set("expver", "xxxx")?;
    handle.set("stream", "oper")?;
    handle.set("date", 20240101_i64)?;
    handle.set("time", 1200_i64)?;
    handle.set("type", "fc")?;
    handle.set("step", step)?;
    handle.set("paramId", 167_i64)?;
    Ok(handle.message_data()?.to_vec())
}
