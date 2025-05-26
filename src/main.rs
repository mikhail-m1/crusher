#![allow(dead_code)]
#![allow(unused)]
use std::net::UdpSocket;
use std::ops::Deref;

use ::parquet::file::reader::{self, FileReader, RowGroupReader};
use parquet::ParquetReaders;
use processing::{
    AsIsMapper, AsIsProcessor, Equal, Group, Literal, Not, ProcessingError, Processor, Readers,
    Type, make_sum,
};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

mod parquet;
mod processing;
mod sql;

fn main() -> Result<(), ProcessingError> {
    use ::parquet::file::reader::{FileReader, SerializedFileReader};
    use std::{fs::File, path::Path};

    // let path = Path::new("sample.parquet");
    // let path = Path::new("flat_1m.parquet");
    let path = Path::new("with_nulls.parquet");
    if let Ok(file) = File::open(&path) {
        let reader = Box::new(SerializedFileReader::new(file).unwrap());
        let mut readers = ParquetReaders::new(reader);
        // _example2(&mut readers);
        let sql = "select a, b from x where b = 111 and a = 'Hello'";
        let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let mut processor = sql::to_process(&ast[0], &readers)?;
        while let Some(v) = processor.next(&mut readers) {
            println!("{v:?}");
        }
    }
    Ok(())
}

/*
TODO:
    * cli
    * speed tests
    * tests
    * group by
    * unittests
    * THINK: intermidaite tables -> need to join readers and Processor -> look at keep
    * try to use parquet stat
*/
