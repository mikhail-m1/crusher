#![allow(dead_code)]
#![allow(unused)]
use std::net::UdpSocket;
use std::ops::Deref;
use std::process::Output;
use std::time::Instant;

use ::parquet::data_type::{DataType, Int64Type};
use ::parquet::file::reader::{self, FileReader, RowGroupReader};
use parquet::{ParquetColumnReader, ParquetReaders};
use processing::{
    make_sum, AsIsMapper, AsIsProcessor, Equal, Group, Literal, Not, ProcessingError, Processor,
    Readers, Type,
};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use crate::parquet2::{Column, ParquetReader};
use crate::processing::Source;

mod parquet;
mod parquet2;
pub mod processing;
mod sql;

fn main() -> Result<(), ProcessingError> {
    use ::parquet::file::reader::{FileReader, SerializedFileReader};
    use std::{fs::File, path::Path};
    println!("column");
    main_external_filter_col()?;
    return Ok(());
    println!("old");
    let sql = "select 1 from file('') where Impressions = 442";
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let mut counter = 0;
    let now = Instant::now();
    for _ in 0..100 {
        // let path = Path::new("sample.parquet");
        // let path = Path::new("flat.parquet");
        let path = Path::new("flat_1m.parquet");
        // let path = Path::new("with_nulls.parquet");
        if let Ok(file) = File::open(&path) {
            let reader = Box::new(SerializedFileReader::new(file).unwrap());
            let mut readers = ParquetReaders::new(reader);
            // _example2(&mut readers);
            // let sql = "select * from file('') where b = 111 and a = 'Hello'";

            let mut processor = sql::to_process(&ast[0], &readers)?;
            while let Some(v) = processor.next(&mut readers) {
                counter += 1;
                // println!("{v:?}");
            }
        }
    }
    println!(
        "{} {counter} {}",
        now.elapsed().as_micros(),
        size_of::<Type>()
    );
    Ok(())
}

struct MyFilter();

impl parquet2::Filter for MyFilter {
    // #[cfg_attr(feature = "hotpath", hotpath::measure())]
    fn check(&mut self, columns: &[parquet2::Type]) -> bool {
        //panic!("{columns:?}");
        // if columns[0] == parquet2::Type::I64(442) {
        //     panic!("{columns:?}");
        // }
        columns[0] == parquet2::Type::I64(442)
    }
}

#[inline(never)]
fn main_external_filter_col() -> Result<(), ProcessingError> {
    use ::parquet::file::reader::{FileReader, SerializedFileReader};
    use std::{fs::File, path::Path};

    let now = Instant::now();
    let mut counter = 0;
    for _ in 0..100 {
        // let path = Path::new("sample.parquet");
        // let path = Path::new("flat.parquet");
        let path = Path::new("flat_1m.parquet");
        // let path = Path::new("with_nulls.parquet");
        if let Ok(file) = File::open(&path) {
            let reader = Box::new(SerializedFileReader::new(file).unwrap());
            let mut readers = parquet2::ParquetReader::new(reader)?;
            // _example2(&mut readers);
            // let sql = "select * from file('') where b = 111 and a = 'Hello'";
            let mut filter: Box<dyn parquet2::Filter> = Box::new(MyFilter());

            loop {
                let p = parquet2::find(&mut readers, &[36], &mut filter)?;
                if p.0 == usize::max_value() {
                    break;
                }
                counter += 1;
                let v = parquet2::get(&mut readers, &[0, 3, 19, 27, 36, 37], p)?;
                // println!("r: {v:?}");
            }
        }
    }
    println!("{} {counter}", now.elapsed().as_micros() as f64 / 1e8);
    Ok(())
}

/*
(0, 194193)
(0, 364330)
(0, 740806)
(0, 853387)
(0, 853810)
 */
