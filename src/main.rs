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
    main_external_filter_col();
    // main_split_read_and_iterate();
    println!("ext filter");
    // main_external_filter();
    println!("direct");
    // main_();
    return Ok(());
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

#[inline(never)]
fn main_() -> Result<(), ProcessingError> {
    use ::parquet::file::reader::{FileReader, SerializedFileReader};
    use std::{fs::File, path::Path};

    let now = Instant::now();
    let mut counter = 0;
    for _ in 0..100 {
        // let path = Path::new("sample.parquet");
        //let path = Path::new("flat.parquet");
        let path = Path::new("flat_1m.parquet");
        // let path = Path::new("with_nulls.parquet");
        if let Ok(file) = File::open(&path) {
            let reader = Box::new(SerializedFileReader::new(file).unwrap());
            let mut column_reader = ParquetColumnReader::<Int64Type>::new(36);
            let row_group = reader.get_row_group(0).unwrap();
            column_reader.next_group(row_group.as_ref());
            let rows = row_group.metadata().num_rows();
            for p in 0..rows {
                column_reader.set_position(p as usize);
                if let parquet::Output::Some(v) = column_reader.get().unwrap() {
                    if *v == 442 {
                        counter += 1;
                    }
                }
            }
        }
    }
    println!("{} {counter}", now.elapsed().as_micros());
    Ok(())
}

struct MyFilter();
impl processing::Filter for MyFilter {
    // #[cfg_attr(feature = "hotpath", hotpath::measure())]
    fn check(&mut self, columns: &[processing::Type]) -> bool {
        // println!("{columns:?}");
        columns[0] == Type::I64(442)
    }
}

#[inline(never)]
// #[cfg_attr(feature = "hotpath", hotpath::main())]
fn main_external_filter() -> Result<(), ProcessingError> {
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
            let mut readers = ParquetReaders::new(reader);
            // _example2(&mut readers);
            // let sql = "select * from file('') where b = 111 and a = 'Hello'";
            let mut filter: Box<dyn processing::Filter> = Box::new(MyFilter());
            while readers.findx(&[36], &mut filter)? {
                counter += 1;
                // println!("found");
            }
        }
    }
    println!("{} {counter}", now.elapsed().as_micros());
    Ok(())
}

impl parquet2::Filter for MyFilter {
    // #[cfg_attr(feature = "hotpath", hotpath::measure())]
    fn check(&mut self, columns: &[parquet2::Type]) -> bool {
        // println!("{columns:?}");
        columns[0] == parquet2::Type::I64(442)
    }
}

#[inline(never)]
fn main_external_filter_col() -> Result<(), ProcessingError> {
    use ::parquet::file::reader::{FileReader, SerializedFileReader};
    use std::{fs::File, path::Path};

    let now = Instant::now();
    let mut counter = 0;
    for _ in 0..10 {
        // let path = Path::new("sample.parquet");
        // let path = Path::new("flat.parquet");
        let path = Path::new("flat_1m.parquet");
        // let path = Path::new("with_nulls.parquet");
        if let Ok(file) = File::open(&path) {
            let reader = Box::new(SerializedFileReader::new(file).unwrap());
            let mut readers = parquet2::ParquetReader::new(reader);
            // _example2(&mut readers);
            // let sql = "select * from file('') where b = 111 and a = 'Hello'";
            let mut filter: Box<dyn parquet2::Filter> = Box::new(MyFilter());
            while parquet2::find(&mut readers, &[36], &mut filter)? {
                counter += 1;
                // println!("found");
            }
        }
    }
    println!("{} {counter}", now.elapsed().as_micros());
    Ok(())
}

/*/
#[inline(never)]
#[cfg_attr(feature = "hotpath", hotpath::main())]
fn main_split_read_and_iterate() -> Result<(), ProcessingError> {
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
            let mut readers = ParquetDataReaders::new(reader);
            let mut column_reader = ParquetColumnDataReader::<Int64Type>::new(36);
            readers.init(0, &mut column_reader);
            let mut column_data = ColumnData::new();
            let mut base = 0;
            while let Ok(true) = column_reader.read(&mut column_data) {
                counter += find_in_column(&mut column_data);
                base += column_data.len();
            }
        }
    }
    println!("{} {counter}", now.elapsed().as_micros());
    Ok(())
}

trait CFilter {
    fn check(v: &Vec<Ref>)
}

#[inline(never)]
fn find_in_column(column_data: &mut dyn parquet::Column) -> usize {
    let mut counter = 0;
    let len = column_data.len();
    for c in 0..len {
        if let Type2::I64(v) = column_data.next()
            && *v == 442
        {
            counter += 1;
        }
    }
    counter
}
*/

/*
TODO:
2025-12-11
    * need to rewrite column approach to see real speen when we have dyn calls

2025-06
    * perf is bad!, half of the time in wrappers, total 2_671_688 for 100 repeats on 1m select imp=442 vs 1_393_297
      and even this is most of the time in my loops

    * experiment with removing count() and simplify next() for AsIsProcessor
    * cli
    * speed tests
        1 group is ok, by 8 is n slower, CH - same speed, threads?
        losses: 10% next
        need to reprofile but in general looks ok if add threads
    * tests
    * group by
    * unittests
    * THINK: intermidaite tables -> need to join readers and Processor -> look at keep
    * try to use parquet stat
*/
