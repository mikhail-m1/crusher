#![allow(dead_code)]
#![allow(unused)]
use std::cell::LazyCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::UdpSocket;
use std::ops::Deref;

use parquet::basic::{LogicalType, StringType, Type as PhysicalType};
use parquet::column::reader::{ColumnReaderImpl, get_typed_column_reader};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType, DoubleType, Int32Type, Int64Type,
};
use parquet::file::reader::{self, FileReader, RowGroupReader};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn _sql() {
    /*
    4. parse ast, min/max/sum/count for a value and (*) where with name = value
        create structure to process rows
    5. sql and -> read second column starting from
    6. try to use parquet stat

    AST: [Query(Query {
        with: None,
        body: Select(Select {
            select_token: TokenWithSpan {
                token: Word(Word { value: "select", quote_style: None, keyword: SELECT }),
                span: Span(Location(1,1)..Location(1,7)) },
            distinct: None, top: None, top_before_distinct: false,
            projection: [UnnamedExpr(Function(Function {
                name: ObjectName([Identifier(Ident { value: "sum", quote_style: None, span: Span(Location(1,8)..Location(1,11)) })]),
                uses_odbc_syntax: false,
                parameters: None,
                args: List(FunctionArgumentList { duplicate_treatment: None, args: [Unnamed(Expr(Identifier(Ident { value: "a", quote_style: None, span: Span(Location(1,12)..Location(1,13)) })))], clauses: [] }),
                filter: None, null_treatment: None, over: None, within_group: [] }))],
            into: None,
            from: [TableWithJoins { relation: Table { name: ObjectName([Identifier(Ident { value: "x", quote_style: None, span: Span(Location(1,20)..Location(1,21)) })]), alias: None, args: None, with_hints: [], version: None, with_ordinality: false, partitions: [], json_path: None, sample: None, index_hints: [] }, joins: [] }],
            lateral_views: [], prewhere: None,
            selection: Some(BinaryOp {
                left: Identifier(Ident { value: "b", quote_style: None, span: Span(Location(1,28)..Location(1,29)) }),
                op: Eq,
                right: Value(ValueWithSpan { value: Number("42", false), span: Span(Location(1,32)..Location(1,34)) }) }),
            group_by: Expressions([], []), cluster_by: [], distribute_by: [], sort_by: [], having: None, named_window: [], qualify: None, window_before_qualify: false, value_table_mode: None, connect_by: None, flavor: Standard }),
        order_by: None,
        limit: None,
        limit_by: [], offset: None, fetch: None, locks: [], for_clause: None, settings: None, format_clause: None })]

    body = Select
    projections = [sum(a)]
    selection = [bin op, a = 42]

    */

    let dialect = GenericDialect {}; // or AnsiDialect
    let sql = "select sum(a) from x where b = 42";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("AST: {:?}", ast);
}

#[derive(PartialEq, Debug, Clone)]
enum Type {
    I32(i32),
    I64(i64),
    String(ByteArray),
    Bool(bool),
    Double(f64),
    Null,
    None,
}

impl Eq for Type {}

impl Hash for Type {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Type::String(a) = self {
            a.data().hash(state);
        } else {
            core::mem::discriminant(self).hash(state);
        }
    }
}

impl From<ByteArray> for Type {
    fn from(value: ByteArray) -> Self {
        Type::String(value)
    }
}

impl From<f64> for Type {
    fn from(value: f64) -> Self {
        Type::Double(value)
    }
}

impl From<bool> for Type {
    fn from(value: bool) -> Self {
        Type::Bool(value)
    }
}

impl From<i32> for Type {
    fn from(value: i32) -> Self {
        Type::I32(value)
    }
}

impl From<i64> for Type {
    fn from(value: i64) -> Self {
        Type::I64(value)
    }
}

struct ParquetColumnReader<T: DataType> {
    column: usize,
    column_reader: Option<ColumnReaderImpl<T>>,
    buffer: Vec<T::T>,
    buffer_pos: usize,
    buffer_start: usize,
    to_skip: usize,
    nulls_count: usize,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
}

enum ReaderResult<'a, T: DataType> {
    EOG,
    Null,
    Some(&'a T::T),
}

impl<T: DataType> ParquetColumnReader<T> {
    fn new(column: usize) -> Self {
        let size = 1024;
        Self {
            column,
            column_reader: None,
            buffer: Vec::with_capacity(size),
            buffer_pos: 0,
            buffer_start: 0,
            to_skip: 0,
            nulls_count: 0,
            def_levels: Vec::with_capacity(size),
            rep_levels: Vec::with_capacity(size),
        }
    }

    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.buffer_start = 0;
        self.buffer_pos = 0;
        self.to_skip = 0;
        self.nulls_count = 0;
        self.buffer.clear();
        self.column_reader = Some(get_typed_column_reader::<T>(
            reader.get_column_reader(self.column).unwrap(),
        ));
    }

    fn position(&self) -> usize {
        self.buffer_start + self.buffer_pos + self.to_skip
    }

    fn set_position(&mut self, position: usize) {
        assert!(position >= self.buffer_start, "{position} {self:?}");
        if position <= self.buffer_start + self.buffer.len() {
            self.nulls_count += self.def_levels[self.buffer_pos..position - self.buffer_start]
                .iter()
                .filter(|&&v| v == 0)
                .count();
            self.buffer_pos = position - self.buffer_start;
            // dbg!(self.nulls_count, self.buffer_pos);
        } else {
            self.nulls_count = 0;
            self.buffer_pos = self.buffer.len();
            self.to_skip = position - self.buffer_start - self.buffer.len();
        }
    }

    fn get(&mut self) -> ReaderResult<T> {
        if self.buffer.is_empty() || self.buffer_pos == self.def_levels.len() {
            if self.to_skip > 0 {
                let result = self
                    .column_reader
                    .as_mut()
                    .expect("msg")
                    .skip_records(self.to_skip)
                    .unwrap();
                self.to_skip -= result;
                self.buffer_start += result;
            }
            self.buffer_start += self.buffer.len();
            self.buffer.clear();
            self.def_levels.clear();
            self.rep_levels.clear();
            let result = self.column_reader.as_mut().expect("msg").read_records(
                self.buffer.capacity(),
                Some(&mut self.def_levels),
                Some(&mut self.rep_levels),
                &mut self.buffer,
            );
            // dbg!(&result, &self.def_levels, &self.rep_levels);
            self.buffer_pos = 0;
            if result.is_err() || result.unwrap().0 == 0 {
                return ReaderResult::EOG;
            }
        }
        assert_eq!(self.to_skip, 0);
        if self.def_levels[self.buffer_pos] == 0 {
            ReaderResult::Null
        } else {
            ReaderResult::Some(&self.buffer[self.buffer_pos - self.nulls_count])
        }
    }
}

fn create_column_reader(
    physical_type: PhysicalType,
    logical_type: Option<LogicalType>,
    number: usize,
) -> Box<dyn ParquetColumn> {
    match (physical_type, logical_type) {
        (PhysicalType::BOOLEAN, None) => Box::new(ParquetColumnReader::<BoolType>::new(number)),
        (PhysicalType::INT32, None) => Box::new(ParquetColumnReader::<Int32Type>::new(number)),
        (PhysicalType::INT64, None) => Box::new(ParquetColumnReader::<Int64Type>::new(number)),
        (PhysicalType::INT96, None) => todo!(),
        (PhysicalType::FLOAT, None) => todo!(),
        (PhysicalType::DOUBLE, None) => Box::new(ParquetColumnReader::<DoubleType>::new(number)),
        (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)) => {
            Box::new(ParquetColumnReader::<ByteArrayType>::new(number))
        }
        (PhysicalType::FIXED_LEN_BYTE_ARRAY, None) => todo!(),
        _ => todo!(),
    }
}

impl<T: DataType> Debug for ParquetColumnReader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringFieldFilter")
            .field("column", &self.column)
            .field("buffer len", &self.buffer.len())
            .field("buffer_start", &self.buffer_start)
            .field("buffer_pos", &self.buffer_pos)
            .field("to_skip", &self.to_skip)
            .finish()
    }
}

trait ParquetColumn {
    fn next_group(&mut self, reader: &dyn RowGroupReader);
    fn get(&mut self, position: usize) -> Option<Type>;
}

impl<T> ParquetColumn for ParquetColumnReader<T>
where
    T: DataType,
    T::T: Into<Type>,
{
    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.next_group(reader);
    }

    fn get(&mut self, position: usize) -> Option<Type> {
        self.set_position(position);
        match self.get() {
            ReaderResult::EOG => None,
            ReaderResult::Null => Some(Type::Null),
            ReaderResult::Some(v) => Some(v.clone().into()),
        }
    }
}

///////////// traits

trait Readers {
    fn get(&mut self, column: usize, position: usize) -> Option<Type>;
    fn row_count(&self) -> usize;
    // fn get_type(&self, column: usize) -> ReaderType;
    // fn column_count(&self) -> usize;
}

trait Filter {
    fn check(&mut self, readers: &mut dyn Readers, position: usize) -> bool;
}

trait Mapper {
    fn map(&mut self, position: usize, readers: &mut dyn Readers) -> Type;
}

trait Processor {
    fn next(&mut self, readers: &mut dyn Readers) -> Option<Vec<Type>>;
}

trait Fold {
    fn fold(&self, current: &mut Type, value: Type);
}

enum ReaderType {
    I32,
    I64,
    Bool,
    Double,
    String,
}

//////////////////  implementations

impl ReaderType {
    fn convert(physical_type: PhysicalType, logical_type: Option<LogicalType>) -> Self {
        match (physical_type, logical_type) {
            (PhysicalType::BOOLEAN, None) => ReaderType::Bool,
            (PhysicalType::INT32, None) => ReaderType::I32,
            (PhysicalType::INT64, None) => ReaderType::I64,
            (PhysicalType::INT96, None) => todo!(),
            (PhysicalType::FLOAT, None) => todo!(),
            (PhysicalType::DOUBLE, None) => ReaderType::Double,
            (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)) => ReaderType::String,
            (PhysicalType::FIXED_LEN_BYTE_ARRAY, None) => todo!(),
            _ => todo!(),
        }
    }
}

struct ParquetReaders {
    names: Vec<String>,
    types: Vec<ReaderType>,
    readers: Vec<Box<dyn ParquetColumn>>,
    group_rows: usize,
    total_rows: usize,
    current_row_group: usize,
    file_reader: Box<dyn FileReader>,
}

impl ParquetReaders {
    fn new(file_reader: Box<dyn FileReader>) -> Self {
        let metadata = file_reader.metadata();
        if metadata.num_row_groups() == 0 {
            panic!()
        }
        let row_group = metadata.row_group(0);
        let group_rows = row_group.num_rows() as usize;
        let mut names = Vec::with_capacity(row_group.num_columns());
        let mut readers = Vec::with_capacity(row_group.num_columns());
        let mut types = Vec::with_capacity(row_group.num_columns());
        for column in row_group.columns().iter() {
            let descr = column.column_descr();
            names.push(descr.name().to_string());
            readers.push(create_column_reader(
                descr.physical_type(),
                descr.logical_type(),
                readers.len(),
            ));
            types.push(ReaderType::convert(
                descr.physical_type(),
                descr.logical_type(),
            ))
        }
        let mut total_rows = group_rows;
        for row_groups in &metadata.row_groups()[1..] {
            total_rows += row_group.num_rows() as usize;
            //TODO: check schema
        }
        {
            let row_group = file_reader.get_row_group(0).unwrap();
            for reader in &mut readers {
                reader.next_group(row_group.deref());
            }
        }

        Self {
            names,
            types,
            readers,
            total_rows: total_rows,
            current_row_group: 0,
            group_rows,
            file_reader,
        }
    }
}

impl Readers for ParquetReaders {
    fn get(&mut self, column: usize, position: usize) -> Option<Type> {
        assert!(position < self.group_rows); // TODO: switch to other groups
        self.readers[column].get(position)
    }
    fn row_count(&self) -> usize {
        self.total_rows
    }
}

struct All;

impl Filter for All {
    fn check(&mut self, readers: &mut dyn Readers, position: usize) -> bool {
        true
    }
}

struct ValueFilter {
    value: Type,
    column: usize,
}

impl ValueFilter {
    fn new(column: usize, value: Type) -> Self {
        Self { column, value }
    }
}

impl Filter for ValueFilter {
    fn check(&mut self, readers: &mut dyn Readers, position: usize) -> bool {
        readers.get(self.column, position).unwrap() == self.value
    }
}

struct And {
    left: Box<dyn Filter>,
    right: Box<dyn Filter>,
}

impl And {
    fn new(left: Box<dyn Filter>, right: Box<dyn Filter>) -> Self {
        Self { left, right }
    }
}

impl Filter for And {
    fn check(&mut self, readers: &mut dyn Readers, position: usize) -> bool {
        self.left.check(readers, position) && self.right.check(readers, position)
    }
}

struct Not {
    filter: Box<dyn Filter>,
}

impl Not {
    fn new(filter: Box<dyn Filter>) -> Self {
        Self { filter }
    }
}

impl Filter for Not {
    fn check(&mut self, readers: &mut dyn Readers, position: usize) -> bool {
        !self.filter.check(readers, position)
    }
}

struct AsIsMapper {
    column: usize,
}

impl Mapper for AsIsMapper {
    fn map(&mut self, position: usize, readers: &mut dyn Readers) -> Type {
        readers.get(self.column, position).unwrap()
    }
}

struct FunctionFold<T: Fn(&Type, Type) -> Type> {
    function: T,
}

impl<T: Fn(&Type, Type) -> Type> FunctionFold<T> {
    fn new(function: T) -> Self {
        Self { function }
    }
}

impl<T: Fn(&Type, Type) -> Type> Fold for FunctionFold<T> {
    fn fold(&self, current: &mut Type, value: Type) {
        *current = (&self.function)(current, value);
    }
}

fn make_sum() -> Box<dyn Fold> {
    Box::new(FunctionFold::new(|a, b| match (a, &b) {
        (Type::None, Type::I64(_)) => b,
        (Type::I64(v1), Type::I64(v2)) => Type::I64(v1 + v2),
        (Type::None, Type::I32(_)) => b,
        (Type::I32(v1), Type::I32(v2)) => Type::I32(v1 + v2),
        _ => panic!("{a:?}, {b:?}"),
    }))
}

struct AsIsProcessor {
    mappers: Vec<Box<dyn Mapper>>,
    filter: Box<dyn Filter>,
    position: usize,
}

impl Processor for AsIsProcessor {
    fn next(&mut self, readers: &mut dyn Readers) -> Option<Vec<Type>> {
        while readers.row_count() > self.position {
            self.position += 1;
            if self.filter.check(readers, self.position - 1) {
                return Some(
                    self.mappers
                        .iter_mut()
                        .map(|m| m.map(self.position - 1, readers))
                        .collect(),
                );
            }
        }
        None
    }
}

fn _example(readers: &mut dyn Readers) {
    let mut p = AsIsProcessor {
        position: 0,
        mappers: vec![
            Box::new(AsIsMapper { column: 0 }),
            Box::new(AsIsMapper { column: 1 }),
            // Box::new(AsIsMapper { column: 2 }),
            // Box::new(AsIsMapper { column: 3 }),
            // Box::new(AsIsMapper { column: 4 }),
        ],
        filter: Box::new(All),
        // filter: Box::new(ValueFilter::new(0, Type::String("Hello".into()))),
        // filter: Box::new(And::new(
        // Box::new(ValueFilter::new(3, Type::String("Video".into()))),
        // Box::new(ValueFilter::new(4, Type::String("PC".into()))),
        // )),
    };
    while let Some(v) = p.next(readers) {
        println!("{v:?}");
    }
}

struct Group {
    filter: Box<dyn Filter>,
    mappers: Vec<Box<dyn Mapper>>,
    keys: Vec<Box<dyn Mapper>>,
    folds: Vec<Box<dyn Fold>>,
    result: Vec<Vec<Type>>,
    position: usize,
}

impl Group {
    fn new(
        filter: Box<dyn Filter>,
        mappers: Vec<Box<dyn Mapper>>,
        keys: Vec<Box<dyn Mapper>>,
        folds: Vec<Box<dyn Fold>>,
    ) -> Self {
        Self {
            filter,
            mappers,
            keys,
            folds,
            result: vec![],
            position: 0,
        }
    }
}

impl Processor for Group {
    fn next(&mut self, readers: &mut dyn Readers) -> Option<Vec<Type>> {
        if self.position < readers.row_count() {
            let mut map = HashMap::new();
            while readers.row_count() > self.position {
                let position = self.position;
                self.position += 1;
                if self.filter.check(readers, position) {
                    let key = self
                        .keys
                        .iter_mut()
                        .map(|k| k.map(position, readers))
                        .collect::<Vec<_>>();
                    let value = map
                        .entry(key)
                        .or_insert_with(|| vec![Type::None; self.folds.len()]);
                    for i in 0..self.mappers.len() {
                        self.folds[i].fold(&mut value[i], self.mappers[i].map(position, readers));
                    }
                }
            }
            self.result = map
                .drain()
                .map(|(mut k, mut v)| {
                    k.append(&mut v);
                    k
                })
                .collect();
        }
        self.result.pop()
    }
}

fn _example2(readers: &mut dyn Readers) {
    let mut p = Group::new(
        Box::new(Not::new(Box::new(ValueFilter::new(1, Type::Null)))),
        vec![Box::new(AsIsMapper { column: 1 })],
        vec![Box::new(AsIsMapper { column: 0 })],
        vec![make_sum()],
    );
    while let Some(v) = p.next(readers) {
        println!("{v:?}");
    }
}

fn main() {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::{fs::File, path::Path};

    // let path = Path::new("sample.parquet");
    // let path = Path::new("flat_1m.parquet");
    let path = Path::new("with_nulls.parquet");
    if let Ok(file) = File::open(&path) {
        let reader = Box::new(SerializedFileReader::new(file).unwrap());
        let mut readers = ParquetReaders::new(reader);
        _example2(&mut readers);
        /*    readers: vec![
                Box::new(ParquetColumnReader::<ByteArrayType>::new(0)),
                Box::new(ParquetColumnReader::<Int64Type>::new(1)),
            ],
            group_rows: 4,
        };
        for row_group in 0..metadata.num_row_groups() {
            let row_group_reader = reader.get_row_group(row_group).unwrap();
            readers.readers[0].next_group(row_group_reader.deref());
            readers.readers[1].next_group(row_group_reader.deref());
            _example(&mut readers);
        }
        */
    }
}

/*
TODO:
    * error handling
    * THINK: intermidaite tables -> need to join readers and Processor
    * tests
    * implement parser
*/
