#![allow(dead_code)]
#![allow(unused)]
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;

use parquet::column::reader::{ColumnReaderImpl, get_typed_column_reader};
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DataType, Int64Type};
use parquet::file::reader::RowGroupReader;
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

trait Filter: Debug {
    fn next_group(&mut self, reader: &dyn RowGroupReader);
    fn set_position(&mut self, position: usize);
    fn check(&mut self) -> Option<bool>;
    fn next(&mut self) -> Option<usize>;
}

#[derive(PartialEq, Debug, Clone)]
enum Type {
    I64(i64),
    String(ByteArray),
    Bool(bool),
    Double(f64),
    Null,
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

impl From<Option<ByteArray>> for Type {
    fn from(value: Option<ByteArray>) -> Self {
        if let Some(value) = value {
            Type::String(value)
        } else {
            Type::Null
        }
    }
}

impl From<Option<f64>> for Type {
    fn from(value: Option<f64>) -> Self {
        if let Some(value) = value {
            Type::Double(value)
        } else {
            Type::Null
        }
    }
}

impl From<Option<bool>> for Type {
    fn from(value: Option<bool>) -> Self {
        if let Some(value) = value {
            Type::Bool(value)
        } else {
            Type::Null
        }
    }
}

impl From<Option<i64>> for Type {
    fn from(value: Option<i64>) -> Self {
        if let Some(value) = value {
            Type::I64(value)
        } else {
            Type::Null
        }
    }
}

struct ParquetColumnReader<T: DataType> {
    column: usize,
    column_reader: Option<ColumnReaderImpl<T>>,
    buffer: Vec<T::T>,
    buffer_pos: usize,
    buffer_start: usize,
    to_skip: usize,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
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
            def_levels: Vec::with_capacity(size),
            rep_levels: Vec::with_capacity(size),
        }
    }

    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.buffer_start = 0;
        self.buffer_pos = 0;
        self.to_skip = 0;
        self.buffer.clear();
        match reader.get_column_reader(self.column).unwrap() {
            parquet::column::reader::ColumnReader::BoolColumnReader(generic_column_reader) => {
                println!("bool")
            }
            parquet::column::reader::ColumnReader::Int32ColumnReader(generic_column_reader) => {
                println!("i32")
            }
            parquet::column::reader::ColumnReader::Int64ColumnReader(generic_column_reader) => {
                println!("i64")
            }
            parquet::column::reader::ColumnReader::Int96ColumnReader(generic_column_reader) => {
                println!("i96")
            }
            parquet::column::reader::ColumnReader::FloatColumnReader(generic_column_reader) => {
                println!("fload")
            }
            parquet::column::reader::ColumnReader::DoubleColumnReader(generic_column_reader) => {
                println!("doub")
            }
            parquet::column::reader::ColumnReader::ByteArrayColumnReader(generic_column_reader) => {
                println!("var")
            }
            parquet::column::reader::ColumnReader::FixedLenByteArrayColumnReader(
                generic_column_reader,
            ) => println!("fix"),
        }
        self.column_reader = Some(get_typed_column_reader::<T>(
            reader.get_column_reader(self.column).unwrap(),
        ));
    }

    fn position(&self) -> usize {
        self.buffer_start + self.buffer_pos + self.to_skip
    }

    fn set_position(&mut self, position: usize) {
        assert!(position >= self.buffer_start, "{position} {self:?}");
        if position < self.buffer_start + self.buffer.len() {
            self.buffer_pos = position - self.buffer_start;
        } else {
            self.buffer_pos = self.buffer.len();
            self.to_skip = position - self.buffer_start - self.buffer.len();
        }
    }

    fn get(&mut self) -> Option<&T::T> {
        if self.buffer.is_empty() || self.buffer_pos == self.buffer.len() {
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
            self.buffer_pos = 0;
            if result.is_err() || result.unwrap().0 == 0 {
                return None;
            }
        }
        assert_eq!(self.to_skip, 0);
        Some(&self.buffer[self.buffer_pos])
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

struct FieldFilter<T: DataType> {
    value: T::T,
    reader: ParquetColumnReader<T>,
}

impl<T: DataType> FieldFilter<T> {
    fn new(column: usize, value: T::T) -> Self {
        Self {
            value,
            reader: ParquetColumnReader::new(column),
        }
    }
}

impl<T: DataType> Debug for FieldFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringFieldFilter")
            .field("value", &self.value)
            .field("reader", &self.reader)
            .finish()
    }
}

impl<T: DataType> Filter for FieldFilter<T> {
    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.reader.next_group(reader);
    }

    fn set_position(&mut self, position: usize) {
        self.reader.set_position(position);
    }

    fn check(&mut self) -> Option<bool> {
        self.reader.get().map(|v| *v == self.value)
    }

    fn next(&mut self) -> Option<usize> {
        while let Some(found) = self.check() {
            let position = self.reader.position();
            self.reader.set_position(position + 1);
            if found {
                return Some(position);
            }
        }
        None
    }
}

#[derive(Debug)]
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
    fn next(&mut self) -> Option<usize> {
        while let Some(position) = self.left.next() {
            self.right.set_position(position);
            let result = self.right.check();
            if let Some(true) = result {
                return Some(position);
            }
        }
        None
    }

    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.left.next_group(reader);
        self.right.next_group(reader);
    }

    fn set_position(&mut self, position: usize) {
        self.left.set_position(position);
        self.right.set_position(position);
    }

    fn check(&mut self) -> Option<bool> {
        if self.left.check()? {
            self.right.check()
        } else {
            Some(false)
        }
    }
}

trait Handler {
    fn next_group(&mut self, reader: &dyn RowGroupReader);
    fn handle(&mut self, position: usize);
    fn result(&mut self) -> Type;
}

impl<T> Handler for ParquetColumnReader<T>
where
    T: DataType,
    Option<T::T>: Into<Type>,
{
    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.next_group(reader);
    }

    fn handle(&mut self, position: usize) {
        self.set_position(position);
    }

    fn result(&mut self) -> Type {
        self.get().map(|v| v.clone()).into()
    }
}

struct SingleValue<T: DataType, S>
where
    S: Fn(&T::T, &T::T) -> T::T,
{
    value: Option<T::T>,
    reader: ParquetColumnReader<T>,
    select: S,
}

impl<T: DataType, S> SingleValue<T, S>
where
    S: Fn(&T::T, &T::T) -> T::T,
{
    fn new(column: usize, select: S) -> Self {
        Self {
            value: None,
            reader: ParquetColumnReader::new(column),
            select,
        }
    }
}

impl<T: DataType, S> Handler for SingleValue<T, S>
where
    S: Fn(&T::T, &T::T) -> T::T,
    Option<T::T>: Into<Type>,
{
    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.reader.next_group(reader);
    }

    fn handle(&mut self, position: usize) {
        self.reader.set_position(position);
        match (self.reader.get(), &self.value) {
            (Some(n), Some(c)) => self.value = Some((self.select)(n, c)),
            (Some(n), None) => self.value = Some(n.clone()),
            _ => {}
        }
    }

    fn result(&mut self) -> Type {
        self.value.clone().into()
    }
}

trait Fold {
    fn fold(&self, result: &mut Type, value: Type);
}

impl<T: Fn(&mut Type, Type)> Fold for T {
    fn fold(&self, result: &mut Type, value: Type) {
        self(result, value)
    }
}

struct Sum;
impl Fold for Sum {
    fn fold(&self, result: &mut Type, value: Type) {
        match (result, value) {
            (Type::I64(r), Type::I64(v)) => *r += v,
            (r @ Type::Null, v @ Type::I64(_)) => *r = v,
            _ => panic!(),
        }
    }
}

struct Group {
    keys: Vec<Box<dyn Handler>>,
    readers: Vec<Box<dyn Handler>>,
    folds: Vec<Box<dyn Fold>>,
    values: HashMap<Vec<Type>, Vec<Type>>,
}

impl Group {
    fn new(
        keys: Vec<Box<dyn Handler>>,
        readers: Vec<Box<dyn Handler>>,
        folds: Vec<Box<dyn Fold>>,
    ) -> Self {
        Self {
            keys,
            readers,
            folds,
            values: HashMap::new(),
        }
    }
}

impl Handler for Group {
    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        for r in &mut self.readers {
            r.next_group(reader);
        }
        for v in &mut self.keys {
            v.next_group(reader);
        }
    }

    fn handle(&mut self, position: usize) {
        let key = self
            .keys
            .iter_mut()
            .map(|k| {
                k.handle(position);
                k.result()
            })
            .collect::<Vec<_>>();
        let value = self
            .values
            .entry(key)
            .or_insert_with(|| vec![Type::Null; self.folds.len()]);
        for (i, fold) in self.folds.iter().enumerate() {
            let reader = &mut self.readers[i];
            reader.handle(position);
            fold.fold(&mut value[i], reader.result());
        }
    }

    fn result(&mut self) -> Type {
        for (k, v) in &self.values {
            println!("{:?} {:?}", k, v);
        }
        todo!()
    }
}

fn main() {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::{fs::File, path::Path};

    // let path = Path::new("sample.parquet");
    let path = Path::new("flat_1m.parquet");
    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file).unwrap();

        // let mut sff = StringFieldFilter::new(25, "KG".into());
        let mut filter = And::new(
            Box::new(FieldFilter::<Int64Type>::new(36, 42)),
            // Box::new(FieldFilter::<ByteArrayType>::new(25, "US".into())),
            Box::new(FieldFilter::<ByteArrayType>::new(
                24,
                "BROWSERTYPE_OTHER".into(),
            )),
        );
        // filter.skipn(1139058);
        let mut handler = ParquetColumnReader::<BoolType>::new(8); // 37 - double
        // let mut handler = SingleValue::<ByteArrayType, _>::new(0, |a, b| {
        //     if a.data() <= b.data() {
        //         a.clone()
        //     } else {
        //         b.clone()
        //     }
        // });
        let metadata = reader.metadata();
        let mut c = 0;
        for row_group in 0..metadata.num_row_groups() {
            let row_group_reader = reader.get_row_group(row_group).unwrap();
            filter.next_group(row_group_reader.deref());
            handler.next_group(row_group_reader.deref());
            while let Some(v) = filter.next() {
                c += 1;
                handler.handle(v);
                println!("{} {:?}", v + 1, handler.result());
            }
            println!("{c}\n");
            handler.result();
            c = 0;
            println!("check&skip");
            filter.next_group(row_group_reader.deref());
            let mut v = 0;
            while let Some(res) = filter.check() {
                if res {
                    c += 1;
                    // println!("{}", v + 1);
                    // break;
                }
                v += 2;
                filter.set_position(v);
                // dbg!(&filter);
                // break;
            }
        }
        println!("{c}");

        let mut handler = Group::new(
            vec![Box::new(ParquetColumnReader::<ByteArrayType>::new(0))],
            vec![Box::new(ParquetColumnReader::<Int64Type>::new(36))],
            vec![Box::new(Sum)],
        );
        /*
                let mut filter = FieldFilter::<ByteArrayType>::new(24, "BROWSERTYPE_OTHER".into());
                for row_group in 0..metadata.num_row_groups() {
                    let row_group_reader = reader.get_row_group(row_group).unwrap();
                    filter.next_group(row_group_reader.deref());
                    handler.next_group(row_group_reader.deref());
                    while let Some(v) = filter.next() {
                        c += 1;
                        handler.handle(v);
                    }
                }
                handler.result();
        */
        for row_group in 0..metadata.num_row_groups() {
            let row_group_reader = reader.get_row_group(row_group).unwrap();
            handler.next_group(row_group_reader.deref());
            for v in 0..row_group_reader.metadata().num_rows() as usize {
                handler.handle(v);
            }
        }
        handler.result();
    }
}

/*
TODO:
* group by
    * result() -> ? is it different from Handler?
    * think about layer between read and reulst, there is no need for handler?
     Q: do we need to divide handler and result? two pattersn of use in one interface:
        handler, result, hadnle, result, ... -> for functions like A -> B
        handle, ...., handler, result for Fold
        looks liket the second is more like Fold if Type is a Vec<Type>
    A: need to extract reader from filter and Agg interface for group

* intermidaite tables, is it Type? if so, can we convert input table to Type too? but async

]=
* error handling
* tests
* implement parser
*/
