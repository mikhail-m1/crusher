use std::fmt::Debug;
use std::ops::Deref;

use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{ByteArray, ByteArrayType};
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
    fn skip(&mut self, count: usize);
    fn check(&mut self) -> Option<bool>;
    fn next(&mut self) -> Option<usize>;
}

struct StringFieldFilter {
    value: String,
    column: usize,
    column_reader: Option<ColumnReaderImpl<ByteArrayType>>,
    buffer: Vec<ByteArray>,
    buffer_pos: usize,
    buffer_start: usize,
    to_skip: usize,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
}

impl StringFieldFilter {
    fn new(column: usize, value: String) -> Self {
        let size = 1024;
        Self {
            value,
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
}

impl Debug for StringFieldFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringFieldFilter")
            .field("value", &self.value)
            .field("column", &self.column)
            .field("buffer len", &self.buffer.len())
            .field("buffer_start", &self.buffer_start)
            .field("buffer_pos", &self.buffer_pos)
            .field("to_skip", &self.to_skip)
            .finish()
    }
}

impl Filter for StringFieldFilter {
    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.buffer_start = 0;
        self.buffer_pos = 0;
        self.to_skip = 0;
        self.buffer.clear();
        if let ColumnReader::ByteArrayColumnReader(r) =
            reader.get_column_reader(self.column).unwrap()
        {
            self.column_reader = Some(r);
        } else {
            panic!()
        }
    }

    fn skip(&mut self, mut count: usize) {
        if !self.buffer.is_empty() && self.buffer_pos < self.buffer.len() {
            let reduce = count.min(self.buffer.len() - self.buffer_pos);
            self.buffer_pos += reduce;
            count -= reduce
        }
        self.to_skip += count;
    }

    fn check(&mut self) -> Option<bool> {
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
        Some(self.buffer[self.buffer_pos].data() == self.value.as_bytes())
    }

    fn next(&mut self) -> Option<usize> {
        while let Some(found) = self.check() {
            if found {
                let res = self.buffer_start + self.buffer_pos;
                self.buffer_pos += 1;
                return Some(res);
            }
            self.buffer_pos += 1;
        }
        None
    }
}

#[derive(Debug)]
struct And {
    left: Box<dyn Filter>,
    right: Box<dyn Filter>,
    position: isize,
}

impl And {
    fn new(left: Box<dyn Filter>, right: Box<dyn Filter>) -> Self {
        Self {
            left,
            right,
            position: -1,
        }
    }
}

impl Filter for And {
    fn next(&mut self) -> Option<usize> {
        while let Some(position) = self.left.next() {
            let to_skip = position as isize - self.position - 1;
            if to_skip > 0 {
                self.right.skip(to_skip as usize);
            }
            let result = self.right.check();
            self.right.skip(1);
            self.position = position as isize;
            if let Some(true) = result {
                return Some(position);
            }
        }
        None
    }

    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.left.next_group(reader);
        self.right.next_group(reader);
        self.position = -1;
    }

    fn skip(&mut self, count: usize) {
        self.left.skip(count);
        self.right.skip(count);
        self.position += count as isize;
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
    fn handle(&mut self, row: usize);
    fn result(); // TODO -> enum Value {String, Int64...}
}

struct Print {
    column: usize,
    column_reader: Option<ColumnReaderImpl<ByteArrayType>>,
    buffer: Vec<ByteArray>,
    buffer_start: usize,
    to_skip: usize,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
}

impl Handler for Print {
    fn handle(&mut self, row: usize) {
        todo!()
    }

    fn result() {
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
            Box::new(StringFieldFilter::new(25, "US".into())),
            Box::new(StringFieldFilter::new(24, "BROWSERTYPE_OTHER".into())),
        );
        // filter.skipn(1139058);
        let metadata = reader.metadata();
        let mut c = 0;
        for row_group in 0..metadata.num_row_groups() {
            let row_group_reader = reader.get_row_group(row_group).unwrap();
            filter.next_group(row_group_reader.deref());
            while let Some(v) = filter.next() {
                c += 1;
                // println!("{}", v + 1);
            }
            println!("{c}\n");
            c = 0;
            println!("check&skip");
            filter.next_group(row_group_reader.deref());
            let mut v = 14080 + 1;
            while let Some(res) = filter.check() {
                if res {
                    c += 1;
                    // println!("{}", v);
                    // break;
                }
                v += 1;
                filter.skip(1);
                // dbg!(&filter);
                // break;
            }
        }
        println!("{c}");
    }
}
