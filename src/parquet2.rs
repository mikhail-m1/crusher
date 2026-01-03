use std::any::Any;

use parquet::basic::{LogicalType, StringType, Type as PhysicalType};
use parquet::column;
use parquet::column::reader::{get_typed_column_reader, ColumnReader, ColumnReaderImpl};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType, DoubleType, FloatType, Int32Type, Int64Type,
    Int96, Int96Type,
};
use parquet::errors::ParquetError;
use parquet::file::reader::{self, FileReader, RowGroupReader};
use parquet::format::RowGroup;
use parquet::record::Row;
use recycle_vec::VecExt;

use crate::processing::ProcessingError;

#[derive(Debug, Clone, Copy)]
pub enum Type<'a> {
    I32(i32),
    I64(i64),
    I128(&'a i128),
    String(&'a ByteArray), // TODO: find how to get Bytes from parquet type
    Bool(bool),
    Float(f32),
    Double(f64),
    Null,
}

impl<'a> From<&'a i64> for Type<'a> {
    fn from(value: &'a i64) -> Self {
        Type::I64(*value)
    }
}

impl<'a> PartialEq for Type<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I32(l0), Self::I32(r0)) => l0 == r0,
            (Self::I64(l0), Self::I64(r0)) => l0 == r0,
            (Self::I128(l0), Self::I128(r0)) => *l0 == *r0,
            (Self::String(l0), Self::String(r0)) => *l0 == *r0,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Double(l0), Self::Double(r0)) => l0 == r0,
            _ => false,
        }
    }
}

// Stores position inside to efficiently skip nulls,
// otherwise need to count nulls on every read
pub struct ParquetColumn<T: DataType> {
    values: Vec<T::T>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
    pos: u32,
    nulls_count: u32,
}

impl<T: DataType> ParquetColumn<T> {
    pub fn new() -> Self {
        let capacity = 10240;
        Self {
            values: Vec::with_capacity(capacity),
            nulls_count: 0,
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
            pos: 0,
        }
    }

    fn read(&mut self, column_reader: &mut ColumnReaderImpl<T>) -> Result<bool, ProcessingError> {
        self.values.clear();
        self.def_levels.clear();
        self.rep_levels.clear();
        let result = column_reader
            .read_records(
                self.values.capacity(),
                Some(&mut self.def_levels),
                Some(&mut self.rep_levels),
                &mut self.values,
            )
            .map_err(ProcessingError::ParquetError)?;
        self.nulls_count = 0;
        self.pos = 0;
        Ok(result.0 != 0)
    }
}

// TODO maybe add per type functions or something else to prevent dyn call
pub trait Column {
    fn len(&self) -> u32;
    fn next<'a>(&'a mut self) -> Type<'a>;
    fn skip(&mut self, n: u32);
}

impl Column for ParquetColumn<Int64Type> {
    fn len(&self) -> u32 {
        self.def_levels.len() as u32 - self.pos
    }

    fn next<'a>(&'a mut self) -> Type<'a> {
        if self.def_levels[self.pos as usize] == 0 {
            self.pos += 1;
            self.nulls_count += 1;
            Type::Null
        } else {
            let res = (&self.values[(self.pos - self.nulls_count) as usize]).into();
            self.pos += 1;
            res
        }
    }

    fn skip(&mut self, n: u32) {
        todo!()
    }
}

/* Find a way how to fix it,
impl<'a, T> Column for ColumnData<T>
where
    T: DataType,
    &'a T::T: Into<Type2<'a>>,
{
    fn len(&self) -> u32 {
        self.def_levels.len() as u32
    }

    fn next<'b: 'a>(&'a mut self) -> Type2<'a> {
        if self.def_levels[self.pos as usize] == 0 {
            self.pos += 1;
            self.nulls_count += 1;
            Type2::Null
        } else {
            let res = (&self.values[(self.pos - self.nulls_count) as usize]).into();
            self.pos += 1;
            res
        }
    }

    fn skip(&mut self, n: u32) {
        todo!()
    }
}
*/

#[derive(Clone, Copy)]
enum ColumnKind {
    I32,
    I64,
}
pub struct ParquetReader {
    file_reader: Box<dyn FileReader>,
    column_names: Vec<String>,
    column_types: Vec<ColumnKind>,
    rows_in_group: Vec<usize>,
    total_rows: usize,
    current_row_group: usize,
    group_reader: Option<Box<dyn RowGroupReader>>,
    column_readers: Vec<Box<dyn Reader>>,
}

impl ParquetReader {
    pub fn new(file_reader: Box<dyn FileReader>) -> Self {
        let mut column_readers: Vec<Box<dyn Reader>> = vec![];
        for _ in 0..35 {
            column_readers.push(Box::new(ParquetColumnReader::<Int64Type>::new()))
        }
        let mut self_ = Self {
            file_reader,
            group_reader: None,
            column_readers,
            column_names: vec![],
            column_types: vec![],
            rows_in_group: vec![0],
            total_rows: 1000,
            current_row_group: 0,
        };
        {
            let group_reader = self_.file_reader.get_row_group(0).unwrap();
            for reader in &mut self_.column_readers {
                reader.next_group(group_reader.as_ref().get_column_reader(36).unwrap());
            }
        }
        self_
    }

    // pub fn column(&mut self, column: usize) -> &mut dyn Column {
    //     self.column_readers[column].column()
    // }

    pub fn columns(
        &mut self,
        indices: &[usize],
    ) -> Result<Option<Vec<&mut dyn Column>>, ProcessingError> {
        //FIXME filter
        self.column_readers.iter_mut().map(|v| v.column()).collect()
    }

    fn new_column_reader(kind: ColumnKind) -> Box<dyn Reader> {
        todo!()
    }

    fn init<T: DataType>(&mut self, row_group: usize) {
        let row_group = self.file_reader.get_row_group(row_group).unwrap();
        // reader.next_group(row_group.deref());
    }
}

trait Reader {
    fn next_group(&mut self, reader: ColumnReader);
    fn column(&mut self) -> Result<Option<&mut dyn Column>, ProcessingError>;
}

struct ParquetColumnReader<T: DataType> {
    reader: Option<ColumnReaderImpl<T>>,
    column: ParquetColumn<T>,
    position: usize,
}

impl<T: DataType> ParquetColumnReader<T> {
    pub fn new() -> Self {
        Self {
            position: 0,
            reader: None,
            column: ParquetColumn::new(),
        }
    }
}

impl<T: DataType> Reader for ParquetColumnReader<T>
where
    ParquetColumn<T>: Column,
{
    fn next_group(&mut self, reader: ColumnReader) {
        self.reader = Some(get_typed_column_reader::<T>(reader));
    }

    fn column(&mut self) -> Result<Option<&mut dyn Column>, ProcessingError> {
        if self.column.len() == 0 {
            // TODO: error code support
            if !self.column.read(self.reader.as_mut().unwrap())? {
                return Ok(None);
            }
        }
        Ok(Some(&mut self.column))
    }
}

pub trait Filter {
    fn check(&mut self, values: &[Type]) -> bool;
}

pub fn find(
    reader: &mut ParquetReader,
    filter_columns: &[usize],
    filter: &mut Box<dyn Filter>, // need mut here?
) -> Result<bool, ProcessingError> {
    loop {
        let Some(mut columns) = reader.columns(&filter_columns)? else {
            return Ok(false);
        };
        let mut remains = columns.iter().map(|c| c.len()).min().unwrap();
        if remains == 0 {
            // FIXME: remove
            return Ok(false);
        }
        let mut holder: Vec<Type<'static>> = Vec::with_capacity(columns.len());
        for _ in 0..remains {
            let mut values = holder.recycle();
            values.extend(columns.iter_mut().map(|c| c.next()));
            // let values = columns.iter_mut().map(|c| c.next()).collect::<Vec<_>>();
            if filter.check(&values) {
                return Ok(true);
            }
            holder = values.recycle();
        }
    }
}
