use std::any::Any;
use std::vec;

use parquet::arrow::arrow_reader::RowGroups;
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

use crate::processing::{ProcessingError, ReaderType};

#[derive(Debug, Clone, Copy)]
pub enum Type<'a> {
    I32(i32),
    I64(i64),
    I96(&'a Int96),
    String(&'a ByteArray), // TODO: find how to get Bytes from parquet type
    Bool(bool),
    Float(f32),
    Double(f64),
    Null,
}

impl<'a> From<&'a i32> for Type<'a> {
    fn from(value: &'a i32) -> Self {
        Type::I32(*value)
    }
}

impl<'a> From<&'a i64> for Type<'a> {
    fn from(value: &'a i64) -> Self {
        Type::I64(*value)
    }
}

impl<'a> From<&'a Int96> for Type<'a> {
    fn from(value: &'a Int96) -> Self {
        Type::I96(value)
    }
}

impl<'a> From<&'a ByteArray> for Type<'a> {
    fn from(value: &'a ByteArray) -> Self {
        Type::String(value)
    }
}

impl<'a> From<&'a bool> for Type<'a> {
    fn from(value: &'a bool) -> Self {
        Type::Bool(*value)
    }
}

impl<'a> From<&'a f32> for Type<'a> {
    fn from(value: &'a f32) -> Self {
        Type::Float(*value)
    }
}

impl<'a> From<&'a f64> for Type<'a> {
    fn from(value: &'a f64) -> Self {
        Type::Double(*value)
    }
}

impl<'a> PartialEq for Type<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I32(l0), Self::I32(r0)) => l0 == r0,
            (Self::I64(l0), Self::I64(r0)) => l0 == r0,
            (Self::I96(l0), Self::I96(r0)) => *l0 == *r0,
            (Self::String(l0), Self::String(r0)) => *l0 == *r0,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Double(l0), Self::Double(r0)) => l0 == r0,
            _ => false,
        }
    }
}

// Stores position inside to efficiently skip nulls,
// otherwise need to count nulls on every read.
// current value at pos - nulls_count and def and rep levels at pos.
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
            pos: 0,
            nulls_count: 0,
            values: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
        }
    }

    fn read(&mut self, column_reader: &mut ColumnReaderImpl<T>) -> Result<bool, ProcessingError> {
        self.nulls_count = 0;
        self.pos = 0;
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
        Ok(result.0 != 0)
    }

    // FIXME: doesn't work with nulls
    fn set_position(&mut self, pos: u32) {
        assert!(pos < self.def_levels.len() as u32);
        self.pos = pos;
    }

    fn position(&self) -> u32 {
        self.pos - 1
    }

    fn contains(&self, position: u32) -> bool {
        (self.def_levels.len() as u32) > position
    }

    fn skip(
        &mut self,
        column_reader: &mut ColumnReaderImpl<T>,
        position: usize,
    ) -> Result<(), ProcessingError> {
        let to_skip = position - self.def_levels.len();
        // println!("skip {to_skip}");
        if to_skip > 0 {
            column_reader.skip_records(to_skip)?;
        }
        Ok(())
    }
}

pub trait Column {
    fn len(&self) -> u32;
    fn unprocessed_values(&self) -> u32;
    fn next<'a>(&'a mut self) -> Type<'a>;
    unsafe fn next_to(&mut self, to: &mut Type<'static>);
    fn get(&self, position: u32) -> Type;
}

// TODO: find a way to implement for the trait for all DataType
macro_rules! impl_parquet_column {
    ($t:ty) => {
        impl Column for ParquetColumn<$t> {
            fn len(&self) -> u32 {
                self.def_levels.len() as u32
            }

            fn unprocessed_values(&self) -> u32 {
                self.def_levels.len() as u32 - self.pos
            }

            fn next<'a>(&'a mut self) -> Type<'a> {
                assert!((self.pos as usize) < self.def_levels.len());
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
            unsafe fn next_to<'a>(&mut self, to: &mut Type<'static>) {
                assert!((self.pos as usize) < self.def_levels.len());
                if unsafe { *self.def_levels.get_unchecked(self.pos as usize) } == 0 {
                    self.pos += 1;
                    self.nulls_count += 1;
                    *to = Type::Null
                } else {
                    let t: Type = (&self.values[(self.pos - self.nulls_count) as usize]).into();
                    unsafe {
                        *to = std::mem::transmute(t);
                    }
                    self.pos += 1;
                }
            }

            fn get<'a>(&'_ self, position: u32) -> Type<'_> {
                // TODO: nulls are not supported
                (&self.values[position as usize]).into()
            }
        }
    };
}

impl_parquet_column!(Int32Type);
impl_parquet_column!(Int64Type);
impl_parquet_column!(Int96Type);
impl_parquet_column!(BoolType);
impl_parquet_column!(ByteArrayType);
impl_parquet_column!(FloatType);
impl_parquet_column!(DoubleType);

impl ReaderType {
    pub fn convert(physical_type: PhysicalType, logical_type: Option<LogicalType>) -> Self {
        match (physical_type, logical_type) {
            (PhysicalType::BOOLEAN, None) => ReaderType::Bool,
            (PhysicalType::INT32, None) => ReaderType::I32,
            (PhysicalType::INT64, None) => ReaderType::I64,
            (PhysicalType::INT96, None) => ReaderType::I96,
            (PhysicalType::FLOAT, None) => ReaderType::Bool,
            (PhysicalType::FLOAT, None) => ReaderType::Float,
            (PhysicalType::DOUBLE, None) => ReaderType::Double,
            (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)) => ReaderType::String,
            (PhysicalType::FIXED_LEN_BYTE_ARRAY, None) => todo!(),
            _ => todo!(),
        }
    }
}

pub struct ParquetReader {
    file_reader: Box<dyn FileReader>,
    column_names: Vec<String>,
    column_types: Vec<ReaderType>,
    row_group_count: usize,
    next_row_group: usize,
    group_reader: Option<Box<dyn RowGroupReader>>,
    column_readers: Vec<Box<dyn Reader>>,
}

impl ParquetReader {
    pub fn new(file_reader: Box<dyn FileReader>) -> Result<Self, ProcessingError> {
        let metadata = file_reader.metadata();
        if metadata.num_row_groups() == 0 {
            return Err(ProcessingError::NoRowGroups);
        }
        let first_row_group = metadata.row_group(0);
        let columns = first_row_group.num_columns();
        let mut column_readers: Vec<Box<dyn Reader>> = Vec::with_capacity(columns);
        let mut column_names = Vec::with_capacity(columns);
        let mut column_types = Vec::with_capacity(columns);
        for column in first_row_group.columns() {
            let descr = column.column_descr();
            column_names.push(descr.name().to_string());
            column_readers.push(create_column_reader(
                descr.physical_type(),
                descr.logical_type(),
                column_readers.len(),
            ));
            column_types.push(ReaderType::convert(
                descr.physical_type(),
                descr.logical_type(),
            ));
        }
        Ok(Self {
            row_group_count: metadata.num_row_groups(),
            file_reader,
            group_reader: None,
            column_readers,
            column_names,
            column_types,
            next_row_group: 0,
        })
    }

    pub fn columns(
        &mut self,
        indices: &[usize],
    ) -> Result<Option<Vec<&mut dyn Column>>, ProcessingError> {
        {
            for &i in indices {
                if !self.column_readers[i].prepare_column_data()? {
                    if self.next_row_group >= self.row_group_count {
                        return Ok(None);
                    }
                    // println!("load group {}", self.next_row_group);
                    let group_reader = self.file_reader.get_row_group(self.next_row_group)?;
                    // TODO clean up the rest, think how to deal with filter and result columns
                    for &i in indices {
                        self.column_readers[i]
                            .next_group(group_reader.as_ref(), self.next_row_group)?;
                        if !self.column_readers[i].prepare_column_data()? {
                            return Ok(None);
                        }
                    }
                    self.next_row_group += 1;
                    break;
                }
            }
            Ok(Some(
                self.column_readers
                    .iter_mut()
                    .enumerate()
                    .filter(|(c, _)| indices.contains(c))
                    .map(|v| v.1.column())
                    .collect(),
            ))
        }
    }

    pub fn position(&self, column: usize) -> (usize, usize) {
        self.column_readers[column].position()
    }

    pub fn get_from_position(
        &mut self,
        columns: &[usize],
        position: (usize, usize),
    ) -> Result<Vec<Type>, ProcessingError> {
        if self.next_row_group != position.0 + 1 {
            Err(ProcessingError::InternalErrorOverPosition)
        } else {
            let row_group = self.file_reader.get_row_group(position.0)?;

            // for &i in columns {
            //     println!("get for col {i}");
            //     vec.push(self.column_readers[i].get_from_position(position, row_group.as_ref())?);
            // }
            self.column_readers
                .iter_mut()
                .enumerate()
                .filter(|(i, _)| columns.contains(i))
                .map(|(_, c)| c.get_from_position(position, row_group.as_ref()))
                .collect()
        }
    }
}

fn create_column_reader(
    physical_type: PhysicalType,
    logical_type: Option<LogicalType>,
    number: usize,
) -> Box<dyn Reader> {
    // println!("create column reader {physical_type:?} {logical_type:?} {number}");
    match (physical_type, logical_type) {
        (PhysicalType::BOOLEAN, None) => Box::new(ParquetColumnReader::<BoolType>::new(number)),
        (PhysicalType::INT32, None) => Box::new(ParquetColumnReader::<Int32Type>::new(number)),
        (PhysicalType::INT64, None) => Box::new(ParquetColumnReader::<Int64Type>::new(number)),
        (PhysicalType::INT96, None) => Box::new(ParquetColumnReader::<Int96Type>::new(number)),
        (PhysicalType::FLOAT, None) => Box::new(ParquetColumnReader::<FloatType>::new(number)),
        (PhysicalType::DOUBLE, None) => Box::new(ParquetColumnReader::<DoubleType>::new(number)),
        (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)) => {
            Box::new(ParquetColumnReader::<ByteArrayType>::new(number))
        }
        (PhysicalType::FIXED_LEN_BYTE_ARRAY, None) => todo!(),
        _ => todo!(),
    }
}

trait Reader {
    fn next_group(
        &mut self,
        reader: &dyn RowGroupReader,
        row_group_number: usize,
    ) -> Result<(), ProcessingError>;
    fn prepare_column_data(&mut self) -> Result<bool, ProcessingError>;
    fn load_column_data(&mut self) -> Result<bool, ProcessingError>;
    // can be called if unprocessed has_data returned true
    fn column(&mut self) -> &mut dyn Column;
    fn position(&self) -> (usize, usize);
    fn get_from_position(
        &mut self,
        position: (usize, usize),
        reader: &dyn RowGroupReader,
    ) -> Result<Type, ProcessingError>;
}

struct ParquetColumnReader<T: DataType> {
    reader: Option<ColumnReaderImpl<T>>,
    column: ParquetColumn<T>,
    column_number: usize,
    column_start_position_in_group: usize,
    row_group_number: usize,
}

impl<T: DataType> ParquetColumnReader<T> {
    pub fn new(column_number: usize) -> Self {
        Self {
            column_number,
            reader: None,
            column: ParquetColumn::new(),
            column_start_position_in_group: usize::max_value(),
            row_group_number: usize::max_value(),
        }
    }
}

impl<T: DataType> Reader for ParquetColumnReader<T>
where
    ParquetColumn<T>: Column,
{
    fn next_group(
        &mut self,
        reader: &dyn RowGroupReader,
        row_group_number: usize,
    ) -> Result<(), ProcessingError> {
        self.reader = Some(get_typed_column_reader(
            reader
                .get_column_reader(self.column_number)
                .map_err(ProcessingError::ParquetError)?,
        ));
        self.column_start_position_in_group = 0;
        self.row_group_number = row_group_number;
        Ok(())
    }
    fn prepare_column_data(&mut self) -> Result<bool, ProcessingError> {
        if self.column.unprocessed_values() != 0 {
            Ok(true)
        } else {
            self.load_column_data()
        }
    }

    fn load_column_data(&mut self) -> Result<bool, ProcessingError> {
        self.column_start_position_in_group += self.column.len() as usize;
        let Some(reader) = self.reader.as_mut() else {
            return Ok(false);
        };
        self.column.read(reader)
    }

    fn column(&mut self) -> &mut dyn Column {
        &mut self.column
    }

    fn position(&self) -> (usize, usize) {
        (
            self.row_group_number,
            self.column_start_position_in_group + (self.column.position() as usize),
        )
    }

    fn get_from_position(
        &mut self,
        position: (usize, usize),
        reader: &dyn RowGroupReader,
    ) -> Result<Type, ProcessingError> {
        if self.row_group_number > position.0 && self.reader.is_some() {
            Err(ProcessingError::InternalErrorOverPosition)
        } else {
            if self.row_group_number < position.0 || self.reader.is_none() {
                // println!(" set new reader for {}", self.column_number);
                self.row_group_number = position.0;
                self.column_start_position_in_group = 0;
                self.reader = Some(get_typed_column_reader(
                    reader
                        .get_column_reader(self.column_number)
                        .map_err(ProcessingError::ParquetError)?,
                ));
                self.column.skip(
                    self.reader
                        .as_mut()
                        .ok_or(ProcessingError::InternalErrorNoReader)?,
                    position.1,
                )?;
                self.column_start_position_in_group = position.1;
                self.load_column_data()?;
                Ok(self.column.get(0))
            } else if self.column_start_position_in_group > position.1 {
                Err(ProcessingError::InternalErrorOverPosition)
            } else if self
                .column
                .contains((position.1 - self.column_start_position_in_group) as u32)
            {
                // println!(" get {}", position.1 - self.column_start_position_in_group);
                Ok(self
                    .column
                    .get((position.1 - self.column_start_position_in_group) as u32))
            } else {
                // println!(" skip and read {}", self.column_start_position_in_group);
                self.column.skip(
                    self.reader
                        .as_mut()
                        .ok_or(ProcessingError::InternalErrorNoReader)?,
                    position.1 - self.column_start_position_in_group,
                )?;
                self.column_start_position_in_group = position.1 - self.column.len() as usize;
                self.load_column_data()?;
                Ok(self.column.get(0))
            }
        }
    }
}

pub trait Filter {
    fn check(&mut self, values: &[Type]) -> bool;
}

pub trait Handler {
    fn handle(&mut self, values: &[Type]) -> bool;
}

pub fn find(
    reader: &mut ParquetReader,
    filter_columns: &[usize],
    filter: &mut Box<dyn Filter>,
) -> Result<(usize, usize), ProcessingError> {
    let mut values: Vec<Type<'static>> = vec![Type::Null; filter_columns.len()];
    loop {
        let Some(mut columns) = reader.columns(&filter_columns)? else {
            // println!("EOF");
            return Ok((usize::max_value(), 0));
        };
        let mut remains = columns
            .iter()
            .map(|c| c.unprocessed_values())
            .min()
            .unwrap();
        for _ in 0..remains {
            for (i, c) in columns.iter_mut().enumerate() {
                unsafe { c.next_to(values.get_unchecked_mut(i)) };
            }
            if filter.check(&values) {
                let p = reader.position(filter_columns[0]);
                // println!("{p:?}");
                return Ok(p);
            }
        }
    }
}

// TODO: second is broken, because it sick back for impression and we find it again, but need to check why skip and read
// maybe just implement get
pub fn get<'a>(
    reader: &'a mut ParquetReader,
    columns: &[usize],
    position: (usize, usize),
) -> Result<Vec<Type<'a>>, ProcessingError> {
    reader.get_from_position(columns, position)
}
