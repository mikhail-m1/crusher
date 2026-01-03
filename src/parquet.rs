use super::processing::*;
use parquet::basic::{LogicalType, StringType, Type as PhysicalType};
use parquet::column::reader::{get_typed_column_reader, ColumnReaderImpl};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType, DoubleType, FloatType, Int32Type, Int64Type,
    Int96, Int96Type,
};
use parquet::errors::ParquetError;
use parquet::file::reader::{self, FileReader, RowGroupReader};
use std::fmt::Debug;
use std::fs::ReadDir;
use std::ops::Deref;

impl From<ByteArray> for Type {
    fn from(value: ByteArray) -> Self {
        Type::String(value)
    }
}

impl From<f32> for Type {
    fn from(value: f32) -> Self {
        Type::Float(value)
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

impl From<Int96> for Type {
    fn from(value: Int96) -> Self {
        let data = value.data();
        Type::I128(data[0] as i128 | data[1] as i128 >> 32 | data[2] as i128 >> 64)
    }
}

pub struct ParquetColumnReader<T: DataType> {
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

pub enum Output<'a, T: DataType> {
    Null,
    Some(&'a T::T),
}

impl<T: DataType> ParquetColumnReader<T> {
    pub fn new(column: usize) -> Self {
        let size = 10240;
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

    pub fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.buffer_start = 0;
        self.buffer_pos = 0;
        self.to_skip = 0;
        self.nulls_count = 0;
        self.buffer.clear();
        self.column_reader = Some(get_typed_column_reader::<T>(
            reader.get_column_reader(self.column).unwrap(),
        ));
    }

    pub fn position(&self) -> usize {
        self.buffer_start + self.buffer_pos + self.to_skip
    }

    pub fn set_position(&mut self, position: usize) {
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

    pub fn get(&'_ mut self) -> Result<Output<'_, T>, ProcessingError> {
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
            let result = self
                .column_reader
                .as_mut()
                .expect("msg")
                .read_records(
                    self.buffer.capacity(),
                    Some(&mut self.def_levels),
                    Some(&mut self.rep_levels),
                    &mut self.buffer,
                )
                .map_err(ProcessingError::ParquetError)?;
            // dbg!(&result, &self.def_levels, &self.rep_levels);
            self.buffer_pos = 0;
            assert_ne!(result.0, 0, "Read outside of group {self:?}");
        }
        assert_eq!(self.to_skip, 0);
        if self.def_levels[self.buffer_pos] == 0 {
            // BUG? should the nulls_count be incremented?
            Ok(Output::Null)
        } else {
            Ok(Output::Some(
                &self.buffer[self.buffer_pos - self.nulls_count],
            ))
        }
    }

    pub fn get2(&'_ mut self, output: &mut T::T) -> Result<bool, ProcessingError> {
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
            let result = self
                .column_reader
                .as_mut()
                .expect("msg")
                .read_records(
                    self.buffer.capacity(),
                    Some(&mut self.def_levels),
                    Some(&mut self.rep_levels),
                    &mut self.buffer,
                )
                .map_err(ProcessingError::ParquetError)?;
            // dbg!(&result, &self.def_levels, &self.rep_levels);
            self.buffer_pos = 0;
            assert_ne!(result.0, 0, "Read outside of group {self:?}");
        }
        assert_eq!(self.to_skip, 0);
        if self.def_levels[self.buffer_pos] == 0 {
            Ok(false)
        } else {
            *output = self.buffer[self.buffer_pos - self.nulls_count].clone();
            Ok(true)
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
    fn get(&mut self, position: usize) -> Result<Type, ProcessingError>;
    fn get_into(&mut self, position: usize, result: &mut Type) -> Result<(), ProcessingError>;
}

impl<T> ParquetColumn for ParquetColumnReader<T>
where
    T: DataType,
    T::T: Into<Type>,
{
    fn next_group(&mut self, reader: &dyn RowGroupReader) {
        self.next_group(reader);
    }

    fn get(&mut self, position: usize) -> Result<Type, ProcessingError> {
        self.set_position(position);
        match self.get()? {
            Output::Null => Ok(Type::Null),
            Output::Some(v) => Ok(v.clone().into()),
        }
    }

    //#[cfg_attr(feature = "hotpath", hotpath::measure())]
    fn get_into(&mut self, position: usize, result: &mut Type) -> Result<(), ProcessingError> {
        self.set_position(position);
        match self.get()? {
            Output::Null => *result = Type::Null,
            Output::Some(v) => *result = v.clone().into(),
        }
        Ok(())
    }
}

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

pub struct ParquetReaders {
    names: Vec<String>,
    types: Vec<ReaderType>,
    readers: Vec<Box<dyn ParquetColumn>>,
    rows_in_group: Vec<usize>,
    total_rows: usize,
    current_row_group: usize,
    file_reader: Box<dyn FileReader>,
    values: Vec<Type>,
    current: usize,
}

impl ParquetReaders {
    pub fn new(file_reader: Box<dyn FileReader>) -> Self {
        let metadata = file_reader.metadata();
        if metadata.num_row_groups() == 0 {
            panic!()
        }
        let row_group = metadata.row_group(0);
        let mut rows_in_group = Vec::with_capacity(row_group.num_columns());
        let mut names = Vec::with_capacity(row_group.num_columns());
        let mut readers = Vec::with_capacity(row_group.num_columns());
        let mut types = Vec::with_capacity(row_group.num_columns());
        rows_in_group.push(row_group.num_rows() as usize);
        let group_rows = row_group.num_rows() as usize;
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
        let mut total_rows = row_group.num_rows() as usize;

        for row_group in &metadata.row_groups()[1..] {
            rows_in_group.push(rows_in_group.last().unwrap() + row_group.num_rows() as usize);
            total_rows += row_group.num_rows() as usize;
            //TODO: check schema
        }
        let mut result = Self {
            names,
            types,
            readers,
            total_rows,
            current_row_group: 0,
            rows_in_group,
            file_reader,
            values: vec![],
            current: 0,
        };
        result.init_row_group(0);
        result
    }

    pub fn init_row_group(&mut self, row_group: usize) {
        let row_group = self.file_reader.get_row_group(row_group).unwrap();
        for reader in &mut self.readers {
            reader.next_group(row_group.deref());
        }
    }
}

impl Readers for ParquetReaders {
    fn get(&mut self, column: usize, position: usize) -> Result<Type, ProcessingError> {
        if position >= self.rows_in_group[self.current_row_group] {
            self.current_row_group += 1;
            self.init_row_group(self.current_row_group);
        }
        let offset = if self.current_row_group == 0 {
            0
        } else {
            self.rows_in_group[self.current_row_group - 1]
        };
        self.readers[column].get(position - offset)
    }
    fn row_count(&self) -> usize {
        self.total_rows
    }

    fn get_type(&self, column: usize) -> ReaderType {
        todo!()
    }

    fn column_count(&self) -> usize {
        self.names.len()
    }

    fn find(&self, name: &str) -> Option<usize> {
        self.names
            .iter()
            .enumerate()
            .find(|(_, v)| *v == name)
            .map(|(i, _)| i)
    }
}

impl Source for ParquetReaders {
    // #[cfg_attr(feature = "hotpath", hotpath::measure())]
    fn findx(
        &mut self,
        filter_columns: &[usize],
        filter: &mut Box<dyn Filter>,
    ) -> Result<bool, ProcessingError> {
        if self.values.len() < filter_columns.len() {
            self.values.resize(filter_columns.len(), Type::None);
        }

        while self.current < self.total_rows {
            // can be optimised (put outside)
            if self.current >= self.rows_in_group[self.current_row_group] {
                self.current_row_group += 1;
                self.init_row_group(self.current_row_group);
            }
            let offset = if self.current_row_group == 0 {
                0
            } else {
                self.rows_in_group[self.current_row_group - 1]
            };

            let to = self.rows_in_group[self.current_row_group];
            // hotpath::measure_block!("loop", {
            for p in (self.current - offset)..(to - offset) {
                // hotpath::measure_block!("loop get", {
                for (i, &column) in filter_columns.iter().enumerate() {
                    self.readers[column].get_into(p, &mut self.values[i])?;
                }
                // });
                // hotpath::measure_block!("loop check", {
                //self.current += 1;
                if filter.check(&self.values) {
                    self.current = p + offset + 1;
                    return Ok(true);
                }
                // });
            }
            // });
            self.current = to;
        }
        Ok(false)
    }
    fn get(&mut self, columns: &[usize]) -> Result<&[Type], ProcessingError> {
        if self.values.len() < columns.len() {
            self.values.resize(columns.len(), Type::None);
        }
        todo!();
        Ok(&self.values)
    }
    /*
     fn value(&mut self, row: usize, column: usize) -> Result<&Type, ProcessingError> {
         if row >= self.rows_in_group[self.current_row_group] {
             self.current_row_group += 1;
             self.init_row_group(self.current_row_group);
         }
         let offset = if self.current_row_group == 0 {
             0
         } else {
             self.rows_in_group[self.current_row_group - 1]
         };
         self.readers[column].get_ref(row - offset)
    }
     fn rows(&mut self) -> Result<usize, ProcessingError> {
         Ok(self.total_rows)
     }*/
}
