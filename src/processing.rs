use super::sql::ParsingError;
use bytes::Bytes;
use parquet::data_type::ByteArray;
use parquet::errors::ParquetError;
use std::cell::LazyCell;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProcessingError {
    #[error("Parquet error")]
    ParquetError(#[from] ParquetError),
    #[error("Parsing error")]
    Parsing(#[from] ParsingError),
    #[error("Invaid buffer type")]
    InvalidBufferType,
    #[error("No row groups found in the file")]
    NoRowGroups,
    #[error("Internal Error, column already over position")]
    InternalErrorOverPosition,
    #[error("Internal Error, no reader")]
    InternalErrorNoReader,
}

#[derive(PartialEq, Debug, Clone)]
pub enum Type {
    I32(i32),
    I64(i64),
    I128(i128),
    String(ByteArray), // TODO: find how to get Bytes from parquet type
    Bool(bool),
    Float(f32),
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

///////////// traits

pub trait Filter {
    fn check(&mut self, values: &[Type]) -> bool;
}

pub trait Source {
    fn findx(
        &mut self,
        filter_columns: &[usize],
        filter: &mut Box<dyn Filter>, // need mut here?
    ) -> Result<bool, ProcessingError>;
    fn get(&mut self, columns: &[usize]) -> Result<&[Type], ProcessingError>;
    //fn rows(&mut self) -> Result<usize, ProcessingError>;
    //TODO:
    // fn column_by_name(&self, name: &str) -> Option<usize>;
    // fn get_type(&self, column: usize) -> ReaderType;
    // fn column_count(&self) -> usize;
    //TODO2:
    // fn find_next_row_with_one_of(&[Type]) -> usize //can be optimized
}

pub trait Readers {
    fn get(&mut self, column: usize, position: usize) -> Result<Type, ProcessingError>;
    fn row_count(&self) -> usize;
    fn get_type(&self, column: usize) -> ReaderType;
    fn column_count(&self) -> usize;
    fn find(&self, name: &str) -> Option<usize>;
}

pub trait Mapper {
    fn map(&mut self, position: usize, readers: &mut dyn Readers) -> Type;
}

// TODO: should it return Source, or just remove processsor at all?
pub trait Processor {
    fn next(&mut self, readers: &mut dyn Readers) -> Option<Vec<Type>>;
}

pub trait Fold {
    fn fold(&self, current: &mut Type, value: Type);
}

#[derive(Clone, Copy, Debug)]
pub enum ReaderType {
    I32,
    I64,
    I96,
    Bool,
    Float,
    Double,
    String,
}

//////////////////  implementations

pub struct And {
    left: Box<dyn Mapper>,
    right: Box<dyn Mapper>,
}

impl And {
    pub fn new(left: Box<dyn Mapper>, right: Box<dyn Mapper>) -> Box<dyn Mapper> {
        Box::new(Self { left, right })
    }
}

impl Mapper for And {
    fn map(&mut self, position: usize, readers: &mut dyn Readers) -> Type {
        Type::Bool(
            self.left.map(position, readers) == Type::Bool(true)
                && self.right.map(position, readers) == Type::Bool(true),
        )
    }
}

pub struct Not {
    mapper: Box<dyn Mapper>,
}

impl Not {
    pub fn new(mapper: Box<dyn Mapper>) -> Box<dyn Mapper> {
        Box::new(Self { mapper })
    }
}

impl Mapper for Not {
    fn map(&mut self, position: usize, readers: &mut dyn Readers) -> Type {
        if let Type::Bool(v) = self.mapper.map(position, readers) {
            Type::Bool(!v)
        } else {
            Type::Bool(false)
        }
    }
}

pub struct AsIsMapper {
    column: usize,
}

impl AsIsMapper {
    pub fn new(column: usize) -> Box<dyn Mapper> {
        Box::new(Self { column })
    }
}

impl Mapper for AsIsMapper {
    fn map(&mut self, position: usize, readers: &mut dyn Readers) -> Type {
        readers.get(self.column, position).unwrap()
    }
}

pub struct Equal {
    left: Box<dyn Mapper>,
    right: Box<dyn Mapper>,
}

impl Equal {
    pub fn new(left: Box<dyn Mapper>, right: Box<dyn Mapper>) -> Box<dyn Mapper> {
        Box::new(Self { left, right })
    }
}

impl Mapper for Equal {
    fn map(&mut self, position: usize, readers: &mut dyn Readers) -> Type {
        let left = self.left.map(position, readers);
        let right = self.right.map(position, readers);
        Type::Bool(match (&left, &right) {
            (Type::I64(l), Type::I32(r)) => *l == *r as i64,
            (Type::I32(l), Type::I64(r)) => *l as i64 == *r,
            __ => left == right,
        })
    }
}

pub struct Literal {
    value: Type,
}

impl Literal {
    pub fn new(value: Type) -> Box<dyn Mapper> {
        Box::new(Self { value })
    }
}

impl Mapper for Literal {
    fn map(&mut self, _position: usize, _readers: &mut dyn Readers) -> Type {
        self.value.clone()
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

pub fn make_sum() -> Box<dyn Fold> {
    Box::new(FunctionFold::new(|a, b| match (a, &b) {
        (Type::None, Type::I64(_)) => b,
        (Type::I64(v1), Type::I64(v2)) => Type::I64(v1 + v2),
        (Type::None, Type::I32(_)) => b,
        (Type::I32(v1), Type::I32(v2)) => Type::I32(v1 + v2),
        _ => panic!("{a:?}, {b:?}"),
    }))
}

pub struct AsIsProcessor {
    projections: Vec<Box<dyn Mapper>>,
    filter: Box<dyn Mapper>,
    position: usize,
}

impl AsIsProcessor {
    pub fn new(projections: Vec<Box<dyn Mapper>>, filter: Box<dyn Mapper>) -> Box<dyn Processor> {
        Box::new(Self {
            projections,
            filter,
            position: 0,
        })
    }
}

impl Processor for AsIsProcessor {
    fn next(&mut self, readers: &mut dyn Readers) -> Option<Vec<Type>> {
        while readers.row_count() > self.position {
            self.position += 1;
            if let Type::Bool(true) = self.filter.map(self.position - 1, readers) {
                return Some(
                    self.projections
                        .iter_mut()
                        .map(|m| m.map(self.position - 1, readers))
                        .collect(),
                );
            }
        }
        None
    }
}

pub struct Group {
    filter: Box<dyn Mapper>,
    projections: Vec<Box<dyn Mapper>>,
    keys: Vec<Box<dyn Mapper>>,
    folds: Vec<Box<dyn Fold>>,
    result: Vec<Vec<Type>>,
    position: usize,
}

impl Group {
    pub fn new(
        filter: Box<dyn Mapper>,
        projections: Vec<Box<dyn Mapper>>,
        keys: Vec<Box<dyn Mapper>>,
        folds: Vec<Box<dyn Fold>>,
    ) -> Self {
        Self {
            filter,
            projections,
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
                if self.filter.map(position, readers) == Type::Bool(true) {
                    let key = self
                        .keys
                        .iter_mut()
                        .map(|k| k.map(position, readers))
                        .collect::<Vec<_>>();
                    let value = map
                        .entry(key)
                        .or_insert_with(|| vec![Type::None; self.folds.len()]);
                    for i in 0..self.projections.len() {
                        self.folds[i]
                            .fold(&mut value[i], self.projections[i].map(position, readers));
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
