use super::processing::*;
use sqlparser::ast::{Expr, Statement, ValueWithSpan};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

fn _sql() {
    /*
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
    // let sql = "select sum(a) from x where b = 42";
    let sql = "select a from x where b = 112";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("AST: {:?}", ast);
}

pub fn to_process(
    statement: &Statement,
    readers: &dyn Readers,
) -> Result<Box<dyn Processor>, ParsingError> {
    let Statement::Query(query) = statement else {
        Err(ParsingError::NotSelect)?
    };
    if query.with.is_some() {
        // TODO: check other fields
        Err(ParsingError::NotSupported("with"))?
    }
    let Some(select) = query.body.as_select() else {
        Err(ParsingError::NotSelect)?
    };
    let mut projections = vec![];
    for result in select.projection.iter().map(|p| map_projection(readers, p)) {
        projections.extend(result?);
    }

    let sqlparser::ast::GroupByExpr::Expressions(group_by_expr, _) = &select.group_by else {
        Err(ParsingError::NotSupported("group by all"))?
    };

    let filters = select
        .selection
        .as_ref()
        .map(|s| map_expr(readers, &s))
        .unwrap_or_else(|| Ok(Literal::new(Type::Bool(true))))?;

    let processor = if group_by_expr.is_empty() {
        AsIsProcessor::new(projections, filters)
    } else {
        todo!()
        // Box::new(Group::new(filters, projections, keys, folds))
    };

    Ok(processor)
}

fn map_projection(
    readers: &dyn Readers,
    expr: &sqlparser::ast::SelectItem,
) -> Result<Vec<Box<dyn Mapper>>, ParsingError> {
    match expr {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
            Ok(vec![map_expr(readers, expr)?])
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(
            select_item_qualified_wildcard_kind,
            wildcard_additional_options,
        ) => todo!(),
        sqlparser::ast::SelectItem::Wildcard(wildcard_additional_options) =>
        // TODO: check options
        {
            Ok((0..readers.column_count())
                .map(|i| AsIsMapper::new(i))
                .collect())
        }
    }
}

fn map_expr(
    readers: &dyn Readers,
    expr: &sqlparser::ast::Expr,
) -> Result<Box<dyn Mapper>, ParsingError> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => readers
            .find(&ident.value)
            .map(|i| AsIsMapper::new(i))
            .ok_or_else(|| ParsingError::IdentifierNotFound(ident.value.clone())),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => todo!(),
        sqlparser::ast::Expr::CompoundFieldAccess { root, access_chain } => todo!(),
        sqlparser::ast::Expr::JsonAccess { value, path } => todo!(),
        sqlparser::ast::Expr::IsFalse(expr) => todo!(),
        sqlparser::ast::Expr::IsNotFalse(expr) => todo!(),
        sqlparser::ast::Expr::IsTrue(expr) => todo!(),
        sqlparser::ast::Expr::IsNotTrue(expr) => todo!(),
        sqlparser::ast::Expr::IsNull(expr) => Ok(Equal::new(
            map_expr(readers, expr)?,
            Literal::new(Type::Null),
        )),
        sqlparser::ast::Expr::IsNotNull(expr) => todo!(),
        sqlparser::ast::Expr::IsUnknown(expr) => todo!(),
        sqlparser::ast::Expr::IsNotUnknown(expr) => todo!(),
        sqlparser::ast::Expr::IsDistinctFrom(expr, expr1) => todo!(),
        sqlparser::ast::Expr::IsNotDistinctFrom(expr, expr1) => todo!(),
        sqlparser::ast::Expr::IsNormalized {
            expr,
            form,
            negated,
        } => todo!(),
        sqlparser::ast::Expr::InList {
            expr,
            list,
            negated,
        } => todo!(),
        sqlparser::ast::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => todo!(),
        sqlparser::ast::Expr::InUnnest {
            expr,
            array_expr,
            negated,
        } => todo!(),
        sqlparser::ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => todo!(),
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            let left = map_expr(readers, left)?;
            let right = map_expr(readers, right)?;
            Ok(match (op) {
                sqlparser::ast::BinaryOperator::Plus => todo!(),
                sqlparser::ast::BinaryOperator::Minus => todo!(),
                sqlparser::ast::BinaryOperator::Multiply => todo!(),
                sqlparser::ast::BinaryOperator::Divide => todo!(),
                sqlparser::ast::BinaryOperator::Modulo => todo!(),
                sqlparser::ast::BinaryOperator::StringConcat => todo!(),
                sqlparser::ast::BinaryOperator::Gt => todo!(),
                sqlparser::ast::BinaryOperator::Lt => todo!(),
                sqlparser::ast::BinaryOperator::GtEq => todo!(),
                sqlparser::ast::BinaryOperator::LtEq => todo!(),
                sqlparser::ast::BinaryOperator::Spaceship => todo!(),
                sqlparser::ast::BinaryOperator::Eq => Equal::new(left, right),
                sqlparser::ast::BinaryOperator::NotEq => todo!(),
                sqlparser::ast::BinaryOperator::And => And::new(left, right),
                sqlparser::ast::BinaryOperator::Or => todo!(),
                sqlparser::ast::BinaryOperator::Xor => todo!(),
                sqlparser::ast::BinaryOperator::BitwiseOr => todo!(),
                sqlparser::ast::BinaryOperator::BitwiseAnd => todo!(),
                sqlparser::ast::BinaryOperator::BitwiseXor => todo!(),
                sqlparser::ast::BinaryOperator::DuckIntegerDivide => todo!(),
                sqlparser::ast::BinaryOperator::MyIntegerDivide => todo!(),
                sqlparser::ast::BinaryOperator::Custom(_) => todo!(),
                sqlparser::ast::BinaryOperator::PGBitwiseXor => todo!(),
                sqlparser::ast::BinaryOperator::PGBitwiseShiftLeft => todo!(),
                sqlparser::ast::BinaryOperator::PGBitwiseShiftRight => todo!(),
                sqlparser::ast::BinaryOperator::PGExp => todo!(),
                sqlparser::ast::BinaryOperator::PGOverlap => todo!(),
                sqlparser::ast::BinaryOperator::PGRegexMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGRegexIMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGRegexNotMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGRegexNotIMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGLikeMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGILikeMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGNotLikeMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGNotILikeMatch => todo!(),
                sqlparser::ast::BinaryOperator::PGStartsWith => todo!(),
                sqlparser::ast::BinaryOperator::Arrow => todo!(),
                sqlparser::ast::BinaryOperator::LongArrow => todo!(),
                sqlparser::ast::BinaryOperator::HashArrow => todo!(),
                sqlparser::ast::BinaryOperator::HashLongArrow => todo!(),
                sqlparser::ast::BinaryOperator::AtAt => todo!(),
                sqlparser::ast::BinaryOperator::AtArrow => todo!(),
                sqlparser::ast::BinaryOperator::ArrowAt => todo!(),
                sqlparser::ast::BinaryOperator::HashMinus => todo!(),
                sqlparser::ast::BinaryOperator::AtQuestion => todo!(),
                sqlparser::ast::BinaryOperator::Question => todo!(),
                sqlparser::ast::BinaryOperator::QuestionAnd => todo!(),
                sqlparser::ast::BinaryOperator::QuestionPipe => todo!(),
                sqlparser::ast::BinaryOperator::PGCustomBinaryOperator(items) => todo!(),
                sqlparser::ast::BinaryOperator::Overlaps => todo!(),
                sqlparser::ast::BinaryOperator::DoubleHash => todo!(),
                sqlparser::ast::BinaryOperator::LtDashGt => todo!(),
                sqlparser::ast::BinaryOperator::AndLt => todo!(),
                sqlparser::ast::BinaryOperator::AndGt => todo!(),
                sqlparser::ast::BinaryOperator::LtLtPipe => todo!(),
                sqlparser::ast::BinaryOperator::PipeGtGt => todo!(),
                sqlparser::ast::BinaryOperator::AndLtPipe => todo!(),
                sqlparser::ast::BinaryOperator::PipeAndGt => todo!(),
                sqlparser::ast::BinaryOperator::LtCaret => todo!(),
                sqlparser::ast::BinaryOperator::GtCaret => todo!(),
                sqlparser::ast::BinaryOperator::QuestionHash => todo!(),
                sqlparser::ast::BinaryOperator::QuestionDash => todo!(),
                sqlparser::ast::BinaryOperator::QuestionDashPipe => todo!(),
                sqlparser::ast::BinaryOperator::QuestionDoublePipe => todo!(),
                sqlparser::ast::BinaryOperator::At => todo!(),
                sqlparser::ast::BinaryOperator::TildeEq => todo!(),
            })
        }
        sqlparser::ast::Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => todo!(),
        sqlparser::ast::Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => todo!(),
        sqlparser::ast::Expr::SimilarTo {
            negated,
            expr,
            pattern,
            escape_char,
        } => todo!(),
        sqlparser::ast::Expr::RLike {
            negated,
            expr,
            pattern,
            regexp,
        } => todo!(),
        sqlparser::ast::Expr::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => todo!(),
        sqlparser::ast::Expr::AllOp {
            left,
            compare_op,
            right,
        } => todo!(),
        sqlparser::ast::Expr::UnaryOp { op, expr } => todo!(),
        sqlparser::ast::Expr::Convert {
            is_try,
            expr,
            data_type,
            charset,
            target_before_value,
            styles,
        } => todo!(),
        sqlparser::ast::Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => todo!(),
        sqlparser::ast::Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => todo!(),
        sqlparser::ast::Expr::Extract {
            field,
            syntax,
            expr,
        } => todo!(),
        sqlparser::ast::Expr::Ceil { expr, field } => todo!(),
        sqlparser::ast::Expr::Floor { expr, field } => todo!(),
        sqlparser::ast::Expr::Position { expr, r#in } => todo!(),
        sqlparser::ast::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            special,
        } => todo!(),
        sqlparser::ast::Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => todo!(),
        sqlparser::ast::Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => todo!(),
        sqlparser::ast::Expr::Collate { expr, collation } => todo!(),
        sqlparser::ast::Expr::Nested(expr) => todo!(),
        sqlparser::ast::Expr::Value(value_with_span) => make_literal(value_with_span),
        sqlparser::ast::Expr::IntroducedString { introducer, value } => todo!(),
        sqlparser::ast::Expr::TypedString { data_type, value } => todo!(),
        sqlparser::ast::Expr::Function(function) => todo!(),
        sqlparser::ast::Expr::Case {
            operand,
            conditions,
            else_result,
        } => todo!(),
        sqlparser::ast::Expr::Exists { subquery, negated } => todo!(),
        sqlparser::ast::Expr::Subquery(query) => todo!(),
        sqlparser::ast::Expr::GroupingSets(items) => todo!(),
        sqlparser::ast::Expr::Cube(items) => todo!(),
        sqlparser::ast::Expr::Rollup(items) => todo!(),
        sqlparser::ast::Expr::Tuple(exprs) => todo!(),
        sqlparser::ast::Expr::Struct { values, fields } => todo!(),
        sqlparser::ast::Expr::Named { expr, name } => todo!(),
        sqlparser::ast::Expr::Dictionary(dictionary_fields) => todo!(),
        sqlparser::ast::Expr::Map(map) => todo!(),
        sqlparser::ast::Expr::Array(array) => todo!(),
        sqlparser::ast::Expr::Interval(interval) => todo!(),
        sqlparser::ast::Expr::MatchAgainst {
            columns,
            match_value,
            opt_search_modifier,
        } => todo!(),
        sqlparser::ast::Expr::Wildcard(attached_token) => todo!(),
        sqlparser::ast::Expr::QualifiedWildcard(object_name, attached_token) => todo!(),
        sqlparser::ast::Expr::OuterJoin(expr) => todo!(),
        sqlparser::ast::Expr::Prior(expr) => todo!(),
        sqlparser::ast::Expr::Lambda(lambda_function) => todo!(),
    }
}

#[derive(Error, Debug)]
pub enum ParsingError {
    #[error("only select is supported")]
    NotSelect,
    #[error("only select is supported")]
    NotSupported(&'static str),
    #[error("only select is supported")]
    IdentifierNotFound(String),
}

fn make_literal(value: &ValueWithSpan) -> Result<Box<dyn Mapper>, ParsingError> {
    let value = match &value.value {
        sqlparser::ast::Value::Number(v, _) => Ok(Type::I64((v.parse().unwrap()))),
        sqlparser::ast::Value::SingleQuotedString(s) => Ok(Type::String(s.as_bytes().into())),
        sqlparser::ast::Value::Boolean(v) => Ok(Type::Bool(*v)),
        sqlparser::ast::Value::Null => Ok(Type::Null),
        _ => Err(ParsingError::NotSupported("literal")),
    }?;
    Ok(Literal::new(value))
}
