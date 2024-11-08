//! Dynamo Db implementation.
use aws_sdk_dynamodb::types::AttributeValue;

use crate::{Value, HasCrudFields, Crud};

impl From<Value> for AttributeValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(i) => AttributeValue::N(i.to_string()),
            Value::Float(i) => AttributeValue::N(i.to_string()),
            Value::String(i) => AttributeValue::S(i),
            Value::Bytes(i) => AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(i)),
            Value::None => AttributeValue::Null(true),
        }
    }
}

impl From<AttributeValue> for Value {
    fn from(value: AttributeValue) -> Self {
        match value {
            AttributeValue::B(b) => Value::Bytes(b.into_inner()),
            AttributeValue::N(n) => {
                if let Ok(i) = n.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = n.parse::<f64>() {
                    Value::Float(f)
                } else {
                    Value::None
                }
            }
            AttributeValue::S(s) => Value::String(s),
            _ => Value::None,
        }
    }
}

pub struct DynamoDb;

impl<T: HasCrudFields + Clone + Sized + 'static> Crud<DynamoDb> for T {
    type Connection<'a> = &'a aws_sdk_dynamodb::Client;

    fn create(_: Self::Connection<'_>) -> Result<(), snafu::Whatever> {
        Ok(())
    }

    fn insert(&self, client: Self::Connection<'_>) -> Result<(), snafu::Whatever> {
        client
            .put_item()
            .
    }

    fn read_all<'a>(
        connection: Self::Connection<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        todo!()
    }

    fn read_where<'a>(
        connection: Self::Connection<'a>,
        key_name: &'a str,
        comparison: &'a str,
        key_value: impl crate::IsCrudField,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        todo!()
    }

    fn read<'a, Key: crate::IsCrudField>(
        connection: Self::Connection<'a>,
        key: Key,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        todo!()
    }

    fn update(&self, connection: Self::Connection<'_>) -> Result<(), snafu::Whatever> {
        todo!()
    }

    fn delete(self, connection: Self::Connection<'_>) -> Result<(), snafu::Whatever> {
        todo!()
    }

}
