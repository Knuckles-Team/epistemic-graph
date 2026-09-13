//! The `ResultPayload` encodings a declared result may use.

use serde::Serialize;

use crate::protocol::ResultPayload;

/// How a body of type `Body` becomes a [`ResultPayload`].
pub trait Encoding<Body> {
    /// The `ResultPayload` variant the encoding produces, as published.
    const NAME: &'static str;
    fn encode(body: Body) -> Result<ResultPayload, String>;
}

/// An encoding that serializes through a borrow, so a handler need not give up the value.
pub trait EncodeRef<Body: ?Sized> {
    fn encode_ref(body: &Body) -> Result<ResultPayload, String>;
}

/// An encoding that cannot fail.
pub trait EncodeScalar<Body> {
    fn encode_scalar(body: Body) -> ResultPayload;
}

macro_rules! scalar_encoding {
    ($name:ident, $body:ty, $variant:ident) => {
        #[doc = concat!("`ResultPayload::", stringify!($variant), "`.")]
        pub enum $name {}

        impl Encoding<$body> for $name {
            const NAME: &'static str = stringify!($variant);

            fn encode(body: $body) -> Result<ResultPayload, String> {
                Ok(ResultPayload::$variant(body))
            }
        }

        impl EncodeScalar<$body> for $name {
            fn encode_scalar(body: $body) -> ResultPayload {
                ResultPayload::$variant(body)
            }
        }
    };
}

scalar_encoding!(Bool, bool, Bool);
scalar_encoding!(Count, u64, Count);
scalar_encoding!(Float, f64, Float);
scalar_encoding!(Text, String, String);
scalar_encoding!(Ids, Vec<String>, Ids);
scalar_encoding!(NodeList, Vec<(String, serde_json::Value)>, NodeList);
scalar_encoding!(EdgeList, Vec<(String, String, Vec<u8>)>, EdgeList);

/// `ResultPayload::Raw`: the body serialized straight to MessagePack.
pub enum Raw {}

impl<T: Serialize> Encoding<T> for Raw {
    const NAME: &'static str = "Raw";

    fn encode(body: T) -> Result<ResultPayload, String> {
        ResultPayload::raw(&body)
    }
}

impl<T: Serialize + ?Sized> EncodeRef<T> for Raw {
    fn encode_ref(body: &T) -> Result<ResultPayload, String> {
        ResultPayload::raw(body)
    }
}

/// `ResultPayload::Json`: the body serialized to a JSON value tree.
pub enum Json {}

fn json_value<T: Serialize + ?Sized>(body: &T) -> Result<ResultPayload, String> {
    serde_json::to_value(body)
        .map(ResultPayload::Json)
        .map_err(|error| format!("result serialization failed: {error}"))
}

impl<T: Serialize> Encoding<T> for Json {
    const NAME: &'static str = "Json";

    fn encode(body: T) -> Result<ResultPayload, String> {
        json_value(&body)
    }
}

impl<T: Serialize + ?Sized> EncodeRef<T> for Json {
    fn encode_ref(body: &T) -> Result<ResultPayload, String> {
        json_value(body)
    }
}

/// `ResultPayload::Raw` for a present body, `ResultPayload::Json(null)` for an absent one --
/// the "found / not found" shape a lookup method answers with.
pub enum RawOrNull {}

fn raw_or_null<T: Serialize>(body: Option<&T>) -> Result<ResultPayload, String> {
    match body {
        Some(body) => ResultPayload::raw(body),
        None => Ok(ResultPayload::Json(serde_json::Value::Null)),
    }
}

impl<T: Serialize> Encoding<Option<T>> for RawOrNull {
    const NAME: &'static str = "RawOrNull";

    fn encode(body: Option<T>) -> Result<ResultPayload, String> {
        raw_or_null(body.as_ref())
    }
}

/// A caller-shaped lookup result is built with `ResultPayload::of_dynamic` or
/// `ResultPayload::of_encoded_or_null`; the uninhabited body is never encoded directly.
impl Encoding<super::Dynamic> for RawOrNull {
    const NAME: &'static str = "RawOrNull";

    fn encode(body: super::Dynamic) -> Result<ResultPayload, String> {
        match body {}
    }
}

impl<T: Serialize> EncodeRef<Option<T>> for RawOrNull {
    fn encode_ref(body: &Option<T>) -> Result<ResultPayload, String> {
        raw_or_null(body.as_ref())
    }
}
