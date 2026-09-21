//! Duplicate-key rejecting JSON with deterministic depth and node budgets.

use std::cell::Cell;
use std::rc::Rc;

pub(super) const MAX_JSON_DEPTH: usize = 64;
const MAX_JSON_NODES: usize = 50_000;

pub(super) fn parse_bounded_json(bytes: &[u8]) -> Result<serde_json::Value, String> {
    use serde::de::DeserializeSeed;

    let nodes = Rc::new(Cell::new(0));
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = JsonSeed { depth: 0, nodes }
        .deserialize(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok(value)
}

struct JsonSeed {
    depth: usize,
    nodes: Rc<Cell<usize>>,
}

impl<'de> serde::de::DeserializeSeed<'de> for JsonSeed {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        if self.depth > MAX_JSON_DEPTH {
            return Err(D::Error::custom("JSON depth exceeds the served bound"));
        }
        let count = self.nodes.get().saturating_add(1);
        if count > MAX_JSON_NODES {
            return Err(D::Error::custom("JSON node count exceeds the served bound"));
        }
        self.nodes.set(count);
        deserializer.deserialize_any(JsonVisitor(self))
    }
}

struct JsonVisitor(JsonSeed);

impl<'de> serde::de::Visitor<'de> for JsonVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bounded JSON without duplicate object keys")
    }
    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }
    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(JsonSeed {
            depth: self.0.depth + 1,
            nodes: self.0.nodes.clone(),
        })? {
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    }
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        use serde::de::Error as _;
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom("duplicate JSON object key"));
            }
            let value = map.next_value_seed(JsonSeed {
                depth: self.0.depth + 1,
                nodes: self.0.nodes.clone(),
            })?;
            values.insert(key, value);
        }
        Ok(serde_json::Value::Object(values))
    }
}

pub(super) fn json_size(value: &serde_json::Value, depth: usize) -> Result<usize, ()> {
    if depth > MAX_JSON_DEPTH {
        return Err(());
    }
    let count = match value {
        serde_json::Value::Array(v) => {
            1 + v
                .iter()
                .map(|x| json_size(x, depth + 1))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<usize>()
        }
        serde_json::Value::Object(v) => {
            1 + v
                .values()
                .map(|x| json_size(x, depth + 1))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<usize>()
        }
        _ => 1,
    };
    if count > MAX_JSON_NODES {
        Err(())
    } else {
        Ok(count)
    }
}
