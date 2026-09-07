use std::fmt;

use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::contract::{BoundedVec, MAX_MUTATION_ENVELOPE_BYTES};
// complete 64-parent scope copies are < 200 KiB, fewer than 96 bounded 1 KiB
// identifiers are < 96 KiB, and all fixed digests, map keys, markers, and
// integers fit in the remaining > 700 KiB. Repeated collections are charged
// item-by-item below against the same aggregate budget.
const MUTATION_FIXED_BUDGET: usize = 1024 * 1024;
pub(super) const PRECONDITION_FIXED_BUDGET: usize = 256;

pub(super) struct StructuralBudget {
    pub(super) used: usize,
}

impl StructuralBudget {
    pub(super) fn new() -> Self {
        Self {
            used: MUTATION_FIXED_BUDGET,
        }
    }

    pub(super) fn charge(&mut self, amount: usize) -> Result<(), String> {
        self.used = self
            .used
            .checked_add(amount)
            .filter(|total| *total <= MAX_MUTATION_ENVELOPE_BYTES)
            .ok_or_else(|| "mutation exceeds the 16 MiB structural budget".to_string())?;
        Ok(())
    }
}

pub(super) trait MutationBudgetCharge {
    fn mutation_budget_charge(&self) -> Result<usize, String>;
}

pub(super) trait MutationBudgetDeserialize<'de>: Sized {
    fn deserialize_budgeted<D>(
        deserializer: D,
        budget: &mut StructuralBudget,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>;
}

pub(super) fn read_map_value_once<'de, T, A>(
    slot: &mut Option<T>,
    field: &'static str,
    map: &mut A,
) -> Result<(), A::Error>
where
    T: Deserialize<'de>,
    A: MapAccess<'de>,
{
    if slot.is_some() {
        return Err(A::Error::duplicate_field(field));
    }
    *slot = Some(map.next_value()?);
    Ok(())
}

pub(super) fn deserialize_and_charge<'de, T, D>(
    deserializer: D,
    budget: &mut StructuralBudget,
) -> Result<T, D::Error>
where
    T: Deserialize<'de> + MutationBudgetCharge,
    D: Deserializer<'de>,
{
    let value = T::deserialize(deserializer)?;
    let charge = value.mutation_budget_charge().map_err(D::Error::custom)?;
    budget.charge(charge).map_err(D::Error::custom)?;
    Ok(value)
}

struct MutationElementSeed<'a, T> {
    budget: &'a mut StructuralBudget,
    marker: std::marker::PhantomData<T>,
}

impl<'de, T> DeserializeSeed<'de> for MutationElementSeed<'_, T>
where
    T: MutationBudgetDeserialize<'de>,
{
    type Value = T;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize_budgeted(deserializer, self.budget)
    }
}

pub(super) struct BudgetedVecSeed<'a, T, const MAXIMUM: usize> {
    budget: &'a mut StructuralBudget,
    marker: std::marker::PhantomData<T>,
}

impl<'a, T, const MAXIMUM: usize> BudgetedVecSeed<'a, T, MAXIMUM> {
    pub(super) fn new(budget: &'a mut StructuralBudget) -> Self {
        Self {
            budget,
            marker: std::marker::PhantomData,
        }
    }
}

struct BudgetedVecVisitor<'a, T, const MAXIMUM: usize> {
    budget: &'a mut StructuralBudget,
    marker: std::marker::PhantomData<T>,
}

impl<'de, T, const MAXIMUM: usize> Visitor<'de> for BudgetedVecVisitor<'_, T, MAXIMUM>
where
    T: MutationBudgetDeserialize<'de>,
{
    type Value = BoundedVec<T, MAXIMUM>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "at most {MAXIMUM} structurally budgeted items")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence.size_hint().is_some_and(|size| size > MAXIMUM) {
            return Err(A::Error::custom(format!(
                "collection exceeds {MAXIMUM} items"
            )));
        }
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAXIMUM));
        while values.len() < MAXIMUM {
            let Some(value) = sequence.next_element_seed(MutationElementSeed {
                budget: self.budget,
                marker: std::marker::PhantomData::<T>,
            })?
            else {
                return BoundedVec::new(values).map_err(A::Error::custom);
            };
            values.push(value);
        }
        if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
            return Err(A::Error::custom(format!(
                "collection exceeds {MAXIMUM} items"
            )));
        }
        BoundedVec::new(values).map_err(A::Error::custom)
    }
}

impl<'de, T, const MAXIMUM: usize> DeserializeSeed<'de> for BudgetedVecSeed<'_, T, MAXIMUM>
where
    T: MutationBudgetDeserialize<'de>,
{
    type Value = BoundedVec<T, MAXIMUM>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BudgetedVecVisitor {
            budget: self.budget,
            marker: self.marker,
        })
    }
}

pub(super) struct BorrowedRecordBytesSeed;

struct BorrowedRecordBytesVisitor;

impl<'de> Visitor<'de> for BorrowedRecordBytesVisitor {
    type Value = &'de [u8];

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("record bytes within the remaining mutation budget")
    }

    fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value.len() > crate::contract::MAX_RECORD_BYTES {
            return Err(E::custom("record exceeds the 1 MiB contract limit"));
        }
        Ok(value)
    }
}

impl<'de> DeserializeSeed<'de> for BorrowedRecordBytesSeed {
    type Value = &'de [u8];

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(BorrowedRecordBytesVisitor)
    }
}
