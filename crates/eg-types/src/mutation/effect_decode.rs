use std::fmt;

use serde::de::{DeserializeSeed, Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::budget::{
    read_map_value_once, BorrowedRecordBytesSeed, MutationBudgetDeserialize, StructuralBudget,
};
use super::effects::{MutationEffectV1, RecordMutationV1};
use super::targets::RecordTargetV1;
use super::EFFECT_FIXED_BUDGET;
use crate::contract::{Digest256V1, RecordBytesV1};

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum RecordMutationField {
    Kind,
    Payload,
    PayloadDigest,
    ExpectedRecordDigest,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordMutationKind {
    Put,
    Delete,
}

enum BorrowedRecordMutationV1<'de> {
    Put {
        payload: &'de [u8],
        payload_digest: Digest256V1,
    },
    Delete {
        expected_record_digest: Digest256V1,
    },
}

struct BorrowedRecordMutationSeed;

struct BorrowedRecordMutationFields<'de> {
    kind: Option<RecordMutationKind>,
    payload: Option<&'de [u8]>,
    payload_digest: Option<Digest256V1>,
    expected_record_digest: Option<Digest256V1>,
}

impl BorrowedRecordMutationFields<'_> {
    fn new() -> Self {
        Self {
            kind: None,
            payload: None,
            payload_digest: None,
            expected_record_digest: None,
        }
    }
}

fn read_record_payload<'de, A>(
    fields: &mut BorrowedRecordMutationFields<'de>,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if fields.payload.is_some() {
        return Err(A::Error::duplicate_field("payload"));
    }
    fields.payload = Some(map.next_value_seed(BorrowedRecordBytesSeed)?);
    Ok(())
}

fn read_record_mutation_field<'de, A>(
    fields: &mut BorrowedRecordMutationFields<'de>,
    field: RecordMutationField,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    match field {
        RecordMutationField::Kind => read_map_value_once(&mut fields.kind, "kind", map),
        RecordMutationField::Payload => read_record_payload(fields, map),
        RecordMutationField::PayloadDigest => {
            read_map_value_once(&mut fields.payload_digest, "payload_digest", map)
        }
        RecordMutationField::ExpectedRecordDigest => read_map_value_once(
            &mut fields.expected_record_digest,
            "expected_record_digest",
            map,
        ),
    }
}

fn finish_put<E>(
    fields: BorrowedRecordMutationFields<'_>,
) -> Result<BorrowedRecordMutationV1<'_>, E>
where
    E: serde::de::Error,
{
    if fields.expected_record_digest.is_some() {
        return Err(E::custom("record mutation fields do not match its kind"));
    }
    Ok(BorrowedRecordMutationV1::Put {
        payload: fields.payload.ok_or_else(|| E::missing_field("payload"))?,
        payload_digest: fields
            .payload_digest
            .ok_or_else(|| E::missing_field("payload_digest"))?,
    })
}

fn finish_delete<E>(
    fields: BorrowedRecordMutationFields<'_>,
) -> Result<BorrowedRecordMutationV1<'_>, E>
where
    E: serde::de::Error,
{
    if fields.payload.is_some() || fields.payload_digest.is_some() {
        return Err(E::custom("record mutation fields do not match its kind"));
    }
    Ok(BorrowedRecordMutationV1::Delete {
        expected_record_digest: fields
            .expected_record_digest
            .ok_or_else(|| E::missing_field("expected_record_digest"))?,
    })
}

fn finish_record_mutation<E>(
    fields: BorrowedRecordMutationFields<'_>,
) -> Result<BorrowedRecordMutationV1<'_>, E>
where
    E: serde::de::Error,
{
    let kind = fields.kind.ok_or_else(|| E::missing_field("kind"))?;
    match kind {
        RecordMutationKind::Put => finish_put(fields),
        RecordMutationKind::Delete => finish_delete(fields),
    }
}

impl<'de> Visitor<'de> for BorrowedRecordMutationSeed {
    type Value = BorrowedRecordMutationV1<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a named record mutation")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = BorrowedRecordMutationFields::new();
        while let Some(field) = map.next_key::<RecordMutationField>()? {
            read_record_mutation_field(&mut fields, field, &mut map)?;
        }
        finish_record_mutation::<A::Error>(fields)
    }
}

impl<'de> DeserializeSeed<'de> for BorrowedRecordMutationSeed {
    type Value = BorrowedRecordMutationV1<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum MutationEffectField {
    Ordinal,
    Target,
    Mutation,
}

struct BudgetedMutationEffectVisitor<'a> {
    budget: &'a mut StructuralBudget,
}

struct MutationEffectFields<'de> {
    ordinal: Option<u32>,
    target: Option<RecordTargetV1>,
    mutation: Option<BorrowedRecordMutationV1<'de>>,
}

impl MutationEffectFields<'_> {
    fn new() -> Self {
        Self {
            ordinal: None,
            target: None,
            mutation: None,
        }
    }
}

fn read_effect_mutation<'de, A>(
    fields: &mut MutationEffectFields<'de>,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if fields.mutation.is_some() {
        return Err(A::Error::duplicate_field("mutation"));
    }
    fields.mutation = Some(map.next_value_seed(BorrowedRecordMutationSeed)?);
    Ok(())
}

fn read_effect_field<'de, A>(
    fields: &mut MutationEffectFields<'de>,
    field: MutationEffectField,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    match field {
        MutationEffectField::Ordinal => read_map_value_once(&mut fields.ordinal, "ordinal", map),
        MutationEffectField::Target => read_map_value_once(&mut fields.target, "target", map),
        MutationEffectField::Mutation => read_effect_mutation(fields, map),
    }
}

fn payload_bytes(mutation: &BorrowedRecordMutationV1<'_>) -> usize {
    match mutation {
        BorrowedRecordMutationV1::Put { payload, .. } => payload.len(),
        BorrowedRecordMutationV1::Delete { .. } => 0,
    }
}

fn effect_charge<E>(
    target: &RecordTargetV1,
    mutation: &BorrowedRecordMutationV1<'_>,
) -> Result<usize, E>
where
    E: serde::de::Error,
{
    [
        EFFECT_FIXED_BUDGET,
        target.tenant.as_str().len(),
        64,
        target.schema_id.as_str().len(),
        target.record_id.as_str().len(),
        payload_bytes(mutation),
    ]
    .into_iter()
    .try_fold(0usize, |total, amount| total.checked_add(amount))
    .ok_or_else(|| E::custom("mutation structural budget overflow"))
}

fn own_record_mutation<E>(mutation: BorrowedRecordMutationV1<'_>) -> Result<RecordMutationV1, E>
where
    E: serde::de::Error,
{
    match mutation {
        BorrowedRecordMutationV1::Put {
            payload,
            payload_digest,
        } => Ok(RecordMutationV1::Put {
            payload: RecordBytesV1::new(payload.to_vec()).map_err(E::custom)?,
            payload_digest,
        }),
        BorrowedRecordMutationV1::Delete {
            expected_record_digest,
        } => Ok(RecordMutationV1::Delete {
            expected_record_digest,
        }),
    }
}

fn finish_effect<E>(
    fields: MutationEffectFields<'_>,
    budget: &mut StructuralBudget,
) -> Result<MutationEffectV1, E>
where
    E: serde::de::Error,
{
    let target = fields.target.ok_or_else(|| E::missing_field("target"))?;
    let borrowed_mutation = fields
        .mutation
        .ok_or_else(|| E::missing_field("mutation"))?;
    let charge = effect_charge::<E>(&target, &borrowed_mutation)?;
    budget.charge(charge).map_err(E::custom)?;
    let mutation = own_record_mutation::<E>(borrowed_mutation)?;
    Ok(MutationEffectV1 {
        ordinal: fields.ordinal.ok_or_else(|| E::missing_field("ordinal"))?,
        target,
        mutation,
    })
}

impl<'de> Visitor<'de> for BudgetedMutationEffectVisitor<'_> {
    type Value = MutationEffectV1;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a named mutation effect")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = MutationEffectFields::new();
        while let Some(field) = map.next_key::<MutationEffectField>()? {
            read_effect_field(&mut fields, field, &mut map)?;
        }
        finish_effect::<A::Error>(fields, self.budget)
    }
}

impl<'de> MutationBudgetDeserialize<'de> for MutationEffectV1 {
    fn deserialize_budgeted<D>(
        deserializer: D,
        budget: &mut StructuralBudget,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BudgetedMutationEffectVisitor { budget })
    }
}
