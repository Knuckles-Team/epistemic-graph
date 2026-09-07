use std::fmt;

use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::budget::{
    deserialize_and_charge, read_map_value_once, BorrowedRecordBytesSeed, MutationBudgetCharge,
    MutationBudgetDeserialize, StructuralBudget,
};
use super::{OUTBOX_FIXED_BUDGET, OUTBOX_HEADER_FIXED_BUDGET, PROVENANCE_FIXED_BUDGET};
use crate::contract::{
    BoundedVec, Digest256, OpaqueId, RecordBytes, ResourceId, TenantId,
};
use crate::outbox::{OutboxHeader, OutboxIntent, MAX_OUTBOX_HEADERS};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceBinding {
    pub provenance_id: OpaqueId,
    pub source_digest: Digest256,
}

impl ProvenanceBinding {
    pub(super) fn digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            b"eg/provenance-binding/v1",
            &[
                self.provenance_id.as_str().as_bytes(),
                self.source_digest.as_bytes(),
            ],
        )
    }
}

impl MutationBudgetCharge for ProvenanceBinding {
    fn mutation_budget_charge(&self) -> Result<usize, String> {
        PROVENANCE_FIXED_BUDGET
            .checked_add(self.provenance_id.as_str().len())
            .ok_or_else(|| "mutation structural budget overflow".to_string())
    }
}

impl<'de> MutationBudgetDeserialize<'de> for ProvenanceBinding {
    fn deserialize_budgeted<D>(
        deserializer: D,
        budget: &mut StructuralBudget,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_and_charge(deserializer, budget)
    }
}

impl MutationBudgetCharge for OutboxIntent {
    fn mutation_budget_charge(&self) -> Result<usize, String> {
        let headers = self.headers.iter().try_fold(0usize, |total, header| {
            total
                .checked_add(OUTBOX_HEADER_FIXED_BUDGET)
                .and_then(|value| value.checked_add(header.name.as_str().len()))
                .and_then(|value| value.checked_add(header.value.as_str().len()))
                .ok_or_else(|| "mutation structural budget overflow".to_string())
        })?;
        [
            OUTBOX_FIXED_BUDGET,
            self.tenant.as_str().len(),
            64,
            self.topic.as_str().len(),
            self.partition_key.as_str().len(),
            self.event_schema.as_str().len(),
            self.payload.as_slice().len(),
            headers,
        ]
        .into_iter()
        .try_fold(0usize, |total, amount| {
            total
                .checked_add(amount)
                .ok_or_else(|| "mutation structural budget overflow".to_string())
        })
    }
}

struct BudgetedOutboxHeadersSeed<'a> {
    budget: &'a mut StructuralBudget,
}

struct BudgetedOutboxHeadersVisitor<'a> {
    budget: &'a mut StructuralBudget,
}

impl<'de> Visitor<'de> for BudgetedOutboxHeadersVisitor<'_> {
    type Value = BoundedVec<OutboxHeader, MAX_OUTBOX_HEADERS>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("budgeted outbox headers")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence
            .size_hint()
            .is_some_and(|size| size > MAX_OUTBOX_HEADERS)
        {
            return Err(A::Error::custom("outbox headers exceed 64 items"));
        }
        let mut values =
            Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_OUTBOX_HEADERS));
        while values.len() < MAX_OUTBOX_HEADERS {
            let Some(header) = sequence.next_element::<OutboxHeader>()? else {
                return BoundedVec::new(values).map_err(A::Error::custom);
            };
            let charge = OUTBOX_HEADER_FIXED_BUDGET
                .checked_add(header.name.as_str().len())
                .and_then(|value| value.checked_add(header.value.as_str().len()))
                .ok_or_else(|| A::Error::custom("mutation structural budget overflow"))?;
            self.budget.charge(charge).map_err(A::Error::custom)?;
            values.push(header);
        }
        if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("outbox headers exceed 64 items"));
        }
        BoundedVec::new(values).map_err(A::Error::custom)
    }
}

impl<'de> DeserializeSeed<'de> for BudgetedOutboxHeadersSeed<'_> {
    type Value = BoundedVec<OutboxHeader, MAX_OUTBOX_HEADERS>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BudgetedOutboxHeadersVisitor {
            budget: self.budget,
        })
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum OutboxIntentField {
    Tenant,
    DestinationScopeDigest,
    Topic,
    PartitionKey,
    EventSchema,
    Payload,
    PayloadDigest,
    Headers,
}

struct BudgetedOutboxIntentVisitor<'a> {
    budget: &'a mut StructuralBudget,
}

struct OutboxIntentFields<'de> {
    tenant: Option<TenantId>,
    destination_scope_digest: Option<Digest256>,
    topic: Option<ResourceId>,
    partition_key: Option<ResourceId>,
    event_schema: Option<ResourceId>,
    payload: Option<&'de [u8]>,
    payload_digest: Option<Digest256>,
    headers: Option<BoundedVec<OutboxHeader, MAX_OUTBOX_HEADERS>>,
}

impl OutboxIntentFields<'_> {
    fn new() -> Self {
        Self {
            tenant: None,
            destination_scope_digest: None,
            topic: None,
            partition_key: None,
            event_schema: None,
            payload: None,
            payload_digest: None,
            headers: None,
        }
    }
}

fn charge_identifier<E>(budget: &mut StructuralBudget, length: usize) -> Result<(), E>
where
    E: serde::de::Error,
{
    budget.charge(length).map_err(E::custom)
}

fn read_tenant<'de, A>(
    fields: &mut OutboxIntentFields<'de>,
    map: &mut A,
    budget: &mut StructuralBudget,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    read_map_value_once(&mut fields.tenant, "tenant", map)?;
    charge_identifier::<A::Error>(budget, fields.tenant.as_ref().unwrap().as_str().len())
}

fn read_resource<'de, A>(
    slot: &mut Option<ResourceId>,
    label: &'static str,
    map: &mut A,
    budget: &mut StructuralBudget,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    read_map_value_once(slot, label, map)?;
    charge_identifier::<A::Error>(budget, slot.as_ref().unwrap().as_str().len())
}

fn read_payload<'de, A>(fields: &mut OutboxIntentFields<'de>, map: &mut A) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if fields.payload.is_some() {
        return Err(A::Error::duplicate_field("payload"));
    }
    fields.payload = Some(map.next_value_seed(BorrowedRecordBytesSeed)?);
    Ok(())
}

fn read_headers<'de, A>(
    fields: &mut OutboxIntentFields<'de>,
    map: &mut A,
    budget: &mut StructuralBudget,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if fields.headers.is_some() {
        return Err(A::Error::duplicate_field("headers"));
    }
    fields.headers = Some(map.next_value_seed(BudgetedOutboxHeadersSeed { budget })?);
    Ok(())
}

fn read_outbox_field<'de, A>(
    fields: &mut OutboxIntentFields<'de>,
    field: OutboxIntentField,
    map: &mut A,
    budget: &mut StructuralBudget,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    match field {
        OutboxIntentField::Tenant => read_tenant(fields, map, budget),
        OutboxIntentField::DestinationScopeDigest => {
            read_map_value_once(
                &mut fields.destination_scope_digest,
                "destination_scope_digest",
                map,
            )?;
            budget.charge(64).map_err(A::Error::custom)
        }
        OutboxIntentField::Topic => read_resource(&mut fields.topic, "topic", map, budget),
        OutboxIntentField::PartitionKey => {
            read_resource(&mut fields.partition_key, "partition_key", map, budget)
        }
        OutboxIntentField::EventSchema => {
            read_resource(&mut fields.event_schema, "event_schema", map, budget)
        }
        OutboxIntentField::Payload => read_payload(fields, map),
        OutboxIntentField::PayloadDigest => {
            read_map_value_once(&mut fields.payload_digest, "payload_digest", map)
        }
        OutboxIntentField::Headers => read_headers(fields, map, budget),
    }
}

fn finish_outbox<E>(
    fields: OutboxIntentFields<'_>,
    budget: &mut StructuralBudget,
) -> Result<OutboxIntent, E>
where
    E: serde::de::Error,
{
    let payload = fields.payload.ok_or_else(|| E::missing_field("payload"))?;
    budget.charge(payload.len()).map_err(E::custom)?;
    Ok(OutboxIntent {
        tenant: fields.tenant.ok_or_else(|| E::missing_field("tenant"))?,
        destination_scope_digest: fields
            .destination_scope_digest
            .ok_or_else(|| E::missing_field("destination_scope_digest"))?,
        topic: fields.topic.ok_or_else(|| E::missing_field("topic"))?,
        partition_key: fields
            .partition_key
            .ok_or_else(|| E::missing_field("partition_key"))?,
        event_schema: fields
            .event_schema
            .ok_or_else(|| E::missing_field("event_schema"))?,
        payload: RecordBytes::new(payload.to_vec()).map_err(E::custom)?,
        payload_digest: fields
            .payload_digest
            .ok_or_else(|| E::missing_field("payload_digest"))?,
        headers: fields.headers.ok_or_else(|| E::missing_field("headers"))?,
    })
}

impl<'de> Visitor<'de> for BudgetedOutboxIntentVisitor<'_> {
    type Value = OutboxIntent;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a named budgeted outbox intent")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        self.budget
            .charge(OUTBOX_FIXED_BUDGET)
            .map_err(A::Error::custom)?;
        let mut fields = OutboxIntentFields::new();
        while let Some(field) = map.next_key::<OutboxIntentField>()? {
            read_outbox_field(&mut fields, field, &mut map, self.budget)?;
        }
        finish_outbox::<A::Error>(fields, self.budget)
    }
}

impl<'de> MutationBudgetDeserialize<'de> for OutboxIntent {
    fn deserialize_budgeted<D>(
        deserializer: D,
        budget: &mut StructuralBudget,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BudgetedOutboxIntentVisitor { budget })
    }
}
