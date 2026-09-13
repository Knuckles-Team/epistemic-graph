//! Declared results of the `messaging` contract domain.

use crate::messaging_wire::{
    ChannelCreated, ChannelDeparture, ChannelMessage, ChannelSummary, ConfirmToken,
    IdempotentPublish,
};
#[cfg(feature = "streaming")]
use crate::wire::{
    CdcReadResult, ContinuousQueryResult, FiredTriggersResult, TriggerInfo, WatchBatch,
};

method_results! {
    visit_messaging;
    DeclareExchange(DeclareExchange) => Text<String>;
    DeleteExchange(DeleteExchange) => Bool<bool>;
    BindQueue(BindQueue) => Text<String>;
    UnbindQueue(UnbindQueue) => Bool<bool>;
    Publish(Publish) => Count<u64>;
    DeclareQueue(DeclareQueue) => Text<String>;
    PublishEx(PublishEx) => Count<u64>;
    // `(message_node_id, message_properties)` of the claimed message, or nil.
    BrokerConsume(BrokerConsume) => Raw<Option<(String, serde_json::Value)>>;
    BrokerAck(BrokerAck) => Bool<bool>;
    BrokerReject(BrokerReject) => Text<String>;
    SweepExpired(SweepExpired) => Count<u64>;
    StreamDeclare(StreamDeclare) => Text<String>;
    StreamPublish(StreamPublish) => Count<u64>;
    // `(offset, payload)` pairs; the payload is an array of byte values.
    StreamRead(StreamRead) => Raw<Vec<(i64, Vec<u8>)>>;
    StreamTrim(StreamTrim) => Count<u64>;
    StreamCommitOffset(StreamCommitOffset) => Text<String>;
    StreamCommittedOffset(StreamCommittedOffset) => Raw<Option<i64>>;
    PublishConfirmed(PublishConfirmed) => Raw<ConfirmToken>;
    PublishIdempotent(PublishIdempotent) => Raw<IdempotentPublish>;
    BrokerAckTag(BrokerAckTag) => Bool<bool>;
    BrokerNackTag(BrokerNackTag) => Text<String>;
    BrokerRenewTag(BrokerRenewTag) => Bool<bool>;
    CreateChannel(CreateChannel) => Json<ChannelCreated>;
    JoinChannel(JoinChannel) => Text<String>;
    LeaveChannel(LeaveChannel) => Json<ChannelDeparture>;
    CloseChannel(CloseChannel) => Json<ChannelDeparture>;
    SendMessage(SendMessage) => Text<String>;
    GetChannelMessages(GetChannelMessages) => Json<Vec<ChannelMessage>>;
    ListChannels(ListChannels) => Json<Vec<ChannelSummary>>;
    GetChannelMembers(GetChannelMembers) => Ids<Vec<String>>;
    #[cfg(feature = "streaming")]
    CdcRead(CdcRead) => Raw<CdcReadResult>;
    RegisterContinuousQuery(RegisterContinuousQuery) => Text<String>;
    #[cfg(feature = "streaming")]
    ReadContinuousQuery(ReadContinuousQuery) => Raw<ContinuousQueryResult>;
    DropContinuousQuery(DropContinuousQuery) => Bool<bool>;
    #[cfg(feature = "streaming")]
    Watch(Watch) => Raw<WatchBatch>;
    RegisterTrigger(RegisterTrigger) => Text<String>;
    DropTrigger(DropTrigger) => Bool<bool>;
    #[cfg(feature = "streaming")]
    ListTriggers(ListTriggers) => Raw<Vec<TriggerInfo>>;
    #[cfg(feature = "streaming")]
    FiredTriggers(FiredTriggers) => Raw<FiredTriggersResult>;
    CepSubscribe(CepSubscribe) => Count<u64>;
    CepUnsubscribe(CepUnsubscribe) => Bool<bool>;
}
