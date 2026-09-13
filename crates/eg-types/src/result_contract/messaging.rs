//! Declared results of the `messaging` contract domain.

method_results! {
    visit_messaging;
    DeclareExchange(DeclareExchange) => Text<String>;
    DeleteExchange(DeleteExchange) => Bool<bool>;
    BindQueue(BindQueue) => Text<String>;
    UnbindQueue(UnbindQueue) => Bool<bool>;
    Publish(Publish) => Count<u64>;
    DeclareQueue(DeclareQueue) => Text<String>;
    PublishEx(PublishEx) => Count<u64>;
    BrokerAck(BrokerAck) => Bool<bool>;
    BrokerReject(BrokerReject) => Text<String>;
    SweepExpired(SweepExpired) => Count<u64>;
    StreamDeclare(StreamDeclare) => Text<String>;
    StreamPublish(StreamPublish) => Count<u64>;
    StreamTrim(StreamTrim) => Count<u64>;
    StreamCommitOffset(StreamCommitOffset) => Text<String>;
    BrokerAckTag(BrokerAckTag) => Bool<bool>;
    BrokerNackTag(BrokerNackTag) => Text<String>;
    BrokerRenewTag(BrokerRenewTag) => Bool<bool>;
    RegisterContinuousQuery(RegisterContinuousQuery) => Text<String>;
    DropContinuousQuery(DropContinuousQuery) => Bool<bool>;
    RegisterTrigger(RegisterTrigger) => Text<String>;
    DropTrigger(DropTrigger) => Bool<bool>;
    CepSubscribe(CepSubscribe) => Count<u64>;
    CepUnsubscribe(CepUnsubscribe) => Bool<bool>;
    GetChannelMembers(GetChannelMembers) => Ids<Vec<String>>;
}
