/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.specification.nukleus.kafka.streams;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;

public class FetchIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("scripts", "org/reaktivity/specification/nukleus/kafka/streams/fetch");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout);

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${scripts}/begin.ext.missing/client",
        "${scripts}/begin.ext.missing/server"})
    public void shouldRejectWhenBeginExtMissing() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.bootstrap.historical.uses.cached.key.then.live/client",
        "${scripts}/compacted.bootstrap.historical.uses.cached.key.then.live/server"})
    public void shouldReceiveCompactedMessagesAfterBootstrapUsingCachedKeyThenLive() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.delivers.deleted.messages/client",
        "${scripts}/compacted.delivers.deleted.messages/server"})
    public void shouldReceiveCompactedDeletedMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.delivers.compacted.messages/client",
        "${scripts}/compacted.delivers.compacted.messages/server"})
    public void shouldReceiveCompactedMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.historical.uses.cached.key.after.unsubscribe/client",
        "${scripts}/compacted.historical.uses.cached.key.after.unsubscribe/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyAfterUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.historical.uses.cached.key.then.latest.offset/client",
        "${scripts}/compacted.historical.uses.cached.key.then.latest.offset/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenLatestOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.historical.uses.cached.key.then.live/client",
        "${scripts}/compacted.historical.uses.cached.key.then.live/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenLive() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.historical.uses.cached.key.then.live.after.null.message/client",
        "${scripts}/compacted.historical.uses.cached.key.then.live.after.null.message/server"})
    public void shouldReceiveCompactedMessagesFromLiveStreamAfterCachedKeyRemovedByNullMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/client",
        "${scripts}/compacted.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/server"})
    public void shouldReceiveCompactedMessagesFromLiveStreamAfterOffsetTooEarlyAndCachedKeyRemovedByNullMessage()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.historical.uses.cached.key.then.zero.offset/client",
        "${scripts}/compacted.historical.uses.cached.key.then.zero.offset/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenZerotOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.message/client",
        "${scripts}/compacted.message/server"})
    public void shouldReceiveCompactedMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.message.fanout/client",
        "${scripts}/compacted.message.fanout/server"})
    public void shouldReceiveCompactedMessageWithFanout() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.message.multiple.topics/client",
        "${scripts}/compacted.message.multiple.topics/server"})
    public void shouldReceiveCompactedMessagesFromMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.message.subscribed.to.key/client",
        "${scripts}/compacted.message.subscribed.to.key/server"})
    public void shouldReceiveCompactedMessageWhenSubscribedToKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.messages/client",
        "${scripts}/compacted.messages/server"})
    public void shouldReceiveMessagesFromCompactedTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.messages.header/client",
        "${scripts}/compacted.messages.header/server"})
    public void shouldReceiveCompactedMessagesFilteredByHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.messages.header.multiple.clients/client",
        "${scripts}/compacted.messages.header.multiple.clients/server"})
    public void shouldReceiveCompactedMessagesFilteredByHeaderOnMultipleClients() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.messages.header.multiple.routes/client",
        "${scripts}/compacted.messages.header.multiple.routes/server"})
    public void shouldReceiveCompactedMessagesFilteredByHeaderOnMultipleRoutes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.messages.one.per.key/client",
        "${scripts}/compacted.messages.one.per.key/server"})
    public void shouldReceiveMessagesFromCompactedTopicUltraCompacted() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.messages.historical/client",
        "${scripts}/compacted.messages.historical/server"})
    public void shouldReceiveCompactedHistoricalMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.header.messages.and.tombstone/client",
        "${scripts}/compacted.header.messages.and.tombstone/server"})
    public void shouldReceiveCompactedMessagesWithTombstone() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.messages.multiple.nodes/client",
        "${scripts}/compacted.messages.multiple.nodes/server"})
    public void shouldReceiveCompactedMessagesFromMultipleNodes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/distinct.offset.messagesets.fanout/client",
        "${scripts}/distinct.offset.messagesets.fanout/server"})
    public void shouldFanoutMessageSetsAtDistinctOffsets() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/distinct.offsets.message.fanout/client",
        "${scripts}/distinct.offsets.message.fanout/server"})
    public void shouldHandleParallelSubscribesAtDistinctOffsets() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fanout.with.historical.message/client",
        "${scripts}/fanout.with.historical.message/server"})
    public void shouldFanoutUsingHistoricalConnection() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fanout.with.historical.messages/client",
        "${scripts}/fanout.with.historical.messages/server"})
    public void shouldFanoutDiscardingHistoricalMessageToJoinLiveStream() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fanout.with.slow.consumer/client",
        "${scripts}/fanout.with.slow.consumer/server"})
    public void shouldFanoutWithSlowConsumer() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.and.no.key.messages/client",
        "${scripts}/fetch.key.and.no.key.messages/server"})
    public void shouldFanoutToConsumersWithAndWithoutFetchKeys() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.and.no.key.unsubscribe/client",
        "${scripts}/fetch.key.and.no.key.unsubscribe/server"})
    public void shouldHandleSubscribeWithAndWithoutKeyAndUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("PARTITION_ONE_FETCH_REQUEST_RECEIVED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.and.hash.code.picks.partition.zero/client",
        "${scripts}/fetch.key.and.hash.code.picks.partition.zero/server"})
    public void shouldReceiveMessageUsingFetchKeyAndExplicitHashCode() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.default.partitioner.picks.partition.one/client",
        "${scripts}/fetch.key.default.partitioner.picks.partition.one/server"})
    public void shouldReceiveMessageUsingFetchKeyWithDefaultHashCode() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.nonzero.offset.message/client",
        "${scripts}/fetch.key.nonzero.offset.message/server"})
    public void shouldReceiveMessageUsingFetchKeyAndNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.zero.offset.message/client",
        "${scripts}/fetch.key.zero.offset.message/server"})
    public void shouldReceiveMessageUsingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.no.offsets.message/client",
        "${scripts}/fetch.key.no.offsets.message/server"})
    public void shouldReceiveMessagesWithoutUsingKeyOffsetsCache() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.no.offsets.message/client",
        "${scripts}/fetch.key.no.offsets.message/server"})
    public void shouldReceiveMessageUsingFetchKeyWithEmptyOffsetsArray() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.zero.offset.messages/client",
        "${scripts}/fetch.key.zero.offset.messages/server"})
    public void shouldReceiveMessagesUsingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.zero.offset.messages.historical/client",
        "${scripts}/fetch.key.zero.offset.messages.historical/server"})
    public void shouldReceiveHistoricalMessagesUsingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.zero.offset.no.messages/client",
        "${scripts}/fetch.key.zero.offset.no.messages/server"})
    public void shouldReceiveNoMessagesUsingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/fetch.key.zero.offset.three.messages/client",
        "${scripts}/fetch.key.zero.offset.three.messages/server"})
    public void shouldReceiveThreeMessagesUsingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/header.and.fetch.key.zero.offset.message/client",
        "${scripts}/header.and.fetch.key.zero.offset.message/server"})
    public void shouldReceiveMessageUsingFetchKeyAndHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/header.zero.offset.message/client",
        "${scripts}/header.zero.offset.message/server"})
    public void shouldReceiveMessageUsingHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/header.zero.offset.messages/client",
        "${scripts}/header.zero.offset.messages/server"})
    public void shouldReceiveMessagesUsingHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/headers.and.fetch.key.zero.offset.message/client",
        "${scripts}/headers.and.fetch.key.zero.offset.message/server"})
    public void shouldReceiveMessageUsingFetchKeyAndMultipleHeaders() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/headers.zero.offset.messages.historical/client",
        "${scripts}/headers.zero.offset.messages.historical/server"})
    public void shouldReceiveHistoricalMessagesUsingHeaders() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/invalid.fetch.key.and.multiple.offsets/client",
        "${scripts}/invalid.fetch.key.and.multiple.offsets/server"})
    public void shouldRejectInvalidBeginExWithFetchKeyAndMultipleOffsets() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/invalid.missing.fetch.key/client",
        "${scripts}/invalid.missing.fetch.key/server"})
    public void shouldRejectInvalidBeginExWithMissingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/invalid.more.than.one.fetch.key.hash/client",
        "${scripts}/invalid.more.than.one.fetch.key.hash/server"})
    public void shouldRejectInvalidBeginExWithMoreThanOneFetchKeyHash() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/invalid.topic.name/client",
        "${scripts}/invalid.topic.name/server"})
    public void shouldRejectInvalidTopicName() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/no.offsets.message/client",
        "${scripts}/no.offsets.message/server"})
    public void shouldReceiveMessageAtZeroOffsetWithEmptyOffsetsArray() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/nonzero.offset/client",
        "${scripts}/nonzero.offset/server"})
    public void shouldRequestMessagesAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/nonzero.offset.message/client",
        "${scripts}/nonzero.offset.message/server"})
    public void shouldReceiveMessageAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/nonzero.offset.messages/client",
        "${scripts}/nonzero.offset.messages/server"})
    public void shouldReceiveMessagesAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/offset.too.early.message/client",
        "${scripts}/offset.too.early.message/server"})
    public void shouldRefetchUsingReportedFirstOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/offset.too.early.multiple.topics/client",
        "${scripts}/offset.too.early.multiple.topics/server"})
    public void shouldRefetchUsingReportedFirstOffsetOnMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/record.batch.ends.with.deleted.record/client",
        "${scripts}/record.batch.ends.with.deleted.record/server"})
    public void shouldReceiveMessagesWithOffsetGap() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/record.batch.truncated/client",
        "${scripts}/record.batch.truncated/server"})
    public void shouldReceiveMessageWithTruncatedRecord() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/topic.name.not.equals.route.ext/client",
        "${scripts}/topic.name.not.equals.route.ext/server"})
    public void shouldRejectTopicNameNutEqualToRoutedTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unknown.topic.name/client",
        "${scripts}/unknown.topic.name/server"})
    public void shouldRejectUnknownTopicName() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset/client",
        "${scripts}/zero.offset/server"})
    public void shouldRequestMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.and.reset/client",
        "${scripts}/zero.offset.and.reset/server"})
    public void shouldUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("FETCH_REQUEST_RECEIVED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.large.message/client",
        "${scripts}/zero.offset.large.message/server"})
    public void shouldReceiveLargeMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.large.response/client",
        "${scripts}/zero.offset.large.response/server"})
    public void shouldReceiveLargResponse() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.message/client",
        "${scripts}/zero.offset.message/server"})
    public void shouldReceiveMessageAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.messages/client",
        "${scripts}/zero.offset.messages/server"})
    public void shouldReceiveMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.messages.multiple.partitions/client",
        "${scripts}/zero.offset.messages.multiple.partitions/server"})
    public void shouldReceiveMessagesAtZeroOffsetMultiplePartitions() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.messages.multiple.partitions.partition.1/client",
        "${scripts}/zero.offset.messages.multiple.partitions.partition.1/server"})
    public void shouldReceiveMessagesAtZeroOffsetMultiplePartitionsPartition1() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.messages.multiple.partitions.max.bytes.256/client",
        "${scripts}/zero.offset.messages.multiple.partitions.max.bytes.256/server"})
    public void shouldReceiveMessagesAtZeroOffsetMultiplePartitionsMaxBytes256() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.messagesets/client",
        "${scripts}/zero.offset.messagesets/server"})
    public void shouldReceiveMessageSetsAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.messages.fanout/client",
        "${scripts}/zero.offset.messages.fanout/server"})
    public void shouldFanoutMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
            "${scripts}/zero.offset.messages.group.budget/client",
            "${scripts}/zero.offset.messages.group.budget/server"})
    public void shouldFanoutMessagesAtZeroOffsetUsingGroupBudget() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
            "${scripts}/zero.offset.messages.group.budget.reset/client",
            "${scripts}/zero.offset.messages.group.budget.reset/server"})
    public void shouldFanoutMessagesAtZeroOffsetUsingGroupBudgetReset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/zero.offset.messagesets.fanout/client",
        "${scripts}/zero.offset.messagesets.fanout/server"})
    public void shouldFanoutMessageSetsAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }
}
