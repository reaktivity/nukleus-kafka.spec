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
package org.reaktivity.specification.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;

public class FetchIT
{
    private final K3poRule k3po = new K3poRule().addScriptRoot("scripts",
            "org/reaktivity/specification/kafka/fetch.v5");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout);

    @Test
    @Specification(
    {"${scripts}/compacted.bootstrap.historical.uses.cached.key.then.live/client",
     "${scripts}/compacted.bootstrap.historical.uses.cached.key.then.live/server"})
    public void shouldBootstrapTopicAndUseCachedKeyOffsetThenLive() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_HISTORICAL_FETCH_RESPONSE");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.bootstrap.uses.historical/client",
     "${scripts}/compacted.bootstrap.uses.historical/server"})
    public void shouldBootstrapTopicUsingHistoricalConnectionWhenNeeded() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.awaitBarrier("FIRST_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("WRITE_FIRST_LIVE_FETCH_RESPONSE");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.awaitBarrier("HISTORICAL_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("WRITE_HISTORICAL_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.delivers.deleted.messages/client",
     "${scripts}/compacted.delivers.deleted.messages/server"})
    public void shouldReceiveompactedDeletedMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.delivers.compacted.messages/client",
     "${scripts}/compacted.delivers.compacted.messages/server"})
    public void shouldReceiveCompactedMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.header.first.matches/client",
     "${scripts}/compacted.header.first.matches/server"})
    public void shouldReceiveCompactedMessagesMatchingHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.header.first.matches.repeated/client",
     "${scripts}/compacted.header.first.matches.repeated/server"})
    public void shouldReceiveCompactedMessagesMatchingHeaderFetchRepeated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.header.matches.then.updated/client",
     "${scripts}/compacted.header.matches.then.updated/server"})
    public void shouldReceiveCompactedMessagesWithHeaderUpdated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.header.matches.removed.in.subsequent.response/client",
     "${scripts}/compacted.header.matches.removed.in.subsequent.response/server"})
    public void shouldReceiveCompactedMessagesWithHeaderThenRemoved() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.header.repeated.tombstone/client",
     "${scripts}/compacted.header.repeated.tombstone/server"})
    public void shouldReceiveCompactedMessagesWithRepeatedHeaderThenOneValueUpdated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.after.unsubscribe/client",
     "${scripts}/compacted.historical.uses.cached.key.after.unsubscribe/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyAfterUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.then.latest.offset/client",
     "${scripts}/compacted.historical.uses.cached.key.then.latest.offset/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenLatestOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.then.latest.offset.no.historical/client",
     "${scripts}/compacted.historical.uses.cached.key.then.latest.offset.no.historical/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenLatestOffsetNoHistorical() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.then.live/client",
     "${scripts}/compacted.historical.uses.cached.key.then.live/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenLive() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.then.live.no.historical/client",
     "${scripts}/compacted.historical.uses.cached.key.then.live.no.historical/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenLiveNoHistorical() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.then.live.after.null.message/client",
     "${scripts}/compacted.historical.uses.cached.key.then.live.after.null.message/server"})
    public void shouldReceiveCompactedMessagesFromLiveStreamAfterCachedKeyRemovedByNullMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/client",
     "${scripts}/compacted.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/server"})
    public void shouldReceiveCompactedMessagesFromLiveStreamAfterOffsetTooEarlyAndCachedKeyRemovedByNullMessage()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.historical.uses.cached.key.then.zero.offset/client",
     "${scripts}/compacted.historical.uses.cached.key.then.zero.offset/server"})
    public void shouldReceiveCompactedMessagesUsingCachedKeyThenZerotOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.message/client",
     "${scripts}/compacted.message/server"})
    public void shouldReceiveCompactedMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.message.delayed.describe.response/client",
     "${scripts}/compacted.message.delayed.describe.response/server"})
    public void shouldReceiveCompactedMessageDelayedDescribeResponse() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_DESCRIBE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages/client",
     "${scripts}/compacted.messages/server"})
    public void shouldReceiveMessagesFromCompactedTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.tombstone.repeated/client",
     "${scripts}/compacted.messages.tombstone.repeated/server"})
    public void shouldReceiveRepeatedTombstoneMessagesFromCompactedTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.tombstone.then.message/client",
     "${scripts}/compacted.tombstone.then.message/server"})
    public void shouldReceiveTombstoneAndMessageFromCompactedTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.first.exceeds.256.bytes/client",
     "${scripts}/compacted.messages.first.exceeds.256.bytes/server"})
    public void shouldReceiveLargeCompactedMessageFilteredByKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.first.exceeds.256.bytes.repeated/client",
     "${scripts}/compacted.messages.first.exceeds.256.bytes.repeated/server"})
    public void shouldReceiveLargeCompactedMessageFilteredByKeyFetchRepeated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.header/client",
     "${scripts}/compacted.messages.header/server"})
    public void shouldReceiveCompactedMessagesFilteredByHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.header.multiple.values/client",
     "${scripts}/compacted.messages.header.multiple.values/server"})
    public void shouldReceiveCompactedMessagesFilteredByHeaderWithMultipleValues() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.headers/client",
     "${scripts}/compacted.messages.headers/server"})
    public void shouldReceiveCompactedMessagesFilteredByMultipleHeaders() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.historical/client",
     "${scripts}/compacted.messages.historical/server"})
    public void shouldReceiveHistoricalCompactedMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.messages.multiple.nodes/client",
     "${scripts}/compacted.messages.multiple.nodes/server"})
    public void shouldReceiveCompactedMessagesFromMultipleNodes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.message.multiple.networks/client",
     "${scripts}/compacted.message.multiple.networks/server"})
    public void shouldReceiveCompactedMessagesFromMultipleNetworks() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.message.multiple.topics/client",
     "${scripts}/compacted.message.multiple.topics/server"})
    public void shouldReceiveCompactedMessagesFromMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/compacted.zero.offset/client",
     "${scripts}/compacted.zero.offset/server"})
    public void shouldAttachToCompactedTopicAtOffsetZero() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/distinct.offset.messagesets.fanout/client",
     "${scripts}/distinct.offset.messagesets.fanout/server"})
    public void shouldFanoutMessageSetsAtDistinctOffsets() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_THREE");
        k3po.notifyBarrier("SERVER_DELIVER_HISTORICAL_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/distinct.offsets.message.fanout/client",
     "${scripts}/distinct.offsets.message.fanout/server"})
    public void shouldHandleParallelSubscribesAtDistinctOffsets() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.awaitBarrier("SERVER_LIVE_REQUEST_RECEIVED");
        k3po.notifyBarrier("SERVER_DELIVER_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fanout.with.historical.message/client",
     "${scripts}/fanout.with.historical.message/server"})
    public void shouldFanoutUsingHistoricalConnection() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fanout.with.historical.messages/client",
     "${scripts}/fanout.with.historical.messages/server"})
    public void shouldFanoutDiscardingHistoricalMessageToJoinLiveStream() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fanout.with.slow.consumer/client",
     "${scripts}/fanout.with.slow.consumer/server"})
    public void shouldFanoutWithSlowConsumer() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_LIVE_RESPONSE_TWO");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.and.hash.code.picks.partition.zero/client",
     "${scripts}/fetch.key.and.hash.code.picks.partition.zero/server"})
    public void shouldReceiveMessageUsingFetchKeyAndExplicitHashCode() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.and.no.key.multiple.partitions/client",
     "${scripts}/fetch.key.and.no.key.multiple.partitions/server"})
    public void shouldReceiveMessagesWithAndWithoutMessageKeysMultiplePartitions() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("MESSAGE_ONE_RECEIVED");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.and.no.key.multiple.partitions.unsubscribe/client",
     "${scripts}/fetch.key.and.no.key.multiple.partitions.unsubscribe/server"})
    public void shouldHandleUnsubscribeWithAndWithoutMessageKeysMultiplePartitions() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("CLIENT_TWO_UNSUBSCRIBED");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.default.partitioner.picks.partition.one/client",
     "${scripts}/fetch.key.default.partitioner.picks.partition.one/server"})
    public void shouldReceiveMessageUsingFetchKeyAndDefaultPartitioner() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.historical.does.not.use.cached.key/client",
     "${scripts}/fetch.key.historical.does.not.use.cached.key/server"})
    public void shouldReceiveMessageFromNonCompactedTopicWithoutCachingKeyOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.multiple.matches.flow.controlled/client",
     "${scripts}/fetch.key.multiple.matches.flow.controlled/server"})
    public void shouldReceiveMessagesMatchingFetchKeyFlowControlled() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.multiple.record.batches.first.matches/client",
     "${scripts}/fetch.key.multiple.record.batches.first.matches/server"})
    public void shouldReceiveMessageMatchingFetchKeyWithMultipleRecordBatches() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.multiple.record.batches.no.matches/client",
     "${scripts}/fetch.key.multiple.record.batches.no.matches/server"})
    public void shouldReceiveNoMessagesMatchingFetchKeyWithMultipleRecordBatches() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.no.matches/client",
     "${scripts}/fetch.key.no.matches/server"})
    public void shouldReceiveNoMessagesMatchingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.nonzero.offset.first.matches/client",
     "${scripts}/fetch.key.nonzero.offset.first.matches/server"})
    public void shouldReceiveMessageMatchingFetchKeyFirstNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.three.matches.flow.controlled/client",
     "${scripts}/fetch.key.three.matches.flow.controlled/server"})
    public void shouldReceiveMessagesThreeMatchingFetchKeyFlowControlled() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.high.water.mark.offset.first.matches/client",
     "${scripts}/fetch.key.high.water.mark.offset.first.matches/server"})
    public void shouldReceiveLiveMessageMatchingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.zero.offset.first.matches/client",
     "${scripts}/fetch.key.zero.offset.first.matches/server"})
    public void shouldReceiveMessageMatchingFetchKeyFirst() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.zero.offset.last.matches/client",
     "${scripts}/fetch.key.zero.offset.last.matches/server"})
    public void shouldReceiveMessageMatchingFetchKeyLast() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.zero.offset.multiple.matches/client",
     "${scripts}/fetch.key.zero.offset.multiple.matches/server"})
    public void shouldReceiveMultipleMessagesMatchingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.zero.offset.multiple.matches.historical/client",
     "${scripts}/fetch.key.zero.offset.multiple.matches.historical/server"})
    public void shouldReceiveMultipleHistoricalMessagesMatchingFetchKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.and.fetch.key.zero.offset.first.matches/client",
     "${scripts}/header.and.fetch.key.zero.offset.first.matches/server"})
    public void shouldReceiveMessageMatchingFetchKeyAndHeaderFirst() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/headers.and.fetch.key.zero.offset.first.matches/client",
     "${scripts}/headers.and.fetch.key.zero.offset.first.matches/server"})
    public void shouldReceiveMessageMatchingFetchKeyAndHeadersFirst() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.first.message.has.empty.header.value/client",
     "${scripts}/header.first.message.has.empty.header.value/server"})
    public void shouldReceiveMessagesMatchingEmptyHeaderValue() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/headers.zero.offset.multiple.matches.historical/client",
     "${scripts}/headers.zero.offset.multiple.matches.historical/server"})
    public void shouldReceiveHistoricalMessagesMatchingHeaders() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.large.equals.window.then.small.messages.multiple.partitions/client",
     "${scripts}/header.large.equals.window.then.small.messages.multiple.partitions/server"})
    public void shouldReceiveMatchingLargeEqualsWindowFragmentedAndSmallMessagesFromMultiplePartitions()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.large.exceeding.window.then.small.messages.multiple.partitions/client",
     "${scripts}/header.large.exceeding.window.then.small.messages.multiple.partitions/server"})
    public void shouldReceiveMatchingLargeExceedsWindowFragmentedAndSmallMessagesFromMultiplePartitions()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.zero.offset.first.matches/client",
     "${scripts}/header.zero.offset.first.matches/server"})
    public void shouldReceiveMessageMatchingHeaderFirst() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.zero.offset.last.matches/client",
     "${scripts}/header.zero.offset.last.matches/server"})
    public void shouldReceiveMessageMatchingHeaderLast() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.zero.offset.multiple.matches/client",
     "${scripts}/header.zero.offset.multiple.matches/server"})
    public void shouldReceiveMultipleMessagesMatchingHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.zero.offset.repeated/client",
     "${scripts}/header.zero.offset.repeated/server"})
    public void shouldReceiveMatchingMessageWithRepeatedHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/high.water.mark.offset/client",
     "${scripts}/high.water.mark.offset/server"})
    public void shouldRequestMessagesAtHighWatermarkOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/high.water.mark.offset.multiple.topics/client",
     "${scripts}/high.water.mark.offset.multiple.topics/server"})
    public void shouldRequestMessagesAtHighWatermarkOffsetFromMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/historical.connection.aborted/client",
     "${scripts}/historical.connection.aborted/server"})
    public void shouldReconnectRequeryMetadataAndContinueReceivingMessagesWhenHistoricalFetchConnectionIsAborted()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/historical.connection.closed/client",
     "${scripts}/historical.connection.closed/server"})
    public void shouldReconnectAndReceiveMessagesWhenHistoricalFetchConnectionIsClosed() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/historical.connection.reset/client",
     "${scripts}/historical.connection.reset/server"})
    public void shouldReconnectRequeryMetadataAndContinueReceivingMessagesWhenHistoricalFetchConnectionIsReset()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/large.equals.window.then.small.messages.multiple.partitions/client",
     "${scripts}/large.equals.window.then.small.messages.multiple.partitions/server"})
    public void shouldDeliverLargeMessageFillingWindowThenSmallMessagesFromMultiplePartitions()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/large.exceeding.window.then.small.messages.multiple.partitions/client",
     "${scripts}/large.exceeding.window.then.small.messages.multiple.partitions/server"})
    public void shouldNotDeliverMessageFromPartitionDifferentFromFragmentedMessageUntilFragmentedFullyWritten()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/large.then.small.other.partition.first.in.next.response/client",
     "${scripts}/large.then.small.other.partition.first.in.next.response/server"})
    public void shouldReceiveLargeFragmentedAndSmallMessagesFromMultiplePartitions()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.broker.restarted.with.recreated.topic/client",
     "${scripts}/live.fetch.broker.restarted.with.recreated.topic/server"})
    public void shouldReceiveMessagesAcrossBrokerRestartWithRecreatedTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SHUTDOWN_BROKER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.connection.aborted/client",
     "${scripts}/live.fetch.connection.aborted/server"})
    public void shouldReconnectRequeryPartitionMetadataAndContinueReceivingMessagesWhenLiveFetchConnectionIsAborted()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.connection.closed/client",
     "${scripts}/live.fetch.connection.closed/server"})
    public void shouldReconnectAndReceiveMessagesWhenLiveFetchConnectionIsClosed() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.connection.closed.then.reset/client",
     "${scripts}/live.fetch.connection.closed.then.reset/server"})
    public void shouldRefreshMetadataAndReceiveMessagesWhenLiveFetchConnectionIsClosedThenResetThenReconnected() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.connection.fails.during.metadata.refresh/client",
     "${scripts}/live.fetch.connection.fails.during.metadata.refresh/server"})
    public void shouldHandleFetchConnectionFailureDuringMetadataRefresh() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.awaitBarrier("DESCRIBE_CONFIGS_REQUEST_RECEIVED");
        k3po.notifyBarrier("ABORT_CONNECTION_TWO");
        k3po.awaitBarrier("CONNECTION_TWO_ABORTED");
        k3po.notifyBarrier("WRITE_DESCRIBE_CONFIGS_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.connection.reset/client",
     "${scripts}/live.fetch.connection.reset/server"})
    public void shouldContinueReceivingMessagesWhenTopicFetchResponseIsErrorCode3NotFoundButMetadataIsFound() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.error.recovered/client",
     "${scripts}/live.fetch.error.recovered/server"})
    @ScriptProperty("errorCode \"6s\"")
    public void shouldContinueReceivingMessagesWhenTopicFetchResponseIsRecoverableError() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.error.recovered.partition.added/client",
     "${scripts}/live.fetch.error.recovered.partition.added/server"})
    public void shouldContinueDeliveringMessagesWhenPartitionIsAdded() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.error.then.metadata.error/client",
     "${scripts}/live.fetch.error.then.metadata.error/server"})
    public void shouldHandleFetchErrorFollowedByMetadataRefreshError() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.topic.not.found.permanently/client",
     "${scripts}/live.fetch.topic.not.found.permanently/server"})
    public void shouldKeepTryingToFindMetadataWhenTopicIsPermanentlyDeleted() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.topic.not.found.recovered/client",
     "${scripts}/live.fetch.topic.not.found.recovered/server"})
    public void shouldEndWithOffsetZeroWhenTopicIsDeletedThenRecreated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/message.size.exceeds.max.partition.bytes/client",
     "${scripts}/message.size.exceeds.max.partition.bytes/server"})
    public void shouldHandleMessageExceedingConfiguredMaximum() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/nonzero.offset/client",
     "${scripts}/nonzero.offset/server"})
    public void shouldRequestMessagesAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/nonzero.offset.message/client",
     "${scripts}/nonzero.offset.message/server"})
    public void shouldReceiveMessageAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/nonzero.offset.messages/client",
     "${scripts}/nonzero.offset.messages/server"})
    public void shouldReceiveMessagesAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/offset.too.early.message/client",
     "${scripts}/offset.too.early.message/server"})
    public void shouldRefetchUsingReportedFirstOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/offset.too.early.multiple.topics/client",
     "${scripts}/offset.too.early.multiple.topics/server"})
    public void shouldRefetchUsingReportedFirstOffsetOnMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_METADATA_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_DESCRIBE_CONFIGS_RESPONSE");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/record.batch.ends.with.deleted.record/client",
     "${scripts}/record.batch.ends.with.deleted.record/server"})
    public void shouldReportLastMessageOffsetFromRecordBatch() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/record.batch.ends.with.truncated.record.length/client",
     "${scripts}/record.batch.ends.with.truncated.record.length/server"})
    public void shouldReceiveMessageWithTruncatedRecordLength() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/record.batch.ends.with.truncated.record/client",
     "${scripts}/record.batch.ends.with.truncated.record/server"})
    public void shouldReceiveMessageWithTruncatedRecord() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/record.batch.truncated/client",
     "${scripts}/record.batch.truncated/server"})
    public void shouldReceiveMessageRecordBatchTruncatedInItsOwnFields() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/record.batch.truncated.at.record.boundary/client",
     "${scripts}/record.batch.truncated.at.record.boundary/server"})
    public void shouldReceiveMessageRecordBatchTruncatedOnRecordBoundary() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/record.set.size.zero.record.too.large/client",
     "${scripts}/record.set.size.zero.record.too.large/server"})
    public void shouldReceiveMessageAfterSkippingTooLargeRecord() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.then.specified.offset.then.live.messages/client",
     "${scripts}/live.then.specified.offset.then.live.messages/server"})
    public void shouldReceiveLiveHistoricalThenLiveMessagesFromStreamingTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_HISTORICAL_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_THIRD_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/specified.offset.then.live.messages/client",
     "${scripts}/specified.offset.then.live.messages/server"})
    public void shouldReceiveHistoricalThenLiveMessagesFromStreamingTopic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_HISTORICAL_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset/client",
     "${scripts}/zero.offset/server"})
    public void shouldRequestMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.first.record.batch.large.requires.3.fetches/client",
     "${scripts}/zero.offset.first.record.batch.large.requires.3.fetches/server"})
    public void shouldReceiveRecordBatchRequiringRepeatedFetches() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.no.messages/client",
     "${scripts}/zero.offset.no.messages/server"})
    public void shouldRequestMessagesAtZeroOffsetAndNotSkipEmptyRecordBatch() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.message/client",
     "${scripts}/zero.offset.message/server"})
    public void shouldReceiveMessageAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.message.single.partition.multiple.nodes/client",
    "${scripts}/zero.offset.message.single.partition.multiple.nodes/server"})
    public void shouldReceiveMessageAtZeroOffsetSinglePartitionMultipleNodes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages/client",
     "${scripts}/zero.offset.messages/server"})
    public void shouldReceiveMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.fanout/client",
     "${scripts}/zero.offset.messages.fanout/server"})
    public void shouldFanoutMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.first.exceeds.256.bytes/client",
     "${scripts}/zero.offset.messages.first.exceeds.256.bytes/server"})
    public void shouldReceiveLargeThenSmallMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.first.exceeds.256.bytes.repeated/client",
     "${scripts}/zero.offset.messages.first.exceeds.256.bytes.repeated/server"})
    public void shouldReceiveLargeFragmentedThenSmallMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.group.budget/client",
     "${scripts}/zero.offset.messages.group.budget/server"})
    public void shouldFanoutMessagesAtZeroOffsetUsingGroupBudget() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.multiple.nodes/client",
     "${scripts}/zero.offset.messages.multiple.nodes/server"})
    public void shouldReceiveMessagesAtZeroOffsetMultipleNodes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.multiple.partitions/client",
    "${scripts}/zero.offset.messages.multiple.partitions/server"})
    public void shouldReceiveMessagesAtZeroOffsetFromMultiplPartitions() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.multiple.partitions.partition.1/client",
    "${scripts}/zero.offset.messages.multiple.partitions.partition.1/server"})
    public void shouldReceiveMessagesAtZeroOffsetFromMultiplPartitionsPartition1() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messagesets/client",
     "${scripts}/zero.offset.messagesets/server"})
    public void shouldReceiveMessageSetsAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messagesets.fanout/client",
    "${scripts}/zero.offset.messagesets.fanout/server"})
    public void shouldFanoutMessageSetsAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.finish();
    }
}
