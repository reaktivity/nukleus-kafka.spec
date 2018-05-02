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
    {"${scripts}/distinct.offsets.message.fanout/client", "${scripts}/distinct.offsets.message.fanout/server"})
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
    {"${scripts}/fanout.with.historical.message/client", "${scripts}/fanout.with.historical.message/server"})
    public void shouldFanoutUsingHistoricalConnection() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fanout.with.historical.messages/client", "${scripts}/fanout.with.historical.messages/server"})
    public void shouldFanoutDiscardingHistoricalMessageToJoinLiveStream() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fanout.with.slow.consumer/client", "${scripts}/fanout.with.slow.consumer/server"})
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
    {"${scripts}/fetch.key.no.matches/client", "${scripts}/fetch.key.no.matches/server"})
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
    {"${scripts}/fetch.key.zero.offset.first.matches/client", "${scripts}/fetch.key.zero.offset.first.matches/server"})
    public void shouldReceiveMessageMatchingFetchKeyFirst() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/fetch.key.zero.offset.last.matches/client", "${scripts}/fetch.key.zero.offset.last.matches/server"})
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
    {"${scripts}/header.zero.offset.first.matches/client", "${scripts}/header.zero.offset.first.matches/server"})
    public void shouldReceiveMessageMatchingHeaderFirst() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.zero.offset.last.matches/client", "${scripts}/header.zero.offset.last.matches/server"})
    public void shouldReceiveMessageMatchingHeaderLast() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/header.zero.offset.multiple.matches/client", "${scripts}/header.zero.offset.multiple.matches/server"})
    public void shouldReceiveMultipleMessagesMatchingHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.abort.and.reconnect/client", "${scripts}/live.fetch.abort.and.reconnect/server"})
    public void shouldReconnectWhenLiveFetchReceivesAbort() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/live.fetch.reset.reconnect.and.message/client",
            "${scripts}/live.fetch.reset.reconnect.and.message/server"})
    public void shouldReconnectAndReceiveMessageWhenLiveFetchReceivesReset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
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
    {"${scripts}/zero.offset/client", "${scripts}/zero.offset/server"})
    public void shouldRequestMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.first.record.batch.large/client",
            "${scripts}/zero.offset.first.record.batch.large/server"})
    public void shouldSkipRecordBatchExceedingMaximumConfiguredSize() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.large.response/client",
            "${scripts}/zero.offset.large.response/server"})
    public void shouldHandleLargeResponse() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.no.messages/client", "${scripts}/zero.offset.no.messages/server"})
    public void shouldRequestMessagesAtZeroOffsetAndNotSkipEmptyRecordBatch() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.message/client", "${scripts}/zero.offset.message/server"})
    public void shouldReceiveMessageAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.length.record.batch/client",
    "${scripts}/zero.length.record.batch/server"})
    public void shouldReceiveMessageAtZeroOffsetAfterSkippingZeroLengthBatch() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.bootstrap.historical.uses.cached.key.then.live/client",
            "${scripts}/ktable.bootstrap.historical.uses.cached.key.then.live/server"})
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
    {"${scripts}/ktable.bootstrap.uses.historical/client",
            "${scripts}/ktable.bootstrap.uses.historical/server"})
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
    {"${scripts}/ktable.delivers.compacted.deleted.messages/client",
            "${scripts}/ktable.delivers.compacted.deleted.messages/server"})
    public void shouldReceiveKTableCompactedDeletedMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.delivers.compacted.messages/client", "${scripts}/ktable.delivers.compacted.messages/server"})
    public void shouldReceiveKTableCompactedMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.historical.uses.cached.key.after.unsubscribe/client",
            "${scripts}/ktable.historical.uses.cached.key.after.unsubscribe/server"})
    public void shouldReceiveKTableMessagesUsingCachedKeyAfterUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.historical.uses.cached.key.then.latest.offset/client",
            "${scripts}/ktable.historical.uses.cached.key.then.latest.offset/server"})
    public void shouldReceiveKTableMessagesUsingCachedKeyThenLatestOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.historical.uses.cached.key.then.live/client",
            "${scripts}/ktable.historical.uses.cached.key.then.live/server"})
    public void shouldReceiveKTableMessagesUsingCachedKeyThenLive() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.historical.uses.cached.key.then.live.after.null.message/client",
            "${scripts}/ktable.historical.uses.cached.key.then.live.after.null.message/server"})
    public void shouldReceiveKTableMessagesFromLiveStreamAfterCachedKeyRemovedByNullMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/client",
            "${scripts}/ktable.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/server"})
    public void shouldReceiveKTableMessagesFromLiveStreamAfterOffsetTooEarlyAndCachedKeyRemovedByNullMessage()
            throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.historical.uses.cached.key.then.zero.offset/client",
            "${scripts}/ktable.historical.uses.cached.key.then.zero.offset/server"})
    public void shouldReceiveKTableMessagesUsingCachedKeyThenZerotOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.message/client", "${scripts}/ktable.message/server"})
    public void shouldReceiveKtableMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.message.delayed.describe.response/client",
            "${scripts}/ktable.message.delayed.describe.response/server"})
    public void shouldReceiveKtableMessageDelayedDescribeResponse() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("DELIVER_DESCRIBE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.messages/client", "${scripts}/ktable.messages/server"})
    public void shouldReceiveKtableMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.messages.header.multiple.matches/client",
            "${scripts}/ktable.messages.header.multiple.matches/server"})
    public void shouldReceiveKtableMessagesFilteredByHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.messages.historical/client", "${scripts}/ktable.messages.historical/server"})
    public void shouldReceiveHistoricalKtableMessages() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.messages.multiple.nodes/client", "${scripts}/ktable.messages.multiple.nodes/server"})
    public void shouldReceiveKtableMessagesFromMultipleNodes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.message.multiple.networks/client", "${scripts}/ktable.message.multiple.networks/server"})
    public void shouldReceiveKtableMessagesFromMultipleNetworks() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/ktable.message.multiple.topics/client", "${scripts}/ktable.message.multiple.topics/server"})
    public void shouldReceiveKtableMessagesFromMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
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
    {"${scripts}/zero.offset.messages/client", "${scripts}/zero.offset.messages/server"})
    public void shouldReceiveMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messagesets/client", "${scripts}/zero.offset.messagesets/server"})
    public void shouldReceiveMessageSetsAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.fanout/client", "${scripts}/zero.offset.messages.fanout/server"})
    public void shouldFanoutMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.group.budget/client", "${scripts}/zero.offset.messages.group.budget/server"})
    public void shouldFanoutMessagesAtZeroOffsetUsingGroupBudget() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.group.budget.reset/client",
            "${scripts}/zero.offset.messages.group.budget.reset/server"})
    public void shouldFanoutMessagesAtZeroOffsetUsingGroupBudgetReset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/zero.offset.messages.multiple.nodes/client", "${scripts}/zero.offset.messages.multiple.nodes/server"})
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
    {"${scripts}/zero.offset.messagesets.fanout/client", "${scripts}/zero.offset.messagesets.fanout/server"})
    public void shouldFanoutMessageSetsAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/nonzero.offset/client", "${scripts}/nonzero.offset/server"})
    public void shouldRequestMessagesAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/nonzero.offset.message/client", "${scripts}/nonzero.offset.message/server"})
    public void shouldReceiveMessageAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/nonzero.offset.messages/client", "${scripts}/nonzero.offset.messages/server"})
    public void shouldReceiveMessagesAtNonZeroOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/offset.too.early.message/client", "${scripts}/offset.too.early.message/server"})
    public void shouldRefetchUsingReportedFirstOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification(
    {"${scripts}/offset.too.early.multiple.topics/client", "${scripts}/offset.too.early.multiple.topics/server"})
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
    {"${scripts}/distinct.offset.messagesets.fanout/client", "${scripts}/distinct.offset.messagesets.fanout/server"})
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
}
