/**
 * Copyright 2016-2020 The Reaktivity Project
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;

public class MergedIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("scripts", "org/reaktivity/specification/nukleus/kafka/streams/merged");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout);

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.header/client",
        "${scripts}/merged.fetch.filter.header/server"})
    public void shouldFetchMergedMessagesWithHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.header.and.header/client",
        "${scripts}/merged.fetch.filter.header.and.header/server"})
    public void shouldFetchMergedMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.header.or.header/client",
        "${scripts}/merged.fetch.filter.header.or.header/server"})
    public void shouldFetchMergedMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.key/client",
        "${scripts}/merged.fetch.filter.key/server"})
    public void shouldFetchMergedMessagesWithKeyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.key.and.header/client",
        "${scripts}/merged.fetch.filter.key.and.header/server"})
    public void shouldFetchMergedMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.key.or.header/client",
        "${scripts}/merged.fetch.filter.key.or.header/server"})
    public void shouldFetchMergedMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.none/client",
        "${scripts}/merged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.message.values/client",
        "${scripts}/merged.fetch.message.values/server"})
    public void shouldFetchMergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.partition.offsets.latest/client",
        "${scripts}/merged.fetch.partition.offsets.latest/server"})
    public void shouldFetchMergedPartitionOffsetsLatest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.partition.offsets.earliest/client",
        "${scripts}/merged.fetch.partition.offsets.earliest/server"})
    public void shouldFetchMergedPartitionOffsetsEarliest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.partition.offsets.earliest.overflow/client",
        "${scripts}/merged.fetch.partition.offsets.earliest.overflow/server"})
    public void shouldFetchMergedPartitionOffsetsEarliestOverflow() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.partition.leader.changed/client",
        "${scripts}/merged.fetch.partition.leader.changed/server"})
    public void shouldFetchMergedPartitionLeaderChanged() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.partition.leader.aborted/client",
        "${scripts}/merged.fetch.partition.leader.aborted/server"})
    public void shouldFetchMergedPartitionLeaderAborted() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.produce.message.values/client",
        "${scripts}/merged.produce.message.values/server"})
    public void shouldProduceMergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.produce.message.values.dynamic/client",
        "${scripts}/merged.produce.message.values.dynamic/server"})
    public void shouldProduceMergedMessageValuesDynamic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.produce.message.values.dynamic.hashed/client",
        "${scripts}/merged.produce.message.values.dynamic.hashed/server"})
    public void shouldProduceMergedMessageValuesDynamicHashed() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.produce.message.flags.incomplete/client",
        "${scripts}/merged.produce.message.flags.incomplete/server"})
    public void shouldProduceMergedMessageWithIncompleteFlags() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.server.sent.close/client",
        "${scripts}/merged.fetch.server.sent.close/server"})
    public void shouldMergedFetchServerSentClose() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.server.sent.abort.with.message/client",
        "${scripts}/merged.fetch.server.sent.abort.with.message/server"})
    public void shouldMergedFetchServerSentAbortWithMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.server.sent.close.with.message/client",
        "${scripts}/merged.fetch.server.sent.close.with.message/server"})
    public void shouldMergedFetchServerSentCloseWithMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.server.sent.abort/client",
        "${scripts}/merged.fetch.server.sent.abort/server"})
    public void shouldMergedFetchServerSentAbort() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }


    @Test
    @Specification({
        "${scripts}/unmerged.fetch.filter.none/client",
        "${scripts}/unmerged.fetch.filter.none/server"})
    public void shouldFetchUnmergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("SEND_MESSAGE_A3");
        k3po.notifyBarrier("SEND_MESSAGE_B3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.message.values/client",
        "${scripts}/unmerged.fetch.message.values/server"})
    public void shouldFetchUnmergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("CHANGING_PARTITION_COUNT");
        k3po.notifyBarrier("CHANGED_PARTITION_COUNT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.partition.offsets.latest/client",
        "${scripts}/unmerged.fetch.partition.offsets.latest/server"})
    public void shouldFetchUnmergedPartitionOffsetsLatest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.partition.offsets.earliest/client",
        "${scripts}/unmerged.fetch.partition.offsets.earliest/server"})
    public void shouldFetchUnmergedPartitionOffsetsEarliest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.partition.leader.changed/client",
        "${scripts}/unmerged.fetch.partition.leader.changed/server"})
    public void shouldFetchUnmergedPartitionLeaderChanged() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("CHANGING_PARTITION_LEADER");
        k3po.notifyBarrier("CHANGED_PARTITION_LEADER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.partition.leader.aborted/client",
        "${scripts}/unmerged.fetch.partition.leader.aborted/server"})
    public void shouldFetchUnmergedPartitionLeaderAborted() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.produce.message.values/client",
        "${scripts}/unmerged.produce.message.values/server"})
    public void shouldProduceUnmergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.produce.message.values.dynamic/client",
        "${scripts}/unmerged.produce.message.values.dynamic/server"})
    public void shouldProduceUnmergedMessageValuesDynamic() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.produce.message.flags.incomplete/client",
        "${scripts}/unmerged.produce.message.flags.incomplete/server"})
    public void shouldProduceUnmergedMessageFlagsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.produce.message.values.dynamic.hashed/client",
        "${scripts}/unmerged.produce.message.values.dynamic.hashed/server"})
    public void shouldProduceUnmergedMessageValuesDynamicHashed() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.server.sent.close/client",
        "${scripts}/unmerged.fetch.server.sent.close/server"})
    public void shouldUnmergedFetchServerSentClose() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.server.sent.close.with.message/client",
        "${scripts}/unmerged.fetch.server.sent.close.with.message/server"})
    public void shouldUnmergedFetchServerSentCloseWithMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CLOSE_UNMERGED_FETCH_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.server.sent.abort.with.message/client",
        "${scripts}/unmerged.fetch.server.sent.abort.with.message/server"})
    public void shouldUnmergedFetchServerSentAbortWithMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("ABORT_UNMERGED_FETCH_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.server.sent.reset.and.abort.with.message/client",
        "${scripts}/unmerged.fetch.server.sent.reset.and.abort.with.message/server"})
    public void shouldUnmergedFetchServerSentResetAndAbortWithMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("RESET_UNMERGED_FETCH_INITIAL");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.not.header/client",
        "${scripts}/merged.fetch.filter.not.header/server"})
    public void shouldFetchMergedMessagesWithNotHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.not.key/client",
        "${scripts}/merged.fetch.filter.not.key/server"})
    public void shouldFetchMergedMessagesWithNotKeyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.key.and.not.header/client",
        "${scripts}/merged.fetch.filter.key.and.not.header/server"})
    public void shouldFetchMergedMessagesWithKeyAndNotHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.headers.one/client",
        "${scripts}/merged.fetch.filter.headers.one/server"})
    public void shouldFetchMergedMessagesWithHeadersOneFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.headers.one.empty/client",
        "${scripts}/merged.fetch.filter.headers.one.empty/server"})
    public void shouldFetchMergedMessagesWithHeadersOneEmptyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.headers.many/client",
        "${scripts}/merged.fetch.filter.headers.many/server"})
    public void shouldFetchMergedMessagesWithHeadersManyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.headers.many.empty/client",
        "${scripts}/merged.fetch.filter.headers.many.empty/server"})
    public void shouldFetchMergedMessagesWithHeadersManyEmptyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.headers.skip.one/client",
        "${scripts}/merged.fetch.filter.headers.skip.one/server"})
    public void shouldFetchMergedMessagesWithHeadersSkipOneFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.headers.skip.two/client",
        "${scripts}/merged.fetch.filter.headers.skip.two/server"})
    public void shouldFetchMergedMessagesWithHeadersSkipTwoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.headers.skip.many/client",
        "${scripts}/merged.fetch.filter.headers.skip.many/server"})
    public void shouldFetchMergedMessagesWithHeadersSkipManyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }
}
