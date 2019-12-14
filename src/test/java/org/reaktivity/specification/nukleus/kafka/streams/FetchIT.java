/**
 * Copyright 2016-2019 The Reaktivity Project
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

    @Test
    @Specification({
        "${scripts}/topic.missing/client",
        "${scripts}/topic.missing/server"})
    public void shouldRejectWhenTopicMissing() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/topic.not.routed/client",
        "${scripts}/topic.not.routed/server"})
    public void shouldRejectWhenTopicNotRouted() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/partition.offset.missing/client",
        "${scripts}/partition.offset.missing/server"})
    public void shouldRejectWhenPartitionOffsetMissing() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/partition.offset.repeated/client",
        "${scripts}/partition.offset.repeated/server"})
    public void shouldRejectWhenPartitionOffsetRepeated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/partition.unknown/client",
        "${scripts}/partition.unknown/server"})
    public void shouldRejectWhenPartitionUnknown() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/partition.not.leader/client",
        "${scripts}/partition.not.leader/server"})
    public void shouldRejectPartitionNotLeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/partition.offset/client",
        "${scripts}/partition.offset/server"})
    public void shouldRequestPartitionOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/partition.offset.earliest/client",
        "${scripts}/partition.offset.earliest/server"})
    public void shouldRequestPartitionOffsetEarliest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/partition.offset.zero/client",
        "${scripts}/partition.offset.zero/server"})
    public void shouldRequestPartitionOffsetZero() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key/client",
        "${scripts}/message.key/server"})
    public void shouldReceiveMessageKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.null/client",
        "${scripts}/message.key.null/server"})
    public void shouldReceiveMessageKeyNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.with.value.null/client",
        "${scripts}/message.key.with.value.null/server"})
    public void shouldReceiveMessageKeyWithValueNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.with.value.distinct/client",
        "${scripts}/message.key.with.value.distinct/server"})
    public void shouldReceiveMessageKeyWithValueDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.with.header/client",
        "${scripts}/message.key.with.header/server"})
    public void shouldReceiveMessageKeyWithHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.distinct/client",
        "${scripts}/message.key.distinct/server"})
    public void shouldReceiveMessageKeyDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value/client",
        "${scripts}/message.value/server"})
    public void shouldReceiveMessageValue() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.null/client",
        "${scripts}/message.value.null/server"})
    public void shouldReceiveMessageValueNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.10k/client",
        "${scripts}/message.value.10k/server"})
    public void shouldReceiveMessageValue10k() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${scripts}/message.value.gzip/client",
        "${scripts}/message.value.gzip/server"})
    public void shouldReceiveMessageValueGzip() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${scripts}/message.value.snappy/client",
        "${scripts}/message.value.snappy/server"})
    public void shouldReceiveMessageValueSnappy() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${scripts}/message.value.lz4/client",
        "${scripts}/message.value.lz4/server"})
    public void shouldReceiveMessageValueLz4() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.distinct/client",
        "${scripts}/message.value.distinct/server"})
    public void shouldReceiveMessageValueDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.header/client",
        "${scripts}/message.header/server"})
    public void shouldReceiveMessageHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.header/client",
        "${scripts}/message.header/server"})
    public void shouldReceiveMessageHeaderNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.headers.distinct/client",
        "${scripts}/message.headers.distinct/server"})
    public void shouldReceiveMessageHeadersDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.headers.repeated/client",
        "${scripts}/message.headers.repeated/server"})
    public void shouldReceiveMessageHeadersRepeated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/filter.none/client",
        "${scripts}/filter.none/server"})
    public void shouldReceiveMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/filter.key/client",
        "${scripts}/filter.key/server"})
    public void shouldReceiveMessagesWithKeyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/filter.key.and.header/client",
        "${scripts}/filter.key.and.header/server"})
    public void shouldReceiveMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/filter.key.or.header/client",
        "${scripts}/filter.key.or.header/server"})
    public void shouldReceiveMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/filter.header/client",
        "${scripts}/filter.header/server"})
    public void shouldReceiveMessagesWithHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/filter.header.and.header/client",
        "${scripts}/filter.header.and.header/server"})
    public void shouldReceiveMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/filter.header.or.header/client",
        "${scripts}/filter.header.or.header/server"})
    public void shouldReceiveMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compact.message.with.message/client",
        "${scripts}/compact.message.with.message/server"})
    public void shouldCompactMessageWithMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compact.message.with.tombstone/client",
        "${scripts}/compact.message.with.tombstone/server"})
    public void shouldCompactMessageWithTombstone() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compact.tombstone.with.message/client",
        "${scripts}/compact.tombstone.with.message/server"})
    public void shouldCompactTombstoneWithMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compact.tombstone.with.tombstone/client",
        "${scripts}/compact.tombstone.with.tombstone/server"})
    public void shouldCompactTombstoneWithTombstone() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.message.with.message/client",
        "${scripts}/compacted.message.with.message/server"})
    public void shouldReceiveMessageAfterCompactedMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.message.with.tombstone/client",
        "${scripts}/compacted.message.with.tombstone/server"})
    public void shouldReceiveTombstoneAfterCompactedMessage() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.tombstone.with.message/client",
        "${scripts}/compacted.tombstone.with.message/server"})
    public void shouldReceiveMessageAfterCompactedTombstone() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/compacted.tombstone.with.tombstone/client",
        "${scripts}/compacted.tombstone.with.tombstone/server"})
    public void shouldReceiveTombstoneAfterCompactedTombstone() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }
}
