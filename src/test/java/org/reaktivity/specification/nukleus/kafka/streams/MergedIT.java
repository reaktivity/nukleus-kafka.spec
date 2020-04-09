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
    public void shouldReceiveMergedMessagesWithHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.header.and.header/client",
        "${scripts}/merged.fetch.filter.header.and.header/server"})
    public void shouldReceiveMergedMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.header.or.header/client",
        "${scripts}/merged.fetch.filter.header.or.header/server"})
    public void shouldReceiveMergedMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.key/client",
        "${scripts}/merged.fetch.filter.key/server"})
    public void shouldReceiveMergedMessagesWithKeyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.key.and.header/client",
        "${scripts}/merged.fetch.filter.key.and.header/server"})
    public void shouldReceiveMergedMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.key.or.header/client",
        "${scripts}/merged.fetch.filter.key.or.header/server"})
    public void shouldReceiveMergedMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.filter.none/client",
        "${scripts}/merged.fetch.filter.none/server"})
    public void shouldReceiveMergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.fetch.message.values/client",
        "${scripts}/merged.fetch.message.values/server"})
    public void shouldReceiveMergedMessageValues() throws Exception
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
        "${scripts}/unmerged.fetch.filter.none/client",
        "${scripts}/unmerged.fetch.filter.none/server"})
    public void shouldFetchUnmergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
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
    public void shouldRequestUnmergedPartitionOffsetsLatest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.partition.offsets.earliest/client",
        "${scripts}/unmerged.fetch.partition.offsets.earliest/server"})
    public void shouldRequestUnmergedPartitionOffsetsEarliest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.fetch.partition.leader.changed/client",
        "${scripts}/unmerged.fetch.partition.leader.changed/server"})
    public void shouldRequestUnmergedPartitionLeaderChanged() throws Exception
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
    public void shouldRequestUnmergedPartitionLeaderAborted() throws Exception
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
}
