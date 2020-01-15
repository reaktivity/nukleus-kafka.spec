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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;

public class FetchAllIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("scripts", "org/reaktivity/specification/nukleus/kafka/streams/fetch.all");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout);

    @Test
    @Specification({
        "${scripts}/merged.filter.header/client",
        "${scripts}/merged.filter.header/server"})
    public void shouldReceiveMergedMessagesWithHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.filter.header.and.header/client",
        "${scripts}/merged.filter.header.and.header/server"})
    public void shouldReceiveMergedMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.filter.header.or.header/client",
        "${scripts}/merged.filter.header.or.header/server"})
    public void shouldReceiveMergedMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.filter.key/client",
        "${scripts}/merged.filter.key/server"})
    public void shouldReceiveMergedMessagesWithKeyFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.filter.key.and.header/client",
        "${scripts}/merged.filter.key.and.header/server"})
    public void shouldReceiveMergedMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.filter.key.or.header/client",
        "${scripts}/merged.filter.key.or.header/server"})
    public void shouldReceiveMergedMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.filter.none/client",
        "${scripts}/merged.filter.none/server"})
    public void shouldReceiveMergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.message.values/client",
        "${scripts}/merged.message.values/server"})
    public void shouldReceiveMergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.partition.offsets.latest/client",
        "${scripts}/merged.partition.offsets.latest/server"})
    public void shouldRequestMergedPartitionOffsetsLatest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/merged.partition.offsets.earliest/client",
        "${scripts}/merged.partition.offsets.earliest/server"})
    public void shouldRequestMergedPartitionOffsetsEarliest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.filter.none/client",
        "${scripts}/unmerged.filter.none/server"})
    public void shouldReceiveUnmergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.message.values/client",
        "${scripts}/unmerged.message.values/server"})
    public void shouldReceiveUnmergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.partition.offsets.latest/client",
        "${scripts}/unmerged.partition.offsets.latest/server"})
    public void shouldRequestUnmergedPartitionOffsetsLatest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unmerged.partition.offsets.earliest/client",
        "${scripts}/unmerged.partition.offsets.earliest/server"})
    public void shouldRequestUnmergedPartitionOffsetsEarliest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }
}
