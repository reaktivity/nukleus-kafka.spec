/**
 * Copyright 2016-2021 The Reaktivity Project
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

public class ProduceIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("scripts", "org/reaktivity/specification/nukleus/kafka/streams/produce");

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
        "${scripts}/message.key/client",
        "${scripts}/message.key/server"})
    public void shouldSendMessageKey() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.null/client",
        "${scripts}/message.key.null/server"})
    public void shouldSendMessageKeyNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.with.value.null/client",
        "${scripts}/message.key.with.value.null/server"})
    public void shouldSendMessageKeyWithValueNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.with.value.distinct/client",
        "${scripts}/message.key.with.value.distinct/server"})
    public void shouldSendMessageKeyWithValueDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.with.header/client",
        "${scripts}/message.key.with.header/server"})
    public void shouldSendMessageKeyWithHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.key.distinct/client",
        "${scripts}/message.key.distinct/server"})
    public void shouldSendMessageKeyDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value/client",
        "${scripts}/message.value/server"})
    public void shouldSendMessageValue() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.null/client",
        "${scripts}/message.value.null/server"})
    public void shouldSendMessageValueNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.10k/client",
        "${scripts}/message.value.10k/server"})
    public void shouldSendMessageValue10k() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.100k/client",
        "${scripts}/message.value.100k/server"})
    public void shouldSendMessageValue100k() throws Exception
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
    public void shouldSendMessageValueGzip() throws Exception
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
    public void shouldSendMessageValueSnappy() throws Exception
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
    public void shouldSendMessageValueLz4() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.distinct/client",
        "${scripts}/message.value.distinct/server"})
    public void shouldSendMessageValueDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.value.repeated/client",
        "${scripts}/message.value.repeated/server"})
    public void shouldSendMessageValueRepeated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.header/client",
        "${scripts}/message.header/server"})
    public void shouldSendMessageHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.header.null/client",
        "${scripts}/message.header.null/server"})
    public void shouldSendMessageHeaderNull() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.headers.distinct/client",
        "${scripts}/message.headers.distinct/server"})
    public void shouldSendMessageHeadersDistinct() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/message.headers.repeated/client",
        "${scripts}/message.headers.repeated/server"})
    public void shouldSendMessageHeadersRepeated() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }
}
