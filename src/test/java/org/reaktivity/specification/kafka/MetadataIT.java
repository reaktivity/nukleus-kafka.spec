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

public class MetadataIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("scripts", "org/reaktivity/specification/kafka/metadata.v5");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout);

    @Test
    @Specification({
        "${scripts}/all.topics/client",
        "${scripts}/all.topics/server"})
    public void shouldRequestMetadataForAllTopics() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/metadata.connection.aborted.and.reconnect/client",
        "${scripts}/metadata.connection.aborted.and.reconnect/server"})
    public void shouldReconnectAndContinueMetadataQueriesWhenMetadataConnectionIsAborted() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/metadata.connection.closed.and.reconnect/client",
        "${scripts}/metadata.connection.closed.and.reconnect/server"})
    public void shouldReconnectAndContinueMetadataQueriesWhenMetadataConnectionIsEnded() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/metadata.connection.reset.and.reconnect/client",
        "${scripts}/metadata.connection.reset.and.reconnect/server"})
    public void shouldReconnectAndContinueMetadataQueriesWhenMetadataConnectionIsReset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/multiple.topics.and.nodes/client",
        "${scripts}/multiple.topics.and.nodes/server"})
    public void shouldRequestMetadataForMultipleTopicsAndNodes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.error.invalid.topic/client",
        "${scripts}/one.topic.error.invalid.topic/server"})
    public void shouldHandleErrorInvalidTopicNameFromSingleTopicMetadataRequest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.error.unknown.topic/client",
        "${scripts}/one.topic.error.unknown.topic/server"})
    public void shouldHandleErrorUnknownTopicFromSingleTopicMetadataRequest() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.error.partition.count.changed/client",
        "${scripts}/one.topic.error.partition.count.changed/server"})
    public void shouldHandleErrorPartitionCountChangedFromMetadataResponse() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.leader.not.available.and.retry/client",
        "${scripts}/one.topic.leader.not.available.and.retry/server"})
    public void shouldRetryWhenLeaderNotAvailable() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.unknown.error.abort.receive.abort.and.retry/client",
        "${scripts}/one.topic.unknown.error.abort.receive.abort.and.retry/server"})
    public void shouldReconnectAndRetryWhenAbortAndAbortReceivedWhenMetadataQueryGivesUnknownError() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.unknown.error.abort.receive.end.and.retry/client",
        "${scripts}/one.topic.unknown.error.abort.receive.end.and.retry/server"})
    public void shouldReconnectAndRetryWhenAbortAndEndReceivedWhenMetadataQueryGivesUnknownError() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/configs.response.unknown.error.abort.receive.abort.and.retry/client",
        "${scripts}/configs.response.unknown.error.abort.receive.abort.and.retry/server"})
    public void shouldReconnectAndRetryWhenAbortAndAbortReceivedWhenConfigsResponseGivesUnknownError() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.multiple.nodes/client",
        "${scripts}/one.topic.multiple.nodes/server"})
    public void shouldRequestMetadataForOneTopicOnMultipleNodes() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.multiple.nodes.and.replicas/client",
        "${scripts}/one.topic.multiple.nodes.and.replicas/server"})
    public void shouldHandleMetadataResponseOneTopicMultipleNodesAndReplicas() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.multiple.partitions/client",
        "${scripts}/one.topic.multiple.partitions/server"})
    public void shouldRequestMetadataForOneTopicMultiplePartitionsSingleNode() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/one.topic.single.partition/client",
        "${scripts}/one.topic.single.partition/server"})
    public void shouldRequestMetadataForOneTopicSinglePartition() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }
    @Test
    @Specification({
        "${scripts}/metadata.incomplete.response.aborts/client",
        "${scripts}/metadata.incomplete.response.aborts/server"})
    public void shouldAbortWhenMetadataResponseIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/metadata.incomplete.response.broker.aborts/client",
        "${scripts}/metadata.incomplete.response.broker.aborts/server"})
    public void shouldAbortWhenMetadataResponseBrokerIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/metadata.incomplete.response.after.brokers.aborts/client",
        "${scripts}/metadata.incomplete.response.after.brokers.aborts/server"})
    public void shouldAbortWhenMetadataResponseAfterBrokersIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/metadata.incomplete.response.topic.aborts/client",
        "${scripts}/metadata.incomplete.response.topic.aborts/server"})
    public void shouldAbortWhenMetadataResponseTopicIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/metadata.incomplete.response.topic.partition.aborts/client",
        "${scripts}/metadata.incomplete.response.topic.partition.aborts/server"})
    public void shouldAbortWhenMetadataResponseTopicPartitionIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/describe.configs.incomplete.response.aborts/client",
        "${scripts}/describe.configs.incomplete.response.aborts/server"})
    public void shouldAbortWhenDescribeConfigsResponseIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/describe.configs.incomplete.response.resource.aborts/client",
        "${scripts}/describe.configs.incomplete.response.resource.aborts/server"})
    public void shouldAbortWhenDescribeConfigsResponseResourceIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/describe.configs.incomplete.response.resource.config.aborts/client",
        "${scripts}/describe.configs.incomplete.response.resource.config.aborts/server"})
    public void shouldAbortWhenDescribeConfigsResponseResourceConfigIsIncomplete() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${scripts}/unknown.and.known.topics/client",
        "${scripts}/unknown.and.known.topics/server"})
    public void shouldRepeatedlyRequestMetadataForTopicsWhichAreNotFound() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.notifyBarrier("WRITE_SECOND_UNKNOWN_TOPIC_METADATA_RESPONSE");
        k3po.notifyBarrier("WRITE_THIRD_UNKNOWN_TOPIC_METADATA_RESPONSE");
        k3po.finish();
    }

}
