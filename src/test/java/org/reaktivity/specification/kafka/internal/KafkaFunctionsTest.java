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
package org.reaktivity.specification.kafka.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.kaazing.k3po.lang.internal.el.ExpressionFactoryUtils.newExpressionFactory;
import static org.reaktivity.specification.kafka.internal.types.KafkaConditionType.HEADER;
import static org.reaktivity.specification.kafka.internal.types.KafkaConditionType.KEY;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.el.ELContext;
import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import org.agrona.DirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.kaazing.k3po.lang.el.BytesMatcher;
import org.kaazing.k3po.lang.internal.el.ExpressionContext;
import org.reaktivity.specification.kafka.internal.types.OctetsFW;
import org.reaktivity.specification.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaApi;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDescribeBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDescribeDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaEndExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaFetchBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaFetchDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaFetchEndExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaMetaBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaMetaDataExFW;

public class KafkaFunctionsTest
{
    private ExpressionFactory factory;
    private ELContext ctx;

    @Before
    public void setUp() throws Exception
    {
        factory = newExpressionFactory();
        ctx = new ExpressionContext();
    }

    @Test
    public void shouldGenerateRouteExtension()
    {
        byte[] build = KafkaFunctions.routeEx()
                                     .topicName("topic")
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaRouteExFW routeEx = new KafkaRouteExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals("topic", routeEx.topicName().asString());
    }

    @Test
    public void shouldGenerateMetaBeginExtension()
    {
        byte[] build = KafkaFunctions.beginEx()
                                     .typeId(0x01)
                                     .meta()
                                         .topic("topic")
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaBeginExFW beginEx = new KafkaBeginExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, beginEx.typeId());
        assertEquals(KafkaApi.META.value(), beginEx.kind());

        final KafkaMetaBeginExFW metaBeginEx = beginEx.meta();
        assertEquals("topic", metaBeginEx.topic().asString());
    }

    @Test
    public void shouldGenerateMetaDataExtension()
    {
        byte[] build = KafkaFunctions.dataEx()
                                     .typeId(0x01)
                                     .meta()
                                         .partition(0, 1)
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaDataExFW dataEx = new KafkaDataExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, dataEx.typeId());
        assertEquals(KafkaApi.META.value(), dataEx.kind());

        final KafkaMetaDataExFW metaDataEx = dataEx.meta();
        final MutableInteger partitionsCount = new MutableInteger();
        metaDataEx.partitions().forEach(f -> partitionsCount.value++);
        assertEquals(1, partitionsCount.value);

        assertNotNull(metaDataEx.partitions()
                .matchFirst(p -> p.partitionId() == 0 && p.leaderId() == 1));
    }

    @Test
    public void shouldGenerateDescribeBeginExtension()
    {
        byte[] build = KafkaFunctions.beginEx()
                                     .typeId(0x01)
                                     .describe()
                                         .topic("topic")
                                         .config("cleanup.policy")
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaBeginExFW beginEx = new KafkaBeginExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, beginEx.typeId());
        assertEquals(KafkaApi.DESCRIBE.value(), beginEx.kind());

        final KafkaDescribeBeginExFW describeBeginEx = beginEx.describe();
        assertEquals("topic", describeBeginEx.topic().asString());

        final MutableInteger configsCount = new MutableInteger();
        describeBeginEx.configs().forEach(f -> configsCount.value++);
        assertEquals(1, configsCount.value);

        assertNotNull(describeBeginEx.configs()
                .matchFirst(c -> "cleanup.policy".equals(c.asString())));
    }

    @Test
    public void shouldGenerateDescribeDataExtension()
    {
        byte[] build = KafkaFunctions.dataEx()
                                     .typeId(0x01)
                                     .describe()
                                         .config("cleanup.policy", "compact")
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaDataExFW dataEx = new KafkaDataExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, dataEx.typeId());
        assertEquals(KafkaApi.DESCRIBE.value(), dataEx.kind());

        final KafkaDescribeDataExFW describeDataEx = dataEx.describe();
        final MutableInteger configsCount = new MutableInteger();
        describeDataEx.configs().forEach(f -> configsCount.value++);
        assertEquals(1, configsCount.value);

        assertNotNull(describeDataEx.configs()
                .matchFirst(c -> "cleanup.policy".equals(c.name().asString()) &&
                                 "compact".equals(c.value().asString())));
    }

    @Test
    public void shouldGenerateFetchBeginExtension()
    {
        byte[] build = KafkaFunctions.beginEx()
                                     .typeId(0x01)
                                     .fetch()
                                         .topic("topic")
                                         .progress(0, 1L)
                                         .filter()
                                             .key("match")
                                             .build()
                                         .filter()
                                             .header("name", "value")
                                             .build()
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaBeginExFW beginEx = new KafkaBeginExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, beginEx.typeId());
        assertEquals(KafkaApi.FETCH.value(), beginEx.kind());

        final KafkaFetchBeginExFW fetchBeginEx = beginEx.fetch();
        assertEquals("topic", fetchBeginEx.topic().asString());

        final MutableInteger progressCount = new MutableInteger();
        fetchBeginEx.progress().forEach(f -> progressCount.value++);
        assertEquals(1, progressCount.value);

        final MutableInteger filterCount = new MutableInteger();
        fetchBeginEx.filters().forEach(f -> filterCount.value++);
        assertEquals(2, filterCount.value);
        assertNotNull(fetchBeginEx.filters()
                .matchFirst(f -> f.conditions()
                .matchFirst(c -> c.kind() == KEY.value() &&
                    "match".equals(c.key()
                                    .value()
                                    .get((b, o, m) -> b.getStringWithoutLengthUtf8(o, m - o)))) != null));
        assertNotNull(fetchBeginEx.filters()
                .matchFirst(f -> f.conditions()
                .matchFirst(c -> c.kind() == HEADER.value() &&
                    "name".equals(c.header().name().asString()) &&
                    "value".equals(c.header().value()
                                    .get((b, o, m) -> b.getStringWithoutLengthUtf8(o, m - o)))) != null));

        assertNotNull(fetchBeginEx.progress()
                .matchFirst(p -> p.partitionId() == 0 && p.offset$() == 1L));
    }

    @Test
    public void shouldGenerateFetchBeginExtensionWithNullKeyAndNullHeaderValue()
    {
        byte[] build = KafkaFunctions.beginEx()
                                     .typeId(0x01)
                                     .fetch()
                                         .topic("topic")
                                         .progress(0, 1L)
                                         .filter()
                                             .key(null)
                                             .build()
                                         .filter()
                                             .header("name", null)
                                             .build()
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaBeginExFW beginEx = new KafkaBeginExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, beginEx.typeId());
        assertEquals(KafkaApi.FETCH.value(), beginEx.kind());

        final KafkaFetchBeginExFW fetchBeginEx = beginEx.fetch();
        assertEquals("topic", fetchBeginEx.topic().asString());

        final MutableInteger progressCount = new MutableInteger();
        fetchBeginEx.progress().forEach(f -> progressCount.value++);
        assertEquals(1, progressCount.value);

        final MutableInteger filterCount = new MutableInteger();
        fetchBeginEx.filters().forEach(f -> filterCount.value++);
        assertEquals(2, filterCount.value);
        assertNotNull(fetchBeginEx.filters()
                .matchFirst(f -> f.conditions()
                .matchFirst(c -> c.kind() == KEY.value() &&
                    Objects.isNull(c.key().value())) != null));
        assertNotNull(fetchBeginEx.filters()
                .matchFirst(f -> f.conditions()
                .matchFirst(c -> c.kind() == HEADER.value() &&
                    "name".equals(c.header().name().asString()) &&
                    Objects.isNull(c.header().value())) != null));

        assertNotNull(fetchBeginEx.progress()
                .matchFirst(p -> p.partitionId() == 0 && p.offset$() == 1L));
    }

    @Test
    public void shouldGenerateFetchDataExtension()
    {
        byte[] build = KafkaFunctions.dataEx()
                                     .typeId(0x01)
                                     .fetch()
                                         .timestamp(12345678L)
                                         .key("match")
                                         .header("name", "value")
                                         .progress(0, 1L)
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaDataExFW dataEx = new KafkaDataExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, dataEx.typeId());
        assertEquals(KafkaApi.FETCH.value(), dataEx.kind());

        final KafkaFetchDataExFW fetchDataEx = dataEx.fetch();
        assertEquals(12345678L, fetchDataEx.timestamp());
        assertEquals("match", fetchDataEx.key()
                                         .value()
                                         .get((b, o, m) -> b.getStringWithoutLengthUtf8(o, m - o)));

        final MutableInteger headersCount = new MutableInteger();
        fetchDataEx.headers().forEach(f -> headersCount.value++);
        assertEquals(1, headersCount.value);
        assertNotNull(fetchDataEx.headers()
                .matchFirst(h ->
                    "name".equals(h.name().asString()) &&
                    "value".equals(h.value()
                                    .get((b, o, m) -> b.getStringWithoutLengthUtf8(o, m - o)))) != null);

        final MutableInteger progressCount = new MutableInteger();
        fetchDataEx.progress().forEach(f -> progressCount.value++);
        assertEquals(1, progressCount.value);

        assertNotNull(fetchDataEx.progress()
                .matchFirst(p -> p.partitionId() == 0 && p.offset$() == 1L));
    }

    @Test
    public void shouldGenerateFetchDataExtensionWithNullKeyAndNullHeaderValue()
    {
        byte[] build = KafkaFunctions.dataEx()
                                     .typeId(0x01)
                                     .fetch()
                                         .timestamp(12345678L)
                                         .key(null)
                                         .header("name", null)
                                         .progress(0, 1L)
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaDataExFW dataEx = new KafkaDataExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, dataEx.typeId());
        assertEquals(KafkaApi.FETCH.value(), dataEx.kind());

        final KafkaFetchDataExFW fetchDataEx = dataEx.fetch();
        assertEquals(12345678L, fetchDataEx.timestamp());
        assertNull(fetchDataEx.key().value());

        final MutableInteger headersCount = new MutableInteger();
        fetchDataEx.headers().forEach(f -> headersCount.value++);
        assertEquals(1, headersCount.value);
        assertNotNull(fetchDataEx.headers()
                .matchFirst(h ->
                    "name".equals(h.name().asString()) &&
                    Objects.isNull(h.value())));

        final MutableInteger progressCount = new MutableInteger();
        fetchDataEx.progress().forEach(f -> progressCount.value++);
        assertEquals(1, progressCount.value);

        assertNotNull(fetchDataEx.progress()
                .matchFirst(p -> p.partitionId() == 0 && p.offset$() == 1L));
    }

    @Test
    public void shouldGenerateFetchEndExtension()
    {
        byte[] build = KafkaFunctions.endEx()
                                     .typeId(0x01)
                                     .fetch()
                                         .progress(0, 1L)
                                         .build()
                                     .build();

        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaEndExFW endEx = new KafkaEndExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, endEx.typeId());

        final KafkaFetchEndExFW fetchEndEx = endEx.fetch();
        final MutableInteger progressCount = new MutableInteger();
        fetchEndEx.progress().forEach(f -> progressCount.value++);
        assertEquals(1, progressCount.value);

        assertNotNull(fetchEndEx.progress()
                .matchFirst(p -> p.partitionId() == 0 && p.offset$() == 1L));
    }

    @Test
    public void shouldMatchFetchDataExtension() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .fetch()
                                                 .timestamp(12345678L)
                                                 .key("match")
                                                 .header("name", "value")
                                                 .progress(0, 1L)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                             .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                             .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                             .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchDataExtensionTypeId() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchFetchDataExtensionTimestamp() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .fetch()
                                                 .timestamp(12345678L)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchFetchDataExtensionProgress() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .fetch()
                                                 .progress(0, 1L)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchFetchDataExtensionKey() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .fetch()
                                                 .key("match")
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchFetchDataExtensionNullKey() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .fetch()
                                                 .key(null)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value((OctetsFW) null))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchFetchDataExtensionHeader() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .fetch()
                                                 .header("name", "value")
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchFetchDataExtensionHeaderWithNullValue() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .fetch()
                                                 .header("name", null)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value((OctetsFW) null))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test(expected = Exception.class)
    public void shouldNotMatchDataExtensionTypeId() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x02)
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        matcher.match(byteBuf);
    }

    @Test
    public void shouldNotBuildMatcher() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1);

        Object matched = matcher.match(byteBuf);

        assertNull(matched);
    }

    @Test(expected = Exception.class)
    public void shouldNotMatchFetchDataExtensionTimestamp() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .fetch()
                                                 .timestamp(123456789L)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        matcher.match(byteBuf);
    }

    @Test(expected = Exception.class)
    public void shouldNotMatchFetchDataExtensionProgress() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .fetch()
                                                 .timestamp(12345678L)
                                                 .progress(0, 2L)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        matcher.match(byteBuf);
    }

    @Test(expected = Exception.class)
    public void shouldNotMatchFetchDataExtensionKey() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .fetch()
                                                 .timestamp(12345678L)
                                                 .key("no match")
                                                 .progress(0, 1L)
                                                 .build()
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .fetch(f -> f.timestamp(12345678L)
                        .key(k -> k.value(v -> v.set("match".getBytes(UTF_8))))
                        .headersItem(h -> h.name("name").value(v -> v.set("value".getBytes(UTF_8))))
                        .progressItem(p -> p.partitionId(0).offset$(1L)))
                .build();

        matcher.match(byteBuf);
    }

    @Test
    public void shouldInvokeLength() throws Exception
    {
        String expressionText = "${kafka:length(\"text\")}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, int.class);
        Object actual = expression.getValue(ctx);
        assertEquals("text".length(), ((Integer) actual).intValue());
    }

    @Test
    public void shouldInvokeLengthAsShort() throws Exception
    {
        String expressionText = "${kafka:lengthAsShort(\"text\")}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, short.class);
        Object actual = expression.getValue(ctx);
        assertEquals("text".length(), ((Short) actual).intValue());
    }

    @Test
    public void shouldInvokeNewRequestId() throws Exception
    {
        String expressionText = "${kafka:newRequestId()}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, Integer.class);
        Object actual = expression.getValue(ctx);
        assertTrue(actual instanceof Integer);
    }

    @Test
    public void shouldInvokeRandomBytes() throws Exception
    {
        String expressionText = "${kafka:randomBytes(10)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        Object actual = expression.getValue(ctx);
        assertTrue(actual instanceof byte[]);
        assertEquals(10, ((byte[]) actual).length);
    }

    @Test
    public void shouldInvokeTimestamp() throws Exception
    {
        String expressionText = "${kafka:timestamp()}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, Long.class);
        Object actual = expression.getValue(ctx);
        assertTrue(actual instanceof Long);
    }

    @Test
    public void shouldComputeVarintTenBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", Long.MAX_VALUE);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xfe, (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintTenBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 62);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintNineBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 62);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintNineBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 55);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintEightBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 55);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintEightBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 48);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintSevenBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 48);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintSevenBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 41);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintSixBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 41);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintSixBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 34);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintFiveBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 34);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff,
                                       (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintFiveBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 27);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80,
                                       (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintFourBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 27);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintFourBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 20);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintThreeBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 20);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintThreeBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 13);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintTwoBytesMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 13);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0xff, 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintTwoBytesMin() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", 1L << 6);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x80, 0x01 }, actuals);
    }

    @Test
    public void shouldComputeVarintOneByteMax() throws Exception
    {
        String expressionText = String.format("${kafka:varint(%d)}", -1L << 6);
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { 0x7f }, actuals);
    }

    @Test
    public void shouldComputeVarintOneByteMin() throws Exception
    {
        String expressionText = "${kafka:varint(0)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { 0x00 }, actuals);
    }
}
