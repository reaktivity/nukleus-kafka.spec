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
package org.reaktivity.specification.kafka.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Spliterators.spliterator;
import static java.util.stream.StreamSupport.intStream;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.kaazing.k3po.lang.internal.el.ExpressionFactoryUtils.newExpressionFactory;

import java.nio.ByteBuffer;

import javax.el.ELContext;
import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.kaazing.k3po.lang.el.BytesMatcher;
import org.kaazing.k3po.lang.internal.el.ExpressionContext;
import org.reaktivity.specification.kafka.internal.types.OctetsFW;
import org.reaktivity.specification.kafka.internal.types.String16FW;
import org.reaktivity.specification.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaEndExFW;

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
                                     .header("name", "value")
                                     .build();
        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaRouteExFW routeEx = new KafkaRouteExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals("topic", routeEx.topicName().asString());
        routeEx.headers().forEach(onlyHeader ->
        {
            final String16FW key = onlyHeader.key();
            final OctetsFW value = onlyHeader.value();
            final UnsafeBuffer expectedValue = new UnsafeBuffer("value".getBytes(UTF_8));

            assertEquals("name", key.asString());
            assertEquals(new OctetsFW().wrap(expectedValue, 0, expectedValue.capacity()), value);
        });
        assertTrue(routeEx.headers().sizeof() > 0);
    }

    @Test
    public void shouldGenerateBeginExtension()
    {
        byte[] build = KafkaFunctions.beginEx()
                                     .typeId(0x01)
                                     .topicName("topic")
                                     .fetchOffset(1L)
                                     .fetchKey("match")
                                     .fetchKeyHash(new int[] { 1, 2, 3 })
                                     .header("name", "value")
                                     .build();
        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaBeginExFW beginEx = new KafkaBeginExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, beginEx.typeId());
        assertEquals("topic", beginEx.topicName().asString());
        beginEx.fetchOffsets().forEach(onlyVarint ->
        {
            assertEquals(1L, onlyVarint.value());
        });
        assertTrue(beginEx.fetchOffsets().sizeof() > 0);
        final UnsafeBuffer expectedFetchKey = new UnsafeBuffer("match".getBytes(UTF_8));
        assertEquals(new OctetsFW().wrap(expectedFetchKey, 0, expectedFetchKey.capacity()), beginEx.fetchKey());
        assertArrayEquals(new int[] { 1, 2, 3 }, intStream(spliterator(beginEx.fetchKeyHash(), 3, 0), false).toArray());
        beginEx.headers().forEach(onlyHeader ->
        {
            final String16FW key = onlyHeader.key();
            final OctetsFW value = onlyHeader.value();
            final UnsafeBuffer expectedValue = new UnsafeBuffer("value".getBytes(UTF_8));

            assertEquals("name", key.asString());
            assertEquals(new OctetsFW().wrap(expectedValue, 0, expectedValue.capacity()), value);
        });
        assertTrue(beginEx.headers().sizeof() > 0);
    }

    @Test
    public void shouldGenerateDataExtension()
    {
        byte[] build = KafkaFunctions.dataEx()
                                     .typeId(0x01)
                                     .timestamp(12345678L)
                                     .fetchOffset(1L)
                                     .messageKey("match")
                                     .build();
        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaDataExFW dataEx = new KafkaDataExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, dataEx.typeId());
        assertEquals(12345678L, dataEx.timestamp());
        dataEx.fetchOffsets().forEach(onlyVarint ->
        {
            assertEquals(1L, onlyVarint.value());
        });
        assertTrue(dataEx.fetchOffsets().sizeof() > 0);
        final UnsafeBuffer expectedMessageKey = new UnsafeBuffer("match".getBytes(UTF_8));
        assertEquals(new OctetsFW().wrap(expectedMessageKey, 0, expectedMessageKey.capacity()), dataEx.messageKey());
    }

    @Test
    public void shouldGenerateEndExtension()
    {
        byte[] build = KafkaFunctions.endEx()
                                     .typeId(0x01)
                                     .fetchOffset(1L)
                                     .build();
        DirectBuffer buffer = new UnsafeBuffer(build);
        KafkaEndExFW endEx = new KafkaEndExFW().wrap(buffer, 0, buffer.capacity());
        assertEquals(0x01, endEx.typeId());
        endEx.fetchOffsets().forEach(onlyVarint ->
        {
            assertEquals(1L, onlyVarint.value());
        });
        assertTrue(endEx.fetchOffsets().sizeof() > 0);
    }

    @Test
    public void shouldMatchDataExtension() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .timestamp(12345678L)
                                             .fetchOffset(1L)
                                             .messageKey("match")
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
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
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchDataExtensionTimestamp() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .timestamp(12345678L)
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchDataExtensionFetchOffset() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .fetchOffset(1L)
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
                .build();

        assertNotNull(matcher.match(byteBuf));
    }

    @Test
    public void shouldMatchDataExtensionMessageKey() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .messageKey("match")
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
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
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
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
    public void shouldNotMatchDataExtensionTimestamp() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .timestamp(123456789L)
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
                .build();

        matcher.match(byteBuf);
    }

    @Test(expected = Exception.class)
    public void shouldNotMatchDataExtensionFetchOffset() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .timestamp(12345678L)
                                             .fetchOffset(2L)
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
                .build();

        matcher.match(byteBuf);
    }

    @Test(expected = Exception.class)
    public void shouldNotMatchDataExtensionMessageKey() throws Exception
    {
        BytesMatcher matcher = KafkaFunctions.matchDataEx()
                                             .typeId(0x01)
                                             .timestamp(12345678L)
                                             .fetchOffset(1L)
                                             .messageKey("no match")
                                             .build();

        ByteBuffer byteBuf = ByteBuffer.allocate(1024);

        new KafkaDataExFW.Builder().wrap(new UnsafeBuffer(byteBuf), 0, byteBuf.capacity())
                .typeId(0x01)
                .timestamp(12345678L)
                .fetchOffsets(m -> m.item(i -> i.set(1L)))
                .messageKey(m -> m.set("match".getBytes(UTF_8)))
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
