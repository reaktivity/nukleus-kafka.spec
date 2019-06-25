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

import static java.lang.Long.highestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.kaazing.k3po.lang.el.BytesMatcher;
import org.kaazing.k3po.lang.el.Function;
import org.kaazing.k3po.lang.el.spi.FunctionMapperSpi;
import org.reaktivity.specification.kafka.internal.types.ArrayFW;
import org.reaktivity.specification.kafka.internal.types.OctetsFW;
import org.reaktivity.specification.kafka.internal.types.Varint64FW;
import org.reaktivity.specification.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaEndExFW;

public final class KafkaFunctions
{
    @Function
    public static KafkaRouteExBuilder routeEx()
    {
        return new KafkaRouteExBuilder();
    }

    @Function
    public static KafkaBeginExBuilder beginEx()
    {
        return new KafkaBeginExBuilder();
    }

    @Function
    public static KafkaDataExBuilder dataEx()
    {
        return new KafkaDataExBuilder();
    }

    @Function
    public static KafkaDataExMatcherBuilder matchDataEx()
    {
        return new KafkaDataExMatcherBuilder();
    }

    @Function
    public static KafkaEndExBuilder endEx()
    {
        return new KafkaEndExBuilder();
    }

    @Function
    public static int length(String value)
    {
        return value.length();
    }

    @Function
    public static short lengthAsShort(String value)
    {
        return (short) value.length();
    }

    @Function
    public static int newRequestId()
    {
        return ThreadLocalRandom.current().nextInt();
    }

    @Function
    public static byte[] randomBytes(int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = (byte) ThreadLocalRandom.current().nextInt(0x100);
        }
        return bytes;
    }

    @Function
    public static long timestamp()
    {
        return currentTimeMillis();
    }

    @Function
    public static byte[] varint(
        long value)
    {
        final long bits = (value << 1) ^ (value >> 63);

        switch (bits != 0L ? (int) Math.ceil((1 + numberOfTrailingZeros(highestOneBit(bits))) / 7.0) : 1)
        {
        case 1:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f)
            };
        case 2:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f)
            };
        case 3:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f)
            };
        case 4:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f | 0x80),
                (byte) ((bits >> 21) & 0x7f)
            };
        case 5:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f | 0x80),
                (byte) ((bits >> 21) & 0x7f | 0x80),
                (byte) ((bits >> 28) & 0x7f)
            };
        case 6:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f | 0x80),
                (byte) ((bits >> 21) & 0x7f | 0x80),
                (byte) ((bits >> 28) & 0x7f | 0x80),
                (byte) ((bits >> 35) & 0x7f)
            };
        case 7:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f | 0x80),
                (byte) ((bits >> 21) & 0x7f | 0x80),
                (byte) ((bits >> 28) & 0x7f | 0x80),
                (byte) ((bits >> 35) & 0x7f | 0x80),
                (byte) ((bits >> 42) & 0x7f)
            };
        case 8:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f | 0x80),
                (byte) ((bits >> 21) & 0x7f | 0x80),
                (byte) ((bits >> 28) & 0x7f | 0x80),
                (byte) ((bits >> 35) & 0x7f | 0x80),
                (byte) ((bits >> 42) & 0x7f | 0x80),
                (byte) ((bits >> 49) & 0x7f),
            };
        case 9:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f | 0x80),
                (byte) ((bits >> 21) & 0x7f | 0x80),
                (byte) ((bits >> 28) & 0x7f | 0x80),
                (byte) ((bits >> 35) & 0x7f | 0x80),
                (byte) ((bits >> 42) & 0x7f | 0x80),
                (byte) ((bits >> 49) & 0x7f | 0x80),
                (byte) ((bits >> 56) & 0x7f),
            };
        default:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0x7f | 0x80),
                (byte) ((bits >> 7) & 0x7f | 0x80),
                (byte) ((bits >> 14) & 0x7f | 0x80),
                (byte) ((bits >> 21) & 0x7f | 0x80),
                (byte) ((bits >> 28) & 0x7f | 0x80),
                (byte) ((bits >> 35) & 0x7f | 0x80),
                (byte) ((bits >> 42) & 0x7f | 0x80),
                (byte) ((bits >> 49) & 0x7f | 0x80),
                (byte) ((bits >> 56) & 0x7f | 0x80),
                (byte) ((bits >> 63) & 0x01)
            };
        }
    }

    public static final class KafkaRouteExBuilder
    {
        private final DirectBuffer bufferRO = new UnsafeBuffer();

        private final KafkaRouteExFW.Builder routeExRW;

        private KafkaRouteExBuilder()
        {
            MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);
            this.routeExRW = new KafkaRouteExFW.Builder().wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaRouteExBuilder topicName(
            String topicName)
        {
            routeExRW.topicName(topicName);
            return this;
        }

        public KafkaRouteExBuilder header(
            String name,
            String value)
        {
            bufferRO.wrap(value.getBytes(UTF_8));
            routeExRW.headersItem(b -> b.key(name).value(bufferRO, 0, bufferRO.capacity()));
            return this;
        }

        public byte[] build()
        {
            final KafkaRouteExFW routeEx = routeExRW.build();
            final byte[] array = new byte[routeEx.sizeof()];
            routeEx.buffer().getBytes(routeEx.offset(), array);
            return array;
        }
    }

    public static final class KafkaBeginExBuilder
    {
        private final DirectBuffer bufferRO = new UnsafeBuffer();

        private final KafkaBeginExFW.Builder beginExRW;

        private KafkaBeginExBuilder()
        {
            MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);
            this.beginExRW = new KafkaBeginExFW.Builder().wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaBeginExBuilder typeId(
            int typeId)
        {
            beginExRW.typeId(typeId);
            return this;
        }

        public KafkaBeginExBuilder topicName(
            String topicName)
        {
            beginExRW.topicName(topicName);
            return this;
        }

        public KafkaBeginExBuilder fetchOffset(
            long... fetchOffsets)
        {
            beginExRW.fetchOffsets(m -> Arrays.stream(fetchOffsets).forEach(o -> m.item(i -> i.set(o))));
            return this;
        }

        public KafkaBeginExBuilder fetchKey(
            String fetchKey)
        {
            beginExRW.fetchKey(m -> m.set(fetchKey.getBytes(UTF_8)));
            return this;
        }

        public KafkaBeginExBuilder fetchKeyHash(
            int... fetchKeyHash)
        {
            beginExRW.fetchKeyHash(Arrays.stream(fetchKeyHash).iterator());
            return this;
        }

        public KafkaBeginExBuilder header(
            String name,
            String value)
        {
            bufferRO.wrap(value.getBytes(UTF_8));
            beginExRW.headersItem(b -> b.key(name).value(bufferRO, 0, bufferRO.capacity()));
            return this;
        }

        public byte[] build()
        {
            final KafkaBeginExFW beginEx = beginExRW.build();
            final byte[] array = new byte[beginEx.sizeof()];
            beginEx.buffer().getBytes(beginEx.offset(), array);
            return array;
        }
    }

    public static final class KafkaDataExBuilder
    {
        private final KafkaDataExFW.Builder dataExRW;

        private KafkaDataExBuilder()
        {
            MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);
            this.dataExRW = new KafkaDataExFW.Builder().wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaDataExBuilder typeId(
            int typeId)
        {
            dataExRW.typeId(typeId);
            return this;
        }

        public KafkaDataExBuilder timestamp(
            long timestamp)
        {
            dataExRW.timestamp(timestamp);
            return this;
        }

        public KafkaDataExBuilder fetchOffset(
            long... fetchOffsets)
        {
            dataExRW.fetchOffsets(m -> Arrays.stream(fetchOffsets).forEach(o -> m.item(i -> i.set(o))));
            return this;
        }

        public KafkaDataExBuilder messageKey(
            String messageKey)
        {
            dataExRW.messageKey(m -> m.set(messageKey.getBytes(UTF_8)));
            return this;
        }

        public byte[] build()
        {
            final KafkaDataExFW dataEx = dataExRW.build();
            final byte[] array = new byte[dataEx.sizeof()];
            dataEx.buffer().getBytes(dataEx.offset(), array);
            return array;
        }
    }

    public static final class KafkaEndExBuilder
    {
        private final KafkaEndExFW.Builder endExRW;

        private KafkaEndExBuilder()
        {
            MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);
            this.endExRW = new KafkaEndExFW.Builder().wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaEndExBuilder typeId(
            int typeId)
        {
            endExRW.typeId(typeId);
            return this;
        }

        public KafkaEndExBuilder fetchOffset(
            long... fetchOffsets)
        {
            endExRW.fetchOffsets(m -> Arrays.stream(fetchOffsets).forEach(o -> m.item(i -> i.set(o))));
            return this;
        }

        public byte[] build()
        {
            final KafkaEndExFW endEx = endExRW.build();
            final byte[] array = new byte[endEx.sizeof()];
            endEx.buffer().getBytes(endEx.offset(), array);
            return array;
        }
    }

    public static final class KafkaDataExMatcherBuilder
    {
        private final KafkaDataExFW dataExRO = new KafkaDataExFW();
        private final DirectBuffer bufferRO = new UnsafeBuffer();

        private Integer typeId;
        private Long timestamp;
        private ArrayFW.Builder<Varint64FW.Builder, Varint64FW> fetchOffsetsRW;
        private OctetsFW.Builder messageKeyRW;

        private KafkaDataExMatcherBuilder()
        {
        }

        public KafkaDataExMatcherBuilder typeId(
            int typeId)
        {
            this.typeId = typeId;
            return this;
        }

        public KafkaDataExMatcherBuilder timestamp(
            long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        public KafkaDataExMatcherBuilder fetchOffset(
            long... fetchOffsets)
        {
            fetchOffsetsRW = new ArrayFW.Builder<>(new Varint64FW.Builder(), new Varint64FW())
                    .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
            Arrays.stream(fetchOffsets).forEach(o -> fetchOffsetsRW.item(i -> i.set(o)));
            return this;
        }

        public KafkaDataExMatcherBuilder messageKey(
            String messageKey)
        {
            messageKeyRW = new OctetsFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
            messageKeyRW.set(messageKey.getBytes(UTF_8));
            return this;
        }

        public BytesMatcher build()
        {
            return this::match;
        }

        private Object match(
            ByteBuffer byteBuf) throws Exception
        {
            bufferRO.wrap(byteBuf);
            final KafkaDataExFW dataEx = dataExRO.tryWrap(bufferRO, byteBuf.position(), byteBuf.capacity());

            if (dataEx != null)
            {
                byteBuf.position(dataEx.limit());

                if (!matchTypeId(dataEx) ||
                    !matchTimestamp(dataEx) ||
                    !matchFetchOffsets(dataEx) ||
                    !matchMessageKey(dataEx))
                {
                    throw new Exception(dataEx.toString());
                }
            }

            return dataEx;
        }

        private boolean matchTypeId(
            final KafkaDataExFW dataEx)
        {
            return typeId == null || typeId == dataEx.typeId();
        }

        private boolean matchTimestamp(
            final KafkaDataExFW dataEx)
        {
            return timestamp == null || timestamp == dataEx.timestamp();
        }

        private boolean matchFetchOffsets(
            final KafkaDataExFW dataEx)
        {
            return fetchOffsetsRW == null || fetchOffsetsRW.build().equals(dataEx.fetchOffsets());
        }

        private boolean matchMessageKey(
            final KafkaDataExFW dataEx)
        {
            return messageKeyRW == null || messageKeyRW.build().equals(dataEx.messageKey());
        }
    }

    private KafkaFunctions()
    {
        // utility
    }

    public static class Mapper extends FunctionMapperSpi.Reflective
    {

        public Mapper()
        {
            super(KafkaFunctions.class);
        }

        @Override
        public String getPrefixName()
        {
            return "kafka";
        }
    }
}
