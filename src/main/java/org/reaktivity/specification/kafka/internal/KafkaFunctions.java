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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.kaazing.k3po.lang.el.BytesMatcher;
import org.kaazing.k3po.lang.el.Function;
import org.kaazing.k3po.lang.el.spi.FunctionMapperSpi;
import org.reaktivity.specification.kafka.internal.types.ArrayFW;
import org.reaktivity.specification.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.specification.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.specification.kafka.internal.types.KafkaOffsetFW;
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
    public static int length(
        String value)
    {
        return value.length();
    }

    @Function
    public static short lengthAsShort(
        String value)
    {
        return (short) value.length();
    }

    @Function
    public static int newRequestId()
    {
        return ThreadLocalRandom.current().nextInt();
    }

    @Function
    public static byte[] randomBytes(
        int length)
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
        private final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);

        private final KafkaBeginExFW beginExRO = new KafkaBeginExFW();

        private final KafkaBeginExFW.Builder beginExRW = new KafkaBeginExFW.Builder();

        private KafkaBeginExBuilder()
        {
            beginExRW.wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaBeginExBuilder typeId(
            int typeId)
        {
            beginExRW.typeId(typeId);
            return this;
        }

        public KafkaFetchBeginExBuilder fetch()
        {
            beginExRW.kind(KafkaApi.FETCH.value());

            return new KafkaFetchBeginExBuilder();
        }

        public KafkaMetaBeginExBuilder meta()
        {
            beginExRW.kind(KafkaApi.META.value());

            return new KafkaMetaBeginExBuilder();
        }

        public KafkaDescribeBeginExBuilder describe()
        {
            beginExRW.kind(KafkaApi.DESCRIBE.value());

            return new KafkaDescribeBeginExBuilder();
        }

        public byte[] build()
        {
            final KafkaBeginExFW beginEx = beginExRO;
            final byte[] array = new byte[beginEx.sizeof()];
            beginEx.buffer().getBytes(beginEx.offset(), array);
            return array;
        }

        public final class KafkaFetchBeginExBuilder
        {
            private final KafkaFetchBeginExFW.Builder fetchBeginExRW = new KafkaFetchBeginExFW.Builder();

            private KafkaFetchBeginExBuilder()
            {
                fetchBeginExRW.wrap(writeBuffer, KafkaBeginExFW.FIELD_OFFSET_FETCH, writeBuffer.capacity());
            }

            public KafkaFetchBeginExBuilder topic(
                String topic)
            {
                fetchBeginExRW.topic(topic);
                return this;
            }

            public KafkaFetchBeginExBuilder progress(
                int partitionId,
                long offset)
            {
                fetchBeginExRW.progressItem(p -> p.partitionId(partitionId).offset$(offset));
                return this;
            }

            public KafkaFetchBeginExBuilder key(
                String key)
            {
                if (key != null)
                {
                    fetchBeginExRW.filtersItem(f -> f.conditionsItem(c -> c.key(k -> k.value(v -> v.set(key.getBytes(UTF_8))))));
                }
                else
                {
                    fetchBeginExRW.filtersItem(f -> f.conditionsItem(c -> c.key(k -> k.value((OctetsFW) null))));
                }
                return this;
            }

            public KafkaFetchBeginExBuilder header(
                String name,
                String value)
            {
                if (value == null)
                {
                    fetchBeginExRW.filtersItem(
                        f -> f.conditionsItem(c -> c.header(h -> h.name(name)
                                                                  .value((OctetsFW) null))));
                }
                else
                {
                    fetchBeginExRW.filtersItem(
                        f -> f.conditionsItem(c -> c.header(h -> h.name(name)
                                                                  .value(v -> v.set(value.getBytes(UTF_8))))));
                }
                return this;
            }

            public KafkaBeginExBuilder build()
            {
                final KafkaFetchBeginExFW fetchBeginEx = fetchBeginExRW.build();
                beginExRO.wrap(writeBuffer, 0, fetchBeginEx.limit());
                return KafkaBeginExBuilder.this;
            }
        }

        public final class KafkaMetaBeginExBuilder
        {
            private final KafkaMetaBeginExFW.Builder metaBeginExRW = new KafkaMetaBeginExFW.Builder();

            private KafkaMetaBeginExBuilder()
            {
                metaBeginExRW.wrap(writeBuffer, KafkaBeginExFW.FIELD_OFFSET_META, writeBuffer.capacity());
            }

            public KafkaMetaBeginExBuilder topic(
                String topic)
            {
                metaBeginExRW.topic(topic);
                return this;
            }

            public KafkaBeginExBuilder build()
            {
                final KafkaMetaBeginExFW metaBeginEx = metaBeginExRW.build();
                beginExRO.wrap(writeBuffer, 0, metaBeginEx.limit());
                return KafkaBeginExBuilder.this;
            }
        }

        public final class KafkaDescribeBeginExBuilder
        {
            private final KafkaDescribeBeginExFW.Builder describeBeginExRW = new KafkaDescribeBeginExFW.Builder();

            private KafkaDescribeBeginExBuilder()
            {
                describeBeginExRW.wrap(writeBuffer, KafkaBeginExFW.FIELD_OFFSET_DESCRIBE, writeBuffer.capacity());
            }

            public KafkaDescribeBeginExBuilder topic(
                String name)
            {
                describeBeginExRW.topic(name);
                return this;
            }

            public KafkaDescribeBeginExBuilder config(
                String name)
            {
                describeBeginExRW.configsItem(c -> c.set(name, UTF_8));
                return this;
            }

            public KafkaBeginExBuilder build()
            {
                final KafkaDescribeBeginExFW describeBeginEx = describeBeginExRW.build();
                beginExRO.wrap(writeBuffer, 0, describeBeginEx.limit());
                return KafkaBeginExBuilder.this;
            }
        }
    }

    public static final class KafkaDataExBuilder
    {
        private final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);

        private final KafkaDataExFW dataExRO = new KafkaDataExFW();

        private final KafkaDataExFW.Builder dataExRW = new KafkaDataExFW.Builder();

        private KafkaDataExBuilder()
        {
            dataExRW.wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaDataExBuilder typeId(
            int typeId)
        {
            dataExRW.typeId(typeId);
            return this;
        }

        public KafkaFetchDataExBuilder fetch()
        {
            dataExRW.kind(KafkaApi.FETCH.value());

            return new KafkaFetchDataExBuilder();
        }

        public KafkaMetaDataExBuilder meta()
        {
            dataExRW.kind(KafkaApi.META.value());

            return new KafkaMetaDataExBuilder();
        }

        public KafkaDescribeDataExBuilder describe()
        {
            dataExRW.kind(KafkaApi.DESCRIBE.value());

            return new KafkaDescribeDataExBuilder();
        }

        public byte[] build()
        {
            final KafkaDataExFW dataEx = dataExRO;
            final byte[] array = new byte[dataEx.sizeof()];
            dataEx.buffer().getBytes(dataEx.offset(), array);
            return array;
        }

        public final class KafkaFetchDataExBuilder
        {
            private final KafkaFetchDataExFW.Builder fetchDataExRW = new KafkaFetchDataExFW.Builder();

            private KafkaFetchDataExBuilder()
            {
                fetchDataExRW.wrap(writeBuffer, KafkaDataExFW.FIELD_OFFSET_FETCH, writeBuffer.capacity());
            }

            public KafkaFetchDataExBuilder timestamp(
                long timestamp)
            {
                fetchDataExRW.timestamp(timestamp);
                return this;
            }

            public KafkaFetchDataExBuilder key(
                String key)
            {
                if (key == null)
                {
                    fetchDataExRW.key(m -> m.value((OctetsFW) null));
                }
                else
                {
                    fetchDataExRW.key(m -> m.value(v -> v.set(key.getBytes(UTF_8))));
                }
                return this;
            }

            public KafkaFetchDataExBuilder header(
                String name,
                String value)
            {
                if (value == null)
                {
                    fetchDataExRW.headersItem(h -> h.name(name)
                                                    .value((OctetsFW) null));
                }
                else
                {
                    fetchDataExRW.headersItem(h -> h.name(name)
                                                    .value(v -> v.set(value.getBytes(UTF_8))));
                }
                return this;
            }

            public KafkaFetchDataExBuilder progress(
                int partitionId,
                long offset)
            {
                fetchDataExRW.progressItem(p -> p.partitionId(partitionId).offset$(offset));
                return this;
            }

            public KafkaDataExBuilder build()
            {
                final KafkaFetchDataExFW fetchDataEx = fetchDataExRW.build();
                dataExRO.wrap(writeBuffer, 0, fetchDataEx.limit());
                return KafkaDataExBuilder.this;
            }
        }

        public final class KafkaMetaDataExBuilder
        {
            private final KafkaMetaDataExFW.Builder metaDataExRW = new KafkaMetaDataExFW.Builder();

            private KafkaMetaDataExBuilder()
            {
                metaDataExRW.wrap(writeBuffer, KafkaDataExFW.FIELD_OFFSET_META, writeBuffer.capacity());
            }

            public KafkaMetaDataExBuilder partition(
                int partitionId,
                int leaderId)
            {
                metaDataExRW.partitionsItem(p -> p.partitionId(partitionId).leaderId(leaderId));
                return this;
            }

            public KafkaDataExBuilder build()
            {
                final KafkaMetaDataExFW metaDataEx = metaDataExRW.build();
                dataExRO.wrap(writeBuffer, 0, metaDataEx.limit());
                return KafkaDataExBuilder.this;
            }
        }

        public final class KafkaDescribeDataExBuilder
        {
            private final KafkaDescribeDataExFW.Builder describeDataExRW = new KafkaDescribeDataExFW.Builder();

            private KafkaDescribeDataExBuilder()
            {
                describeDataExRW.wrap(writeBuffer, KafkaDataExFW.FIELD_OFFSET_DESCRIBE, writeBuffer.capacity());
            }

            public KafkaDescribeDataExBuilder config(
                String name,
                String value)
            {
                describeDataExRW.configsItem(c -> c.name(name).value(value));
                return this;
            }

            public KafkaDataExBuilder build()
            {
                final KafkaDescribeDataExFW describeDataEx = describeDataExRW.build();
                dataExRO.wrap(writeBuffer, 0, describeDataEx.limit());
                return KafkaDataExBuilder.this;
            }
        }
    }

    public static final class KafkaEndExBuilder
    {
        private final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);

        private final KafkaEndExFW endExRO = new KafkaEndExFW();

        private final KafkaEndExFW.Builder endExRW = new KafkaEndExFW.Builder();

        private KafkaEndExBuilder()
        {
            endExRW.wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaEndExBuilder typeId(
            int typeId)
        {
            endExRW.typeId(typeId);
            return this;
        }

        public KafkaFetchEndExBuilder fetch()
        {
            endExRW.kind(KafkaApi.FETCH.value());

            return new KafkaFetchEndExBuilder();
        }

        public byte[] build()
        {
            final KafkaEndExFW endEx = endExRO;
            final byte[] array = new byte[endEx.sizeof()];
            endEx.buffer().getBytes(endEx.offset(), array);
            return array;
        }

        public final class KafkaFetchEndExBuilder
        {
            private final KafkaFetchEndExFW.Builder fetchEndExRW = new KafkaFetchEndExFW.Builder();

            private KafkaFetchEndExBuilder()
            {
                fetchEndExRW.wrap(writeBuffer, KafkaEndExFW.FIELD_OFFSET_FETCH, writeBuffer.capacity());
            }

            public KafkaFetchEndExBuilder progress(
                int partitionId,
                long offset)
            {
                fetchEndExRW.progressItem(p -> p.partitionId(partitionId).offset$(offset));
                return this;
            }

            public KafkaEndExBuilder build()
            {
                final KafkaFetchEndExFW fetchEndEx = fetchEndExRW.build();
                endExRO.wrap(writeBuffer, 0, fetchEndEx.limit());
                return KafkaEndExBuilder.this;
            }
        }
    }

    public static final class KafkaDataExMatcherBuilder
    {
        private final DirectBuffer bufferRO = new UnsafeBuffer();

        private final KafkaDataExFW dataExRO = new KafkaDataExFW();

        private Integer typeId;
        private Short kind;
        private Predicate<KafkaDataExFW> caseMatcher;

        public KafkaFetchDataExMatcherBuilder fetch()
        {
            final KafkaFetchDataExMatcherBuilder matcherBuilder = new KafkaFetchDataExMatcherBuilder();

            this.kind = KafkaApi.FETCH.value();
            this.caseMatcher = matcherBuilder::match;
            return matcherBuilder;
        }

        public KafkaDataExMatcherBuilder typeId(
            int typeId)
        {
            this.typeId = typeId;
            return this;
        }

        public BytesMatcher build()
        {
            return typeId != null || kind != null ? this::match : buf -> null;
        }

        private KafkaDataExFW match(
            ByteBuffer byteBuf) throws Exception
        {
            bufferRO.wrap(byteBuf);
            final KafkaDataExFW dataEx = dataExRO.tryWrap(bufferRO, byteBuf.position(), byteBuf.capacity());

            if (dataEx != null &&
                matchTypeId(dataEx) &&
                matchKind(dataEx) &&
                matchCase(dataEx))
            {
                return dataEx;
            }

            throw new Exception(dataEx.toString());
        }

        private boolean matchTypeId(
            final KafkaDataExFW dataEx)
        {
            return typeId == null || typeId == dataEx.typeId();
        }

        private boolean matchKind(
            final KafkaDataExFW dataEx)
        {
            return kind == null || kind == dataEx.kind();
        }

        private boolean matchCase(
            final KafkaDataExFW dataEx) throws Exception
        {
            return caseMatcher == null || caseMatcher.test(dataEx);
        }

        public final class KafkaFetchDataExMatcherBuilder
        {
            private Long timestamp;
            private KafkaKeyFW.Builder keyRW;
            private ArrayFW.Builder<KafkaOffsetFW.Builder, KafkaOffsetFW> progressRW;
            private ArrayFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW;

            private KafkaFetchDataExMatcherBuilder()
            {
            }

            public KafkaFetchDataExMatcherBuilder timestamp(
                long timestamp)
            {
                this.timestamp = timestamp;
                return this;
            }

            public KafkaFetchDataExMatcherBuilder key(
                String key)
            {
                assert keyRW == null;
                keyRW = new KafkaKeyFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                if (key == null)
                {
                    keyRW.value((OctetsFW) null);
                }
                else
                {
                    keyRW.value(v -> v.set(key.getBytes(UTF_8)));
                }

                return this;
            }

            public KafkaFetchDataExMatcherBuilder header(
                String name,
                String value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new ArrayFW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }

                if (value == null)
                {
                    headersRW.item(i -> i.name(name).value((OctetsFW) null));
                }
                else
                {
                    headersRW.item(i -> i.name(name).value(v -> v.set(value.getBytes(UTF_8))));
                }

                return this;
            }

            public KafkaFetchDataExMatcherBuilder progress(
                int partitionId,
                long offset)
            {
                if (progressRW == null)
                {
                    this.progressRW = new ArrayFW.Builder<>(new KafkaOffsetFW.Builder(), new KafkaOffsetFW())
                                                 .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }
                progressRW.item(i -> i.partitionId(partitionId).offset$(offset));
                return this;
            }

            public KafkaDataExMatcherBuilder build()
            {
                return KafkaDataExMatcherBuilder.this;
            }

            private boolean match(
                KafkaDataExFW dataEx)
            {
                final KafkaFetchDataExFW fetchDataEx = dataEx.fetch();
                return matchTimestamp(fetchDataEx) &&
                    matchKey(fetchDataEx) &&
                    matchHeaders(fetchDataEx) &&
                    matchProgress(fetchDataEx);
            }

            private boolean matchTimestamp(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return timestamp == null || timestamp == fetchDataEx.timestamp();
            }

            private boolean matchKey(
                    final KafkaFetchDataExFW fetchDataEx)
            {
                return keyRW == null || keyRW.build().equals(fetchDataEx.key());
            }

            private boolean matchHeaders(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return headersRW == null || headersRW.build().equals(fetchDataEx.headers());
            }

            private boolean matchProgress(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return progressRW == null || progressRW.build().equals(fetchDataEx.progress());
            }
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
