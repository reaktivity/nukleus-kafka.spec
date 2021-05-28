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
package org.reaktivity.specification.kafka.internal;

import static java.lang.Long.highestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.specification.kafka.internal.types.KafkaOffsetFW.Builder.DEFAULT_LATEST_OFFSET;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.kaazing.k3po.lang.el.BytesMatcher;
import org.kaazing.k3po.lang.el.Function;
import org.kaazing.k3po.lang.el.spi.FunctionMapperSpi;
import org.reaktivity.specification.kafka.internal.types.Array32FW;
import org.reaktivity.specification.kafka.internal.types.KafkaCapabilities;
import org.reaktivity.specification.kafka.internal.types.KafkaConditionFW;
import org.reaktivity.specification.kafka.internal.types.KafkaDeltaFW;
import org.reaktivity.specification.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.specification.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.specification.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.specification.kafka.internal.types.KafkaHeadersFW;
import org.reaktivity.specification.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.specification.kafka.internal.types.KafkaNotFW;
import org.reaktivity.specification.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.specification.kafka.internal.types.KafkaOffsetType;
import org.reaktivity.specification.kafka.internal.types.KafkaSkip;
import org.reaktivity.specification.kafka.internal.types.KafkaSkipFW;
import org.reaktivity.specification.kafka.internal.types.KafkaValueFW;
import org.reaktivity.specification.kafka.internal.types.KafkaValueMatchFW;
import org.reaktivity.specification.kafka.internal.types.OctetsFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaApi;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaBootstrapBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDescribeBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaDescribeDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaFetchBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaFetchDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaFetchFlushExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaFlushExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaMergedBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaMergedDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaMergedFlushExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaMetaBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaMetaDataExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaProduceBeginExFW;
import org.reaktivity.specification.kafka.internal.types.stream.KafkaProduceDataExFW;

public final class KafkaFunctions
{
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
    public static KafkaFlushExBuilder flushEx()
    {
        return new KafkaFlushExBuilder();
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

    public abstract static class KafkaHeadersBuilder<T>
    {
        private final KafkaHeadersFW.Builder headersRW = new KafkaHeadersFW.Builder();
        private final DirectBuffer nameRO = new UnsafeBuffer(0, 0);
        private final DirectBuffer valueRO = new UnsafeBuffer(0, 0);

        private KafkaHeadersBuilder(
            String name)
        {
            MutableDirectBuffer buffer = new UnsafeBuffer(new byte[1024]);
            headersRW.wrap(buffer, 0, buffer.capacity());
            nameRO.wrap(name.getBytes(UTF_8));

            headersRW.nameLen(nameRO.capacity())
                     .name(nameRO, 0, nameRO.capacity());
        }

        public KafkaHeadersBuilder<T> sequence(
            String... values)
        {
            for (String value : values)
            {
                valueRO.wrap(value.getBytes(UTF_8));
                headersRW.valuesItem(vi -> vi.value(vb -> vb.length(valueRO.capacity())
                                                            .value(valueRO, 0, valueRO.capacity())));
            }
            return this;
        }

        public KafkaHeadersBuilder<T> skip(
            int count)
        {
            for (int i = 0; i < count; i++)
            {
                headersRW.valuesItem(vi -> vi.skip(sb -> sb.set(KafkaSkip.SKIP)));
            }
            return this;
        }

        public KafkaHeadersBuilder<T> skipMany()
        {
            headersRW.valuesItem(vi -> vi.skip(sb -> sb.set(KafkaSkip.SKIP_MANY)));
            return this;
        }

        public T build()
        {
            final KafkaHeadersFW headers = headersRW.build();
            return build(headers);
        }

        protected abstract T build(
            KafkaHeadersFW headers);


        protected void set(
            KafkaConditionFW.Builder builder,
            KafkaHeadersFW headers)
        {
            final OctetsFW name = headers.name();
            final int length = headers.nameLen();
            final Array32FW<KafkaValueMatchFW> values = headers.values();
            builder.headers(hb -> set(hb, name, length, values));
        }

        private void set(
            KafkaHeadersFW.Builder builder,
            OctetsFW name,
            int length,
            Array32FW<KafkaValueMatchFW> values)
        {
            builder.nameLen(length)
                   .name(name);
            values.forEach(v -> builder.valuesItem(vb -> set(vb, v)));
        }

        private void set(
            KafkaValueMatchFW.Builder builder,
            KafkaValueMatchFW valueMatch)
        {
            switch (valueMatch.kind())
            {
            case KafkaValueMatchFW.KIND_VALUE:
                final KafkaValueFW value = valueMatch.value();
                builder.value(vb -> vb.length(value.length())
                                      .value(value.value()));
                break;
            case KafkaValueMatchFW.KIND_SKIP:
                final KafkaSkipFW skip = valueMatch.skip();
                builder.skip(s -> s.set(skip.get()));
                break;
            }
        }
    }

    public abstract static class KafkaFilterBuilder<T>
    {
        private final KafkaFilterFW.Builder filterRW = new KafkaFilterFW.Builder();
        private final DirectBuffer keyRO = new UnsafeBuffer(0, 0);
        private final DirectBuffer nameRO = new UnsafeBuffer(0, 0);
        private final DirectBuffer valueRO = new UnsafeBuffer(0, 0);

        private KafkaFilterBuilder()
        {
            MutableDirectBuffer filterBuffer = new UnsafeBuffer(new byte[1024]);
            filterRW.wrap(filterBuffer, 0, filterBuffer.capacity());
        }

        public KafkaFilterBuilder<T> key(
            String key)
        {
            if (key != null)
            {
                keyRO.wrap(key.getBytes(UTF_8));
                filterRW.conditionsItem(c -> c.key(k -> k.length(keyRO.capacity())
                                                         .value(keyRO, 0, keyRO.capacity())));
            }
            else
            {
                filterRW.conditionsItem(c -> c.key(k -> k.length(-1)
                                                         .value((OctetsFW) null)));
            }
            return this;
        }

        public KafkaFilterBuilder<T> header(
            String name,
            String value)
        {
            if (value == null)
            {
                nameRO.wrap(name.getBytes(UTF_8));
                filterRW.conditionsItem(c -> c.header(h -> h.nameLen(nameRO.capacity())
                                                            .name(nameRO, 0, nameRO.capacity())
                                                            .valueLen(-1)
                                                            .value((OctetsFW) null)));
            }
            else
            {
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(value.getBytes(UTF_8));
                filterRW.conditionsItem(c -> c.header(h -> h.nameLen(nameRO.capacity())
                                                            .name(nameRO, 0, nameRO.capacity())
                                                            .valueLen(valueRO.capacity())
                                                            .value(valueRO, 0, valueRO.capacity())));
            }
            return this;
        }

        public KafkaHeadersBuilder<KafkaFilterBuilder<T>> headers(
            String name)
        {
            return new KafkaHeadersBuilder<>(name)
            {
                @Override
                protected KafkaFilterBuilder<T> build(
                    KafkaHeadersFW headers)
                {
                    filterRW.conditionsItem(ci -> set(ci, headers));
                    return KafkaFilterBuilder.this;
                }
            };
        }

        public KafkaFilterBuilder<T> keyNot(
            String key)
        {
            if (key != null)
            {
                keyRO.wrap(key.getBytes(UTF_8));
                filterRW.conditionsItem(i -> i.not(n -> n.condition(c -> c.key(k -> k.length(keyRO.capacity())
                                                                                     .value(keyRO, 0, keyRO.capacity())))));
            }
            else
            {
                filterRW.conditionsItem(i -> i.not(n -> n.condition(c -> c.key(k -> k.length(-1)
                                                                                     .value((OctetsFW) null)))));
            }
            return this;
        }

        public KafkaFilterBuilder<T> headerNot(
            String name,
            String value)
        {
            if (value == null)
            {
                nameRO.wrap(name.getBytes(UTF_8));
                filterRW.conditionsItem(i -> i.not(n -> n.condition(c -> c.header(h -> h.nameLen(nameRO.capacity())
                                                                                        .name(nameRO, 0, nameRO.capacity())
                                                                                        .valueLen(-1)
                                                                                        .value((OctetsFW) null)))));
            }
            else
            {
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(value.getBytes(UTF_8));
                filterRW.conditionsItem(i -> i.not(n -> n.condition(c -> c.header(h ->
                                                                            h.nameLen(nameRO.capacity())
                                                                              .name(nameRO, 0, nameRO.capacity())
                                                                              .valueLen(valueRO.capacity())
                                                                              .value(valueRO, 0, valueRO.capacity())))));
            }
            return this;
        }

        public T build()
        {
            final KafkaFilterFW filter = filterRW.build();
            return build(filter);
        }

        protected abstract T build(
            KafkaFilterFW filter);

        protected void set(
            KafkaFilterFW.Builder builder,
            KafkaFilterFW filter)
        {
            final Array32FW<KafkaConditionFW> conditions = filter.conditions();
            builder.conditions(csb -> set(csb, conditions));
        }

        private void set(
            Array32FW.Builder<KafkaConditionFW.Builder, KafkaConditionFW> builder,
            Array32FW<KafkaConditionFW> conditions)
        {
            conditions.forEach(c -> builder.item(ib -> set(ib, c)));
        }

        private void set(
            KafkaConditionFW.Builder builder,
            KafkaConditionFW condition)
        {
            switch (condition.kind())
            {
            case KafkaConditionFW.KIND_KEY:
                final KafkaKeyFW key = condition.key();
                builder.key(kb -> kb.length(key.length())
                                    .value(key.value()));
                break;
            case KafkaConditionFW.KIND_HEADER:
                final KafkaHeaderFW header = condition.header();
                builder.header(hb -> hb.nameLen(header.nameLen())
                                       .name(header.name())
                                       .valueLen(header.valueLen())
                                       .value(header.value()));
                break;
            case KafkaConditionFW.KIND_NOT:
                final KafkaNotFW not = condition.not();
                final KafkaConditionFW notCondition = not.condition();

                switch (notCondition.kind())
                {
                case KafkaConditionFW.KIND_KEY:
                    final KafkaKeyFW notKey = notCondition.key();
                    builder.not(n -> n.condition(c -> c.key(kb -> kb.length(notKey.length())
                                                                    .value(notKey.value()))));
                    break;
                case KafkaConditionFW.KIND_HEADER:
                    final KafkaHeaderFW notHeader = notCondition.header();
                    builder.not(n -> n.condition(c -> c.header(hb -> hb.nameLen(notHeader.nameLen())
                                                                       .name(notHeader.name())
                                                                       .valueLen(notHeader.valueLen())
                                                                       .value(notHeader.value()))));
                    break;
                }
                break;
            case KafkaConditionFW.KIND_HEADERS:
                final KafkaHeadersFW headers = condition.headers();
                final Array32FW<KafkaValueMatchFW> values = headers.values();
                builder.headers(hb -> hb.nameLen(headers.nameLen())
                                        .name(headers.name())
                                        .values(values));
                break;
            }
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

        public KafkaBootstrapBeginExBuilder bootstrap()
        {
            beginExRW.kind(KafkaApi.BOOTSTRAP.value());

            return new KafkaBootstrapBeginExBuilder();
        }

        public KafkaMergedBeginExBuilder merged()
        {
            beginExRW.kind(KafkaApi.MERGED.value());

            return new KafkaMergedBeginExBuilder();
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

        public KafkaProduceBeginExBuilder produce()
        {
            beginExRW.kind(KafkaApi.PRODUCE.value());

            return new KafkaProduceBeginExBuilder();
        }

        public byte[] build()
        {
            final KafkaBeginExFW beginEx = beginExRO;
            final byte[] array = new byte[beginEx.sizeof()];
            beginEx.buffer().getBytes(beginEx.offset(), array);
            return array;
        }

        public final class KafkaBootstrapBeginExBuilder
        {
            private final KafkaBootstrapBeginExFW.Builder bootstrapBeginExRW = new KafkaBootstrapBeginExFW.Builder();

            private KafkaBootstrapBeginExBuilder()
            {
                bootstrapBeginExRW.wrap(writeBuffer, KafkaBeginExFW.FIELD_OFFSET_BOOTSTRAP, writeBuffer.capacity());
            }

            public KafkaBootstrapBeginExBuilder topic(
                String topic)
            {
                bootstrapBeginExRW.topic(topic);
                return this;
            }

            public KafkaBeginExBuilder build()
            {
                final KafkaBootstrapBeginExFW bootstrapBeginEx = bootstrapBeginExRW.build();
                beginExRO.wrap(writeBuffer, 0, bootstrapBeginEx.limit());
                return KafkaBeginExBuilder.this;
            }
        }

        public final class KafkaMergedBeginExBuilder
        {
            private final KafkaMergedBeginExFW.Builder mergedBeginExRW = new KafkaMergedBeginExFW.Builder();

            private KafkaMergedBeginExBuilder()
            {
                mergedBeginExRW.wrap(writeBuffer, KafkaBeginExFW.FIELD_OFFSET_MERGED, writeBuffer.capacity());
            }

            public KafkaMergedBeginExBuilder capabilities(
                String capabilities)
            {
                mergedBeginExRW.capabilities(c -> c.set(KafkaCapabilities.valueOf(capabilities)));
                return this;
            }

            public KafkaMergedBeginExBuilder topic(
                String topic)
            {
                mergedBeginExRW.topic(topic);
                return this;
            }

            public KafkaMergedBeginExBuilder partition(
                int partitionId,
                long offset)
            {
                partition(partitionId, offset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaMergedBeginExBuilder partition(
                int partitionId,
                long offset,
                long latestOffset)
            {
                mergedBeginExRW.partitionsItem(p -> p.partitionId(partitionId)
                                                     .partitionOffset(offset)
                                                     .latestOffset(latestOffset));
                return this;
            }

            public KafkaFilterBuilder<KafkaMergedBeginExBuilder> filter()
            {
                return new KafkaFilterBuilder<>()
                {

                    @Override
                    protected KafkaMergedBeginExBuilder build(
                        KafkaFilterFW filter)
                    {
                        mergedBeginExRW.filtersItem(fb -> set(fb, filter));
                        return KafkaMergedBeginExBuilder.this;
                    }
                };
            }

            public KafkaMergedBeginExBuilder deltaType(
                String delta)
            {
                mergedBeginExRW.deltaType(d -> d.set(KafkaDeltaType.valueOf(delta)));
                return this;
            }

            public KafkaBeginExBuilder build()
            {
                final KafkaMergedBeginExFW mergedBeginEx = mergedBeginExRW.build();
                beginExRO.wrap(writeBuffer, 0, mergedBeginEx.limit());
                return KafkaBeginExBuilder.this;
            }
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

            public KafkaFetchBeginExBuilder partition(
                int partitionId,
                long offset)
            {
                partition(partitionId, offset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaFetchBeginExBuilder partition(
                int partitionId,
                long offset,
                long latestOffset)
            {
                fetchBeginExRW.partition(p -> p.partitionId(partitionId)
                                               .partitionOffset(offset)
                                               .latestOffset(latestOffset));
                return this;
            }

            public KafkaFilterBuilder<KafkaFetchBeginExBuilder> filter()
            {
                return new KafkaFilterBuilder<>()
                {

                    @Override
                    protected KafkaFetchBeginExBuilder build(
                        KafkaFilterFW filter)
                    {
                        fetchBeginExRW.filtersItem(fb -> set(fb, filter));
                        return KafkaFetchBeginExBuilder.this;
                    }
                };
            }

            public KafkaFetchBeginExBuilder deltaType(
                String deltaType)
            {
                fetchBeginExRW.deltaType(d -> d.set(KafkaDeltaType.valueOf(deltaType)));
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

        public final class KafkaProduceBeginExBuilder
        {
            private final KafkaProduceBeginExFW.Builder produceBeginExRW = new KafkaProduceBeginExFW.Builder();

            private boolean transactionSet;

            private KafkaProduceBeginExBuilder()
            {
                produceBeginExRW.wrap(writeBuffer, KafkaBeginExFW.FIELD_OFFSET_PRODUCE, writeBuffer.capacity());
            }

            public KafkaProduceBeginExBuilder transaction(
                String transaction)
            {
                produceBeginExRW.transaction(transaction);
                transactionSet = true;
                return this;
            }

            public KafkaProduceBeginExBuilder producerId(
                long producerId)
            {
                ensureTransactionSet();
                produceBeginExRW.producerId(producerId);
                return this;
            }

            public KafkaProduceBeginExBuilder topic(
                String topic)
            {
                ensureTransactionSet();
                produceBeginExRW.topic(topic);
                return this;
            }

            public KafkaProduceBeginExBuilder partition(
                int partitionId)
            {
                partition(partitionId, DEFAULT_LATEST_OFFSET, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaProduceBeginExBuilder partition(
                int partitionId,
                long partitionOffset)
            {
                partition(partitionId, partitionOffset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaProduceBeginExBuilder partition(
                int partitionId,
                long partitionOffset,
                long latestOffset)
            {
                produceBeginExRW.partition(p -> p.partitionId(partitionId)
                                                 .partitionOffset(partitionOffset)
                                                 .latestOffset(latestOffset));
                return this;
            }

            public KafkaBeginExBuilder build()
            {
                final KafkaProduceBeginExFW produceBeginEx = produceBeginExRW.build();
                beginExRO.wrap(writeBuffer, 0, produceBeginEx.limit());
                return KafkaBeginExBuilder.this;
            }

            private void ensureTransactionSet()
            {
                if (!transactionSet)
                {
                    transaction(null);
                }
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

        public KafkaMergedDataExBuilder merged()
        {
            dataExRW.kind(KafkaApi.MERGED.value());

            return new KafkaMergedDataExBuilder();
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

        public KafkaProduceDataExBuilder produce()
        {
            dataExRW.kind(KafkaApi.PRODUCE.value());

            return new KafkaProduceDataExBuilder();
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
            private final DirectBuffer keyRO = new UnsafeBuffer(0, 0);
            private final DirectBuffer nameRO = new UnsafeBuffer(0, 0);
            private final DirectBuffer valueRO = new UnsafeBuffer(0, 0);

            private final KafkaFetchDataExFW.Builder fetchDataExRW = new KafkaFetchDataExFW.Builder();

            private KafkaFetchDataExBuilder()
            {
                fetchDataExRW.wrap(writeBuffer, KafkaDataExFW.FIELD_OFFSET_FETCH, writeBuffer.capacity());
            }

            public KafkaFetchDataExBuilder deferred(
                int deferred)
            {
                fetchDataExRW.deferred(deferred);
                return this;
            }

            public KafkaFetchDataExBuilder timestamp(
                long timestamp)
            {
                fetchDataExRW.timestamp(timestamp);
                return this;
            }

            public KafkaFetchDataExBuilder partition(
                int partitionId,
                long partitionOffset)
            {
                fetchDataExRW.partition(p -> p.partitionId(partitionId).partitionOffset(partitionOffset));
                return this;
            }

            public KafkaFetchDataExBuilder partition(
                int partitionId,
                long partitionOffset,
                long latestOffset)
            {
                fetchDataExRW.partition(p -> p.partitionId(partitionId)
                                              .partitionOffset(partitionOffset)
                                              .latestOffset(latestOffset));
                return this;
            }

            public KafkaFetchDataExBuilder key(
                String key)
            {
                if (key == null)
                {
                    fetchDataExRW.key(m -> m.length(-1)
                                            .value((OctetsFW) null));
                }
                else
                {
                    keyRO.wrap(key.getBytes(UTF_8));
                    fetchDataExRW.key(k -> k.length(keyRO.capacity())
                                            .value(keyRO, 0, keyRO.capacity()));
                }
                return this;
            }

            public KafkaFetchDataExBuilder delta(
                String deltaType,
                long ancestorOffset)
            {
                fetchDataExRW.delta(d -> d.type(t -> t.set(KafkaDeltaType.valueOf(deltaType)))
                                          .ancestorOffset(ancestorOffset));
                return this;
            }

            public KafkaFetchDataExBuilder header(
                String name,
                String value)
            {
                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    fetchDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                    .name(nameRO, 0, nameRO.capacity())
                                                    .valueLen(-1)
                                                    .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value.getBytes(UTF_8));
                    fetchDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                    .name(nameRO, 0, nameRO.capacity())
                                                    .valueLen(valueRO.capacity())
                                                    .value(valueRO, 0, valueRO.capacity()));
                }
                return this;
            }

            public KafkaDataExBuilder build()
            {
                final KafkaFetchDataExFW fetchDataEx = fetchDataExRW.build();
                dataExRO.wrap(writeBuffer, 0, fetchDataEx.limit());
                return KafkaDataExBuilder.this;
            }
        }

        public final class KafkaMergedDataExBuilder
        {
            private final DirectBuffer keyRO = new UnsafeBuffer(0, 0);
            private final DirectBuffer nameRO = new UnsafeBuffer(0, 0);
            private final DirectBuffer valueRO = new UnsafeBuffer(0, 0);

            private final KafkaMergedDataExFW.Builder mergedDataExRW = new KafkaMergedDataExFW.Builder();

            private KafkaMergedDataExBuilder()
            {
                mergedDataExRW.wrap(writeBuffer, KafkaDataExFW.FIELD_OFFSET_MERGED, writeBuffer.capacity());
            }

            public KafkaMergedDataExBuilder deferred(
                int deferred)
            {
                mergedDataExRW.deferred(deferred);
                return this;
            }

            public KafkaMergedDataExBuilder timestamp(
                long timestamp)
            {
                mergedDataExRW.timestamp(timestamp);
                return this;
            }

            public KafkaMergedDataExBuilder partition(
                int partitionId,
                long partitionOffset)
            {
                partition(partitionId, partitionOffset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaMergedDataExBuilder partition(
                int partitionId,
                long partitionOffset,
                long latestOffset)
            {
                mergedDataExRW.partition(p -> p.partitionId(partitionId).partitionOffset(partitionOffset)
                                               .latestOffset(latestOffset));
                return this;
            }

            public KafkaMergedDataExBuilder progress(
                int partitionId,
                long partitionOffset)
            {
                progress(partitionId, partitionOffset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaMergedDataExBuilder progress(
                int partitionId,
                long partitionOffset,
                long latestOffset)
            {
                mergedDataExRW.progressItem(p -> p.partitionId(partitionId).partitionOffset(partitionOffset)
                                                  .latestOffset(latestOffset));
                return this;
            }

            public KafkaMergedDataExBuilder key(
                String key)
            {
                if (key == null)
                {
                    mergedDataExRW.key(m -> m.length(-1)
                                            .value((OctetsFW) null));
                }
                else
                {
                    keyRO.wrap(key.getBytes(UTF_8));
                    mergedDataExRW.key(k -> k.length(keyRO.capacity())
                                            .value(keyRO, 0, keyRO.capacity()));
                }
                return this;
            }

            public KafkaMergedDataExBuilder delta(
                String deltaType,
                long ancestorOffset)
            {
                mergedDataExRW.delta(d -> d.type(t -> t.set(KafkaDeltaType.valueOf(deltaType)))
                                           .ancestorOffset(ancestorOffset));
                return this;
            }

            public KafkaMergedDataExBuilder header(
                String name,
                String value)
            {
                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                    .name(nameRO, 0, nameRO.capacity())
                                                    .valueLen(-1)
                                                    .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value.getBytes(UTF_8));
                    mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                    .name(nameRO, 0, nameRO.capacity())
                                                    .valueLen(valueRO.capacity())
                                                    .value(valueRO, 0, valueRO.capacity()));
                }
                return this;
            }

            public KafkaMergedDataExBuilder headerNull(
                String name)
            {
                nameRO.wrap(name.getBytes(UTF_8));
                mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                .name(nameRO, 0, nameRO.capacity())
                                                .valueLen(-1)
                                                .value((OctetsFW) null));
                return this;
            }

            public KafkaMergedDataExBuilder headerByte(
                String name,
                byte value)
            {
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Byte.BYTES).put(value));
                mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                .name(nameRO, 0, nameRO.capacity())
                                                .valueLen(valueRO.capacity())
                                                .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExBuilder headerShort(
                String name,
                short value)
            {
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Short.BYTES).putShort(value));
                mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                .name(nameRO, 0, nameRO.capacity())
                                                .valueLen(valueRO.capacity())
                                                .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExBuilder headerInt(
                String name,
                int value)
            {
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value));
                mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                .name(nameRO, 0, nameRO.capacity())
                                                .valueLen(valueRO.capacity())
                                                .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExBuilder headerLong(
                String name,
                long value)
            {
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value));
                mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                .name(nameRO, 0, nameRO.capacity())
                                                .valueLen(valueRO.capacity())
                                                .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExBuilder headerBytes(
                String name,
                byte[] value)
            {
                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                     .name(nameRO, 0, nameRO.capacity())
                                                     .valueLen(-1)
                                                     .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value);
                    mergedDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                     .name(nameRO, 0, nameRO.capacity())
                                                     .valueLen(valueRO.capacity())
                                                     .value(valueRO, 0, valueRO.capacity()));
                }
                return this;
            }

            public KafkaDataExBuilder build()
            {
                final KafkaMergedDataExFW mergedDataEx = mergedDataExRW.build();
                dataExRO.wrap(writeBuffer, 0, mergedDataEx.limit());
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

        public final class KafkaProduceDataExBuilder
        {
            private final DirectBuffer keyRO = new UnsafeBuffer(0, 0);
            private final DirectBuffer nameRO = new UnsafeBuffer(0, 0);
            private final DirectBuffer valueRO = new UnsafeBuffer(0, 0);

            private final KafkaProduceDataExFW.Builder produceDataExRW = new KafkaProduceDataExFW.Builder();

            private KafkaProduceDataExBuilder()
            {
                produceDataExRW.wrap(writeBuffer, KafkaDataExFW.FIELD_OFFSET_DESCRIBE, writeBuffer.capacity());
            }

            public KafkaProduceDataExBuilder deferred(
                int deferred)
            {
                produceDataExRW.deferred(deferred);
                return this;
            }

            public KafkaProduceDataExBuilder timestamp(
                long timestamp)
            {
                produceDataExRW.timestamp(timestamp);
                return this;
            }

            public KafkaProduceDataExBuilder sequence(
                int sequence)
            {
                produceDataExRW.sequence(sequence);
                return this;
            }

            public KafkaProduceDataExBuilder key(
                String key)
            {
                if (key == null)
                {
                    produceDataExRW.key(m -> m.length(-1)
                                              .value((OctetsFW) null));
                }
                else
                {
                    keyRO.wrap(key.getBytes(UTF_8));
                    produceDataExRW.key(k -> k.length(keyRO.capacity())
                                              .value(keyRO, 0, keyRO.capacity()));
                }
                return this;
            }

            public KafkaProduceDataExBuilder header(
                String name,
                String value)
            {
                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    produceDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                      .name(nameRO, 0, nameRO.capacity())
                                                      .valueLen(-1)
                                                      .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value.getBytes(UTF_8));
                    produceDataExRW.headersItem(h -> h.nameLen(nameRO.capacity())
                                                      .name(nameRO, 0, nameRO.capacity())
                                                      .valueLen(valueRO.capacity())
                                                      .value(valueRO, 0, valueRO.capacity()));
                }
                return this;
            }

            public KafkaDataExBuilder build()
            {
                final KafkaProduceDataExFW produceDataEx = produceDataExRW.build();
                dataExRO.wrap(writeBuffer, 0, produceDataEx.limit());
                return KafkaDataExBuilder.this;
            }
        }
    }

    public static final class KafkaFlushExBuilder
    {
        private final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024 * 8]);

        private final KafkaFlushExFW flushExRO = new KafkaFlushExFW();

        private final KafkaFlushExFW.Builder flushExRW = new KafkaFlushExFW.Builder();

        private KafkaFlushExBuilder()
        {
            flushExRW.wrap(writeBuffer, 0, writeBuffer.capacity());
        }

        public KafkaFlushExBuilder typeId(
            int typeId)
        {
            flushExRW.typeId(typeId);
            return this;
        }

        public KafkaMergedFlushExBuilder merged()
        {
            flushExRW.kind(KafkaApi.MERGED.value());

            return new KafkaMergedFlushExBuilder();
        }

        public KafkaFetchFlushExBuilder fetch()
        {
            flushExRW.kind(KafkaApi.FETCH.value());

            return new KafkaFetchFlushExBuilder();
        }

        public byte[] build()
        {
            final KafkaFlushExFW flushEx = flushExRO;
            final byte[] array = new byte[flushEx.sizeof()];
            flushEx.buffer().getBytes(flushEx.offset(), array);
            return array;
        }

        public final class KafkaMergedFlushExBuilder
        {
            private final KafkaMergedFlushExFW.Builder mergedFlushExRW = new KafkaMergedFlushExFW.Builder();

            private KafkaMergedFlushExBuilder()
            {
                mergedFlushExRW.wrap(writeBuffer, KafkaFlushExFW.FIELD_OFFSET_FETCH, writeBuffer.capacity());
            }

            public KafkaMergedFlushExBuilder progress(
                int partitionId,
                long offset)
            {
                progress(partitionId, offset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaMergedFlushExBuilder progress(
                int partitionId,
                long offset,
                long latestOffset)
            {
                mergedFlushExRW.progressItem(p -> p.partitionId(partitionId).partitionOffset(offset).latestOffset(latestOffset));
                return this;
            }

            public KafkaMergedFlushExBuilder capabilities(
                String capabilities)
            {
                mergedFlushExRW.capabilities(c -> c.set(KafkaCapabilities.valueOf(capabilities)));
                return this;
            }

            public KafkaFilterBuilder<KafkaMergedFlushExBuilder> filter()
            {
                return new KafkaFilterBuilder<>()
                {

                    @Override
                    protected KafkaMergedFlushExBuilder build(
                        KafkaFilterFW filter)
                    {
                        mergedFlushExRW.filtersItem(fb -> set(fb, filter));
                        return KafkaMergedFlushExBuilder.this;
                    }
                };
            }

            public KafkaFlushExBuilder build()
            {
                final KafkaMergedFlushExFW mergedFlushEx = mergedFlushExRW.build();
                flushExRO.wrap(writeBuffer, 0, mergedFlushEx.limit());
                return KafkaFlushExBuilder.this;
            }
        }

        public final class KafkaFetchFlushExBuilder
        {
            private final KafkaFetchFlushExFW.Builder fetchFlushExRW = new KafkaFetchFlushExFW.Builder();

            private KafkaFetchFlushExBuilder()
            {
                fetchFlushExRW.wrap(writeBuffer, KafkaFlushExFW.FIELD_OFFSET_FETCH, writeBuffer.capacity());
            }

            public KafkaFetchFlushExBuilder partition(
                int partitionId,
                long offset)
            {
                fetchFlushExRW.partition(p -> p.partitionId(partitionId).partitionOffset(offset));
                return this;
            }

            public KafkaFetchFlushExBuilder partition(
                int partitionId,
                long offset,
                long latestOffset)
            {
                fetchFlushExRW.partition(p -> p.partitionId(partitionId)
                                               .partitionOffset(offset)
                                               .latestOffset(latestOffset));
                return this;
            }

            public KafkaFlushExBuilder build()
            {
                final KafkaFetchFlushExFW fetchFlushEx = fetchFlushExRW.build();
                flushExRO.wrap(writeBuffer, 0, fetchFlushEx.limit());
                return KafkaFlushExBuilder.this;
            }
        }
    }

    public static final class KafkaDataExMatcherBuilder
    {
        private final DirectBuffer bufferRO = new UnsafeBuffer();

        private final DirectBuffer keyRO = new UnsafeBuffer(0, 0);
        private final DirectBuffer nameRO = new UnsafeBuffer(0, 0);
        private final DirectBuffer valueRO = new UnsafeBuffer(0, 0);

        private final KafkaDataExFW dataExRO = new KafkaDataExFW();

        private Integer typeId;
        private Integer kind;
        private Predicate<KafkaDataExFW> caseMatcher;

        public KafkaMergedDataExMatcherBuilder merged()
        {
            final KafkaMergedDataExMatcherBuilder matcherBuilder = new KafkaMergedDataExMatcherBuilder();

            this.kind = KafkaApi.MERGED.value();
            this.caseMatcher = matcherBuilder::match;
            return matcherBuilder;
        }

        public KafkaFetchDataExMatcherBuilder fetch()
        {
            final KafkaFetchDataExMatcherBuilder matcherBuilder = new KafkaFetchDataExMatcherBuilder();

            this.kind = KafkaApi.FETCH.value();
            this.caseMatcher = matcherBuilder::match;
            return matcherBuilder;
        }

        public KafkaProduceDataExMatcherBuilder produce()
        {
            final KafkaProduceDataExMatcherBuilder matcherBuilder = new KafkaProduceDataExMatcherBuilder();

            this.kind = KafkaApi.PRODUCE.value();
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
            if (!byteBuf.hasRemaining())
            {
                return null;
            }

            bufferRO.wrap(byteBuf);
            final KafkaDataExFW dataEx = dataExRO.tryWrap(bufferRO, byteBuf.position(), byteBuf.capacity());

            if (dataEx != null &&
                matchTypeId(dataEx) &&
                matchKind(dataEx) &&
                matchCase(dataEx))
            {
                byteBuf.position(byteBuf.position() + dataEx.sizeof());
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
            private Integer deferred;
            private Long timestamp;
            private KafkaOffsetFW.Builder partitionRW;
            private KafkaKeyFW.Builder keyRW;
            private KafkaDeltaFW.Builder deltaRW;
            private Array32FW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW;

            private KafkaFetchDataExMatcherBuilder()
            {
            }

            public KafkaFetchDataExMatcherBuilder deferred(
                int deferred)
            {
                this.deferred = deferred;
                return this;
            }

            public KafkaFetchDataExMatcherBuilder timestamp(
                long timestamp)
            {
                this.timestamp = timestamp;
                return this;
            }

            public KafkaFetchDataExMatcherBuilder partition(
                int partitionId,
                long partitionOffset)
            {
                partition(partitionId, partitionOffset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaFetchDataExMatcherBuilder partition(
                int partitionId,
                long partitionOffset,
                long latestOffset)
            {
                assert partitionRW == null;
                partitionRW = new KafkaOffsetFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                partitionRW.partitionId(partitionId).partitionOffset(partitionOffset).latestOffset(latestOffset);

                return this;
            }

            public KafkaFetchDataExMatcherBuilder key(
                String key)
            {
                assert keyRW == null;
                keyRW = new KafkaKeyFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                if (key == null)
                {
                    keyRW.length(-1)
                         .value((OctetsFW) null);
                }
                else
                {
                    keyRO.wrap(key.getBytes(UTF_8));
                    keyRW.length(keyRO.capacity())
                         .value(keyRO, 0, keyRO.capacity());
                }

                return this;
            }

            public KafkaFetchDataExMatcherBuilder delta(
                String delta,
                long ancestorOffset)
            {
                assert deltaRW == null;
                deltaRW = new KafkaDeltaFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                deltaRW.type(t -> t.set(KafkaDeltaType.valueOf(delta))).ancestorOffset(ancestorOffset);

                return this;
            }

            public KafkaFetchDataExMatcherBuilder header(
                String name,
                String value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }

                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(-1)
                                         .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value.getBytes(UTF_8));
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(valueRO.capacity())
                                         .value(valueRO, 0, valueRO.capacity()));
                }

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
                return matchDeferred(fetchDataEx) &&
                    matchTimestamp(fetchDataEx) &&
                    matchPartition(fetchDataEx) &&
                    matchKey(fetchDataEx) &&
                    matchDelta(fetchDataEx) &&
                    matchHeaders(fetchDataEx);
            }

            private boolean matchDeferred(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return deferred == null || deferred == fetchDataEx.deferred();
            }

            private boolean matchTimestamp(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return timestamp == null || timestamp == fetchDataEx.timestamp();
            }

            private boolean matchPartition(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return partitionRW == null || partitionRW.build().equals(fetchDataEx.partition());
            }

            private boolean matchKey(
                    final KafkaFetchDataExFW fetchDataEx)
            {
                return keyRW == null || keyRW.build().equals(fetchDataEx.key());
            }

            private boolean matchDelta(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return deltaRW == null || deltaRW.build().equals(fetchDataEx.delta());
            }

            private boolean matchHeaders(
                final KafkaFetchDataExFW fetchDataEx)
            {
                return headersRW == null || headersRW.build().equals(fetchDataEx.headers());
            }
        }

        public final class KafkaProduceDataExMatcherBuilder
        {
            private Integer deferred;
            private Long timestamp;
            private Integer sequence;
            private KafkaKeyFW.Builder keyRW;
            private Array32FW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW;

            private KafkaProduceDataExMatcherBuilder()
            {
            }

            public KafkaProduceDataExMatcherBuilder deferred(
                int deferred)
            {
                this.deferred = deferred;
                return this;
            }

            public KafkaProduceDataExMatcherBuilder timestamp(
                long timestamp)
            {
                this.timestamp = timestamp;
                return this;
            }

            public KafkaProduceDataExMatcherBuilder sequence(
                int sequence)
            {
                this.sequence = sequence;
                return this;
            }

            public KafkaProduceDataExMatcherBuilder key(
                String key)
            {
                assert keyRW == null;
                keyRW = new KafkaKeyFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                if (key == null)
                {
                    keyRW.length(-1)
                         .value((OctetsFW) null);
                }
                else
                {
                    keyRO.wrap(key.getBytes(UTF_8));
                    keyRW.length(keyRO.capacity())
                         .value(keyRO, 0, keyRO.capacity());
                }

                return this;
            }

            public KafkaProduceDataExMatcherBuilder header(
                String name,
                String value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                  .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }

                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(-1)
                                         .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value.getBytes(UTF_8));
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(valueRO.capacity())
                                         .value(valueRO, 0, valueRO.capacity()));
                }

                return this;
            }

            public KafkaDataExMatcherBuilder build()
            {
                return KafkaDataExMatcherBuilder.this;
            }

            private boolean match(
                KafkaDataExFW dataEx)
            {
                final KafkaProduceDataExFW produceDataEx = dataEx.produce();
                return matchDeferred(produceDataEx) &&
                    matchTimestamp(produceDataEx) &&
                    matchSequence(produceDataEx) &&
                    matchKey(produceDataEx) &&
                    matchHeaders(produceDataEx);
            }

            private boolean matchDeferred(
                final KafkaProduceDataExFW produceDataEx)
            {
                return deferred == null || deferred == produceDataEx.deferred();
            }

            private boolean matchTimestamp(
                final KafkaProduceDataExFW produceDataEx)
            {
                return timestamp == null || timestamp == produceDataEx.timestamp();
            }

            private boolean matchSequence(
                final KafkaProduceDataExFW produceDataEx)
            {
                return sequence == null || sequence == produceDataEx.sequence();
            }

            private boolean matchKey(
                final KafkaProduceDataExFW produceDataEx)
            {
                return keyRW == null || keyRW.build().equals(produceDataEx.key());
            }

            private boolean matchHeaders(
                final KafkaProduceDataExFW produceDataEx)
            {
                return headersRW == null || headersRW.build().equals(produceDataEx.headers());
            }
        }

        public final class KafkaMergedDataExMatcherBuilder
        {
            private Integer deferred;
            private Long timestamp;
            private KafkaOffsetFW.Builder partitionRW;
            private Array32FW.Builder<KafkaOffsetFW.Builder, KafkaOffsetFW> progressRW;
            private KafkaDeltaFW.Builder deltaRW;
            private KafkaKeyFW.Builder keyRW;
            private Array32FW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW;

            private KafkaMergedDataExMatcherBuilder()
            {
            }

            public KafkaMergedDataExMatcherBuilder deferred(
                int deferred)
            {
                this.deferred = deferred;
                return this;
            }

            public KafkaMergedDataExMatcherBuilder timestamp(
                long timestamp)
            {
                this.timestamp = timestamp;
                return this;
            }

            public KafkaMergedDataExMatcherBuilder partition(
                int partitionId,
                long offset)
            {
                partition(partitionId, offset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaMergedDataExMatcherBuilder partition(
                int partitionId,
                long offset,
                long latestOffset)
            {
                assert partitionRW == null;
                partitionRW = new KafkaOffsetFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                partitionRW.partitionId(partitionId).partitionOffset(offset).latestOffset(latestOffset);

                return this;
            }

            public KafkaMergedDataExMatcherBuilder progress(
                int partitionId,
                long offset)
            {
                progress(partitionId, offset, DEFAULT_LATEST_OFFSET);
                return this;
            }

            public KafkaMergedDataExMatcherBuilder progress(
                int partitionId,
                long offset,
                long latestOffset)
            {
                if (progressRW == null)
                {
                    this.progressRW = new Array32FW.Builder<>(new KafkaOffsetFW.Builder(), new KafkaOffsetFW())
                                                 .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }
                progressRW.item(i -> i.partitionId(partitionId).partitionOffset(offset).latestOffset(latestOffset));
                return this;
            }

            public KafkaMergedDataExMatcherBuilder key(
                String key)
            {
                assert keyRW == null;
                keyRW = new KafkaKeyFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                if (key == null)
                {
                    keyRW.length(-1)
                         .value((OctetsFW) null);
                }
                else
                {
                    keyRO.wrap(key.getBytes(UTF_8));
                    keyRW.length(keyRO.capacity())
                         .value(keyRO, 0, keyRO.capacity());
                }

                return this;
            }

            public KafkaMergedDataExMatcherBuilder delta(
                String delta,
                long ancestorOffset)
            {
                assert deltaRW == null;
                deltaRW = new KafkaDeltaFW.Builder().wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);

                deltaRW.type(t -> t.set(KafkaDeltaType.valueOf(delta))).ancestorOffset(ancestorOffset);

                return this;
            }

            public KafkaMergedDataExMatcherBuilder header(
                String name,
                String value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }

                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(-1)
                                         .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value.getBytes(UTF_8));
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(valueRO.capacity())
                                         .value(valueRO, 0, valueRO.capacity()));
                }

                return this;
            }

            public KafkaMergedDataExMatcherBuilder headerNull(
                String name)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }
                nameRO.wrap(name.getBytes(UTF_8));
                headersRW.item(i -> i.nameLen(nameRO.capacity())
                                     .name(nameRO, 0, nameRO.capacity())
                                     .valueLen(-1)
                                     .value((OctetsFW) null));
                return this;
            }

            public KafkaMergedDataExMatcherBuilder headerByte(
                String name,
                byte value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Byte.BYTES).put(value));
                headersRW.item(i -> i.nameLen(nameRO.capacity())
                                     .name(nameRO, 0, nameRO.capacity())
                                     .valueLen(valueRO.capacity())
                                     .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExMatcherBuilder headerShort(
                String name,
                short value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Short.BYTES).putShort(value));
                headersRW.item(i -> i.nameLen(nameRO.capacity())
                                     .name(nameRO, 0, nameRO.capacity())
                                     .valueLen(valueRO.capacity())
                                     .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExMatcherBuilder headerInt(
                String name,
                int value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value));
                headersRW.item(i -> i.nameLen(nameRO.capacity())
                                     .name(nameRO, 0, nameRO.capacity())
                                     .valueLen(valueRO.capacity())
                                     .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExMatcherBuilder headerLong(
                String name,
                long value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }
                nameRO.wrap(name.getBytes(UTF_8));
                valueRO.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value));
                headersRW.item(i -> i.nameLen(nameRO.capacity())
                                     .name(nameRO, 0, nameRO.capacity())
                                     .valueLen(valueRO.capacity())
                                     .value(valueRO, 0, valueRO.capacity()));
                return this;
            }

            public KafkaMergedDataExMatcherBuilder headerBytes(
                String name,
                byte[] value)
            {
                if (headersRW == null)
                {
                    this.headersRW = new Array32FW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                                                .wrap(new UnsafeBuffer(new byte[1024]), 0, 1024);
                }

                if (value == null)
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(-1)
                                         .value((OctetsFW) null));
                }
                else
                {
                    nameRO.wrap(name.getBytes(UTF_8));
                    valueRO.wrap(value);
                    headersRW.item(i -> i.nameLen(nameRO.capacity())
                                         .name(nameRO, 0, nameRO.capacity())
                                         .valueLen(valueRO.capacity())
                                         .value(valueRO, 0, valueRO.capacity()));
                }

                return this;
            }

            public KafkaDataExMatcherBuilder build()
            {
                return KafkaDataExMatcherBuilder.this;
            }

            private boolean match(
                KafkaDataExFW dataEx)
            {
                final KafkaMergedDataExFW mergedDataEx = dataEx.merged();
                return matchPartition(mergedDataEx) &&
                    matchProgress(mergedDataEx) &&
                    matchDeferred(mergedDataEx) &&
                    matchTimestamp(mergedDataEx) &&
                    matchKey(mergedDataEx) &&
                    matchDelta(mergedDataEx) &&
                    matchHeaders(mergedDataEx);
            }

            private boolean matchPartition(
                final KafkaMergedDataExFW mergedDataEx)
            {
                return partitionRW == null || partitionRW.build().equals(mergedDataEx.partition());
            }

            private boolean matchProgress(
                final KafkaMergedDataExFW mergedDataEx)
            {
                return progressRW == null || progressRW.build().equals(mergedDataEx.progress());
            }

            private boolean matchDeferred(
                final KafkaMergedDataExFW mergedDataEx)
            {
                return deferred == null || deferred == mergedDataEx.deferred();
            }

            private boolean matchTimestamp(
                final KafkaMergedDataExFW mergedDataEx)
            {
                return timestamp == null || timestamp == mergedDataEx.timestamp();
            }

            private boolean matchKey(
                final KafkaMergedDataExFW mergedDataEx)
            {
                return keyRW == null || keyRW.build().equals(mergedDataEx.key());
            }

            private boolean matchDelta(
                final KafkaMergedDataExFW mergedDataEx)
            {
                return deltaRW == null || deltaRW.build().equals(mergedDataEx.delta());
            }

            private boolean matchHeaders(
                final KafkaMergedDataExFW mergedDataEx)
            {
                return headersRW == null || headersRW.build().equals(mergedDataEx.headers());
            }
        }
    }

    @Function
    public static long offset(
        String type)
    {
        return KafkaOffsetType.valueOf(type).value();
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
