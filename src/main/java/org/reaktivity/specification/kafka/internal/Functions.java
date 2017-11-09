/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.System.currentTimeMillis;

import java.util.concurrent.ThreadLocalRandom;

import org.kaazing.k3po.lang.el.Function;
import org.kaazing.k3po.lang.el.spi.FunctionMapperSpi;

public final class Functions
{
    @Function
    public static int newRequestId()
    {
        return ThreadLocalRandom.current().nextInt();
    }

    @Function
    public static long timestamp()
    {
        return currentTimeMillis();
    }

    @Function
    public static byte[] varint(
        int value)
    {
        final int bits = (value << 1) ^ (value >> 31);

        switch (numberOfTrailingZeros(highestOneBit(bits)) >> 3)
        {
        case 0:
        case 4:
            return new byte[]
            {
                (byte) ((bits >> 0) & 0xff)
            };
        case 1:
            return new byte[]
            {
                (byte) ((bits >> 8) & 0xff),
                (byte) ((bits >> 0) & 0xff)
            };
        case 2:
            return new byte[]
            {
                (byte) ((bits >> 16) & 0xff),
                (byte) ((bits >> 8) & 0xff),
                (byte) ((bits >> 0) & 0xff)
            };
        case 3:
        default:
            return new byte[]
            {
                (byte) ((bits >> 24) & 0xff),
                (byte) ((bits >> 16) & 0xff),
                (byte) ((bits >> 8) & 0xff),
                (byte) ((bits >> 0) & 0xff)
            };
        }
    }

    private Functions()
    {
        // utility
    }

    public static class Mapper extends FunctionMapperSpi.Reflective
    {

        public Mapper()
        {
            super(Functions.class);
        }

        @Override
        public String getPrefixName()
        {
            return "kafka";
        }
    }
}
