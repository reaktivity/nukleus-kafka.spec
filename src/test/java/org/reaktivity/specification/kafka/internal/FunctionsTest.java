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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.kaazing.k3po.lang.internal.el.ExpressionFactoryUtils.newExpressionFactory;

import javax.el.ELContext;
import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import org.junit.Before;
import org.junit.Test;
import org.kaazing.k3po.lang.internal.el.ExpressionContext;

public class FunctionsTest
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
    public void shouldInvokeNewRequestId() throws Exception
    {
        String expressionText = "${kafka:newRequestId()}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, Integer.class);
        Object actual = expression.getValue(ctx);
        assertTrue(actual instanceof Integer);
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
    public void shouldComputeVarintFourBytes() throws Exception
    {
        String expressionText = "${kafka:varint(17000000)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x02, 0x06, (byte) 0xcc, (byte) 0x80 }, actuals);
    }

    @Test
    public void shouldComputeVarintThreeBytes() throws Exception
    {
        String expressionText = "${kafka:varint(80000)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { 0x02, 0x71, 0x00 }, actuals);
    }

    @Test
    public void shouldComputeVarintTwoBytes() throws Exception
    {
        String expressionText = "${kafka:varint(300)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { (byte) 0x02, 0x58 }, actuals);
    }

    @Test
    public void shouldComputeVarintOneByte() throws Exception
    {
        String expressionText = "${kafka:varint(12)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { 0x18 }, actuals);
    }

    @Test
    public void shouldComputeVarintZero() throws Exception
    {
        String expressionText = "${kafka:varint(0)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { 0x00 }, actuals);
    }

    @Test
    public void shouldComputeVarintMinus1() throws Exception
    {
        String expressionText = "${kafka:varint(-1)}";
        ValueExpression expression = factory.createValueExpression(ctx, expressionText, byte[].class);
        byte[] actuals = (byte[]) expression.getValue(ctx);
        assertArrayEquals(new byte[] { 0x01 }, actuals);
    }
}
