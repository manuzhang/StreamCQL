/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.cql.semanticanalyzer.parser.visitor;

import java.util.Locale;

import org.antlr.v4.runtime.misc.NotNull;

import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.semanticanalyzer.parser.CQLParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.CreateFunctionStatementContext;

/**
 * create function 语法遍历
 * 
 */
public class CreateFunctionStatementVisitor extends AbsCQLParserBaseVisitor<CreateFunctionStatementContext>
{
    private CreateFunctionStatementContext context = null;
    
    /**
     * <默认构造函数>
     */
    public CreateFunctionStatementVisitor()
    {
        context = new CreateFunctionStatementContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected CreateFunctionStatementContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateFunctionStatementContext visitFunctionName(@NotNull CQLParser.FunctionNameContext ctx)
    {
        context.setFunctionName(ctx.getText().toLowerCase(Locale.US));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateFunctionStatementContext visitClassName(@NotNull CQLParser.ClassNameContext ctx)
    {
        context.setClassName(CQLUtils.cqlStringLiteralTrim(ctx.getText()));
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
     public CreateFunctionStatementContext visitStreamProperties(@NotNull CQLParser.StreamPropertiesContext ctx) {
        StreamPropertiesVisitor visitor = new StreamPropertiesVisitor();
        context.setFunctionProperties(visitor.visit(ctx));
        return context;}


}
