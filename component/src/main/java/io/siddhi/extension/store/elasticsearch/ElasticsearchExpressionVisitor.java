/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.store.elasticsearch;

import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.extension.store.elasticsearch.exceptions.ElasticsearchConditionVisitorException;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;

/**
 * This class represents the Condition vistor implementation specific to Elasticsearch record tables.
 */
public class ElasticsearchExpressionVisitor extends BaseExpressionVisitor {

    private StringBuilder expression;

    public ElasticsearchExpressionVisitor() {
        expression = new StringBuilder();
    }

    public String returnExpression() {
        return expression.toString().trim();
    }

    @Override
    public void beginVisitAnd() {
        throw new ElasticsearchConditionVisitorException("'And' not supported at set in Solr Store ");
    }

    @Override
    public void endVisitAnd() {
        //Not applicable
    }

    @Override
    public void beginVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {
        throw new ElasticsearchConditionVisitorException("'Or' not supported at set in Elasticsearch Store ");
    }

    @Override
    public void endVisitOr() {
        //Not applicable
    }

    @Override
    public void beginVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void endVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitNot() {
        throw new ElasticsearchConditionVisitorException("'Not' not supported at set in Elasticsearch Store ");
    }

    @Override
    public void endVisitNot() {
        //Not applicable
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        throw new ElasticsearchConditionVisitorException("'" + operator + "' not supported at set " +
                "in Elasticsearch Store ");
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        throw new ElasticsearchConditionVisitorException("'Null' not supported at set in Elasticsearch Store ");
    }

    @Override
    public void endVisitIsNull(String streamId) {
        //Not applicable
    }

    @Override
    public void beginVisitIn(String storeId) {
        throw new ElasticsearchConditionVisitorException("'In' not supported at set in Elasticsearch Store ");
    }

    @Override
    public void endVisitIn(String storeId) {
        //Not applicable
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {

    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        expression.append(value.toString());
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        throw new ElasticsearchConditionVisitorException("'" + mathOperator +
                "' not supported at set in Elasticsearch Store ");
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        throw new ElasticsearchConditionVisitorException("Function '"
                + namespace + ":" + functionName + "' not supported at set in Elasticsearch Store ");
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
        //Not applicable
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        expression.append("[" + id + "]");
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {

    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        expression.append(attributeName);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {

    }
}
