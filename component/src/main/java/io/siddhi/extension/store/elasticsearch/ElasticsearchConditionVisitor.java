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
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;

/**
 * This class represents the Condition vistor implementation specific to Elasticsearch record tables.
 */
public class ElasticsearchConditionVisitor extends BaseExpressionVisitor {

    private static final String WHITESPACE = " ";

    private static final String ELASTICSEARCH_AND = "AND";
    private static final String ELASTICSEARCH_OR = "OR";
    private static final String ELASTICSEARCH_NOT = "NOT";
    private static final String SQL_MATH_SUBTRACT = "-";

    private static final String OPEN_PARENTHESIS = "(";
    private static final String CLOSE_PARENTHESIS = ")";
    private static final String COLON = ":";
    private static final String OPEN_SQUARE_BRACKET = "[";
    private static final String CLOSE_SQUARE_BRACKET = "]";
    private static final String ASTERISK = "*";
    private static final String TO = "TO";
    private static final String OPEN_CURLY_BRACKET = "{";
    private static final String CLOSE_CURLY_BRACKET = "}";
    private static final String DOUBLE_QUOTE = "\"";
    private static final String EXCLAMATION_MARK = "!";


    private StringBuilder condition;
    private String currentStreamVariable;
    private String currentStoreVariable;
    private boolean isBeginCompareRightOperand;
    private boolean isStoreVariableOnRight;

    public ElasticsearchConditionVisitor() {
        condition = new StringBuilder();
    }

    public String returnCondition() {
        return condition.toString().trim();
    }

    @Override
    public void beginVisitAnd() {
        condition.append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitAnd() {
        condition.append(CLOSE_PARENTHESIS);
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
        condition.append(ELASTICSEARCH_AND).append(WHITESPACE);
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {
        condition.append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitOr() {
        condition.append(CLOSE_PARENTHESIS);
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
        condition.append(ELASTICSEARCH_OR).append(WHITESPACE);
    }

    @Override
    public void endVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitNot() {
        condition.append(ELASTICSEARCH_NOT).append(WHITESPACE).append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitNot() {
        condition.append(CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        condition.append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        condition.append(CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        switch (operator) {
            case NOT_EQUAL:
                condition.append(EXCLAMATION_MARK);
                break;
            default:
        }
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        isBeginCompareRightOperand = true;
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        condition.append(currentStoreVariable).append(COLON);
        if (!isStoreVariableOnRight) {
            switch (operator) {
                case LESS_THAN:
                    condition.append(OPEN_CURLY_BRACKET).append(ASTERISK).append(WHITESPACE).append(TO)
                            .append(WHITESPACE);
                    condition.append(currentStreamVariable);
                    condition.append(CLOSE_CURLY_BRACKET);
                    break;
                case GREATER_THAN:
                    condition.append(OPEN_CURLY_BRACKET);
                    condition.append(currentStreamVariable);
                    condition.append(WHITESPACE).append(TO).append(WHITESPACE).append(ASTERISK)
                            .append(CLOSE_CURLY_BRACKET);
                    break;
                case LESS_THAN_EQUAL:
                    condition.append(OPEN_SQUARE_BRACKET).append(ASTERISK).append(WHITESPACE).append(TO)
                            .append(WHITESPACE);
                    condition.append(currentStreamVariable);
                    condition.append(CLOSE_SQUARE_BRACKET);
                    break;
                case GREATER_THAN_EQUAL:
                    condition.append(OPEN_SQUARE_BRACKET);
                    condition.append(currentStreamVariable);
                    condition.append(WHITESPACE).append(TO).append(WHITESPACE).append(ASTERISK)
                            .append(CLOSE_SQUARE_BRACKET);
                    break;
                case EQUAL:
                case NOT_EQUAL:
                    condition.append(DOUBLE_QUOTE);
                    condition.append(currentStreamVariable);
                    condition.append(DOUBLE_QUOTE);
                    break;
            }
        } else {
            isStoreVariableOnRight = false;
            switch (operator) {
                case GREATER_THAN_EQUAL:
                    condition.append(OPEN_CURLY_BRACKET).append(ASTERISK).append(WHITESPACE).append(TO)
                            .append(WHITESPACE);
                    condition.append(currentStreamVariable);
                    condition.append(CLOSE_CURLY_BRACKET);
                    break;
                case LESS_THAN_EQUAL:
                    condition.append(OPEN_CURLY_BRACKET);
                    condition.append(currentStreamVariable);
                    condition.append(WHITESPACE).append(TO).append(WHITESPACE).append(ASTERISK)
                            .append(CLOSE_CURLY_BRACKET);
                    break;
                case GREATER_THAN:
                    condition.append(OPEN_SQUARE_BRACKET).append(ASTERISK).append(WHITESPACE)
                            .append(TO).append(WHITESPACE);
                    condition.append(currentStreamVariable);
                    condition.append(CLOSE_SQUARE_BRACKET);
                    break;
                case LESS_THAN:
                    condition.append(OPEN_SQUARE_BRACKET);
                    condition.append(currentStreamVariable);
                    condition.append(WHITESPACE).append(TO).append(WHITESPACE).append(ASTERISK)
                            .append(CLOSE_SQUARE_BRACKET);
                    break;
                case EQUAL:
                case NOT_EQUAL:
                    condition.append(DOUBLE_QUOTE);
                    condition.append(currentStreamVariable);
                    condition.append(DOUBLE_QUOTE);
                    break;
            }
        }
        isBeginCompareRightOperand = false;
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        condition.append(OPEN_PARENTHESIS).append(SQL_MATH_SUBTRACT);
    }

    @Override
    public void endVisitIsNull(String streamId) {
        condition.append(currentStoreVariable);
        condition.append(COLON).append(OPEN_SQUARE_BRACKET).append(ASTERISK).append(WHITESPACE).append(TO).append
                (WHITESPACE).append(ASTERISK).append(CLOSE_SQUARE_BRACKET).append(WHITESPACE).
                append(ELASTICSEARCH_AND).append(WHITESPACE).append(ASTERISK).append(COLON).append(ASTERISK).
                append(CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitIn(String storeId) {
        //Not applicable
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
        currentStreamVariable = value.toString();
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        //Not applicable
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
        //Not applicable
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
        String placeHolder = "[" + id + "]";
        currentStreamVariable = placeHolder;
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {

    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (isBeginCompareRightOperand) {
            isStoreVariableOnRight = true;
        }
        currentStoreVariable = attributeName;
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {

    }
}
