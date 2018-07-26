/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.store.elasticsearch;

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

/**
 * This class represents the compiled condition specific to Elasticsearch record tables.
 */
public class ElasticsearchCompiledCondition implements CompiledCondition {
    private String compiledQuery;

    public ElasticsearchCompiledCondition(String compiledQuery) {
        this.compiledQuery = compiledQuery;
    }

    @Override
    public CompiledCondition cloneCompilation(String key) {
        return null;
    }

    public String getCompiledQuery() {
        return compiledQuery;
    }

    public String toString() {
        return getCompiledQuery();
    }
}
