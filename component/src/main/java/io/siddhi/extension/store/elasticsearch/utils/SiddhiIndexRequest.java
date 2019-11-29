/*
 * Copyright (c)  2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.store.elasticsearch.utils;

import io.siddhi.core.util.transport.DynamicOptions;
import org.elasticsearch.action.index.IndexRequest;

/**
 * Customize IndexRequest class to send index requests with payload and dynamic options.
 */
public class SiddhiIndexRequest extends IndexRequest {

    private Object payload;
    private DynamicOptions dynamicOptions;

    public SiddhiIndexRequest(Object payload, DynamicOptions dynamicOptions, String index) {
        super(index);
        this.payload = payload;
        this.dynamicOptions = dynamicOptions;
    }

    public Object getPayload() {
        return payload;
    }

    public DynamicOptions getDynamicOptions() {
        return dynamicOptions;
    }

}
