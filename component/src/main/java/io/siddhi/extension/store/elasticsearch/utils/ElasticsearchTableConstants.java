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

package io.siddhi.extension.store.elasticsearch.utils;

/**
 * This class contains the constants values required by the elasticsearch table implementation
 */
public class ElasticsearchTableConstants {

    public static final String ANNOTATION_ELEMENT_HOSTNAME = "hostname";
    public static final String ANNOTATION_ELEMENT_PORT = "port";
    public static final String ANNOTATION_ELEMENT_SCHEME = "scheme";
    public static final String ANNOTATION_ELEMENT_USER = "username";
    public static final String ANNOTATION_ELEMENT_PASSWORD = "password";
    public static final String ANNOTATION_ELEMENT_INDEX_NAME = "index.name";
    public static final String ANNOTATION_ELEMENT_INDEX_NUMBER_OF_SHARDS = "index.number.of.shards";
    public static final String ANNOTATION_ELEMENT_INDEX_NUMBER_OF_REPLICAS = "index.number.of.replicas";
    public static final String ANNOTATION_ELEMENT_INDEX_ALIAS = "index.alias";
    public static final String ANNOTATION_ELEMENT_UPDATE_BATCH_SIZE = "update.batch.size";
    public static final String ANNOTATION_ELEMENT_BULK_ACTIONS = "bulk.actions";
    public static final String ANNOTATION_ELEMENT_BULK_SIZE = "bulk.size";
    public static final String ANNOTATION_ELEMENT_CONCURRENT_REQUESTS = "concurrent.requests";
    public static final String ANNOTATION_ELEMENT_FLUSH_INTERVAL = "flush.interval";
    public static final String ANNOTATION_ELEMENT_BACKOFF_POLICY_RETRY_NO = "backoff.policy.retry.no";
    public static final String ANNOTATION_ELEMENT_BACKOFF_POLICY = "backoff.policy";
    public static final String ANNOTATION_ELEMENT_BACKOFF_POLICY_CONSTANT_BACKOFF = "constantBackoff";
    public static final String ANNOTATION_ELEMENT_BACKOFF_POLICY_EXPONENTIAL_BACKOFF = "exponentialBackoff";
    public static final String ANNOTATION_ELEMENT_BACKOFF_POLICY_DISABLE = "disable";
    public static final String ANNOTATION_ELEMENT_BACKOFF_POLICY_WAIT_TIME = "backoff.policy.wait.time";
    public static final String ANNOTATION_ELEMENT_CLIENT_IO_THREAD_COUNT = "io.thread.count";
    public static final String ANNOTATION_ELEMENT_SSL_ENABLED = "ssl.enabled";
    public static final String ANNOTATION_ELEMENT_TRUSRTSTORE_TYPE = "trust.store.type";
    public static final String ANNOTATION_ELEMENT_TRUSRTSTORE_PATH = "trust.store.path";
    public static final String ANNOTATION_ELEMENT_TRUSRTSTORE_PASS = "trust.store.pass";
    public static final String ANNOTATION_ELEMENT_PAYLOAD_INDEX_OF_INDEX_NAME = "payload.index.of.index.name";
    public static final String ANNOTATION_ELEMENT_MEMBER_LIST = "elasticsearch.member.list";
    public static final String ANNOTATION_TYPE_MAPPINGS = "TypeMappings";

    public static final String DEFAULT_HOSTNAME = "localhost";
    public static final int DEFAULT_PORT = 9200;
    public static final int DEFAULT_NUMBER_OF_SHARDS = 3;
    public static final int DEFAULT_NUMBER_OF_REPLICAS = 2;
    public static final int DEFAULT_UPDATE_BATCH_SIZE = 500;
    public static final String DEFAULT_SCHEME = "http";
    public static final String DEFAULT_USER_NAME = "elastic";
    public static final String DEFAULT_PASSWORD = "changeme";
    public static final int DEFAULT_BULK_ACTIONS = 1;
    public static final long DEFAULT_BULK_SIZE_IN_MB = 1;
    public static final int DEFAULT_CONCURRENT_REQUESTS = 0;
    public static final long DEFAULT_FLUSH_INTERVAL = 10;
    public static final String DEFAULT_BACKOFF_POLICY = null;
    public static final int DEFAULT_BACKOFF_POLICY_RETRY_NO = 3;
    public static final long DEFAULT_BACKOFF_POLICY_WAIT_TIME = 1;
    public static final int DEFAULT_IO_THREAD_COUNT = 1;
    public static final boolean DEFAULT_SSL_ENABLED = false;
    public static final String DEFAULT_TRUSTSTORE_PASS = "wso2carbon";
    public static final String DEFAULT_TRUSTSTORE_TYPE = "jks";
    public static final int DEFAULT_PAYLOAD_INDEX_OF_INDEX_NAME = -1;
    public static final String SETTING_INDEX_NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final String SETTING_INDEX_NUMBER_OF_REPLICAS = "index.number_of_replicas";


    public static final String MAPPING_PROPERTIES_ELEMENT = "properties";
    public static final String MAPPING_TYPE_ELEMENT = "type";

}
