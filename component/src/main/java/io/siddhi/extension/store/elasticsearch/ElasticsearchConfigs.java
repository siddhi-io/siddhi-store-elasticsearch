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
package io.siddhi.extension.store.elasticsearch;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.elasticsearch.exceptions.ElasticsearchEventTableException;
import io.siddhi.extension.store.elasticsearch.sink.ElasticsearchSink;
import io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableUtils;
import io.siddhi.extension.store.elasticsearch.utils.SiddhiIndexRequest;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Locale;

import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BACKOFF_POLICY;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BACKOFF_POLICY_CONSTANT_BACKOFF;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BACKOFF_POLICY_DISABLE;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BACKOFF_POLICY_EXPONENTIAL_BACKOFF;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BACKOFF_POLICY_RETRY_NO;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BACKOFF_POLICY_WAIT_TIME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BULK_ACTIONS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_BULK_SIZE;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_CLIENT_IO_THREAD_COUNT;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_CONCURRENT_REQUESTS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_FLUSH_INTERVAL;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_HOSTNAME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_INDEX_ALIAS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_INDEX_NAME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_INDEX_NUMBER_OF_REPLICAS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_INDEX_NUMBER_OF_SHARDS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_MEMBER_LIST;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_PASSWORD;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_PAYLOAD_INDEX_OF_INDEX_NAME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_PORT;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_SCHEME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_SSL_ENABLED;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_TRUSRTSTORE_PASS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_TRUSRTSTORE_PATH;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_TRUSRTSTORE_TYPE;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_ELEMENT_USER;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_BACKOFF_POLICY;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_BACKOFF_POLICY_RETRY_NO;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_BACKOFF_POLICY_WAIT_TIME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_BULK_ACTIONS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_BULK_SIZE_IN_MB;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_CONCURRENT_REQUESTS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_FLUSH_INTERVAL;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_HOSTNAME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_IO_THREAD_COUNT;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_NUMBER_OF_REPLICAS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_NUMBER_OF_SHARDS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_PASSWORD;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_PAYLOAD_INDEX_OF_INDEX_NAME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_PORT;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_SCHEME;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_SSL_ENABLED;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_TRUSTSTORE_PASS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_TRUSTSTORE_TYPE;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.DEFAULT_USER_NAME;

/**
 * Configuration for the elasticsearch sink and the store.
 */
public class ElasticsearchConfigs {

    private static final Logger logger = LogManager.getLogger(ElasticsearchConfigs.class);
    private static final long serialVersionUID = 6106269076155338045L;
    private RestHighLevelClient restHighLevelClient;
    private List<Attribute> attributes;
    private String hostname = DEFAULT_HOSTNAME;
    private String indexName;
    private String indexAlias;
    private int port = DEFAULT_PORT;
    private String scheme = DEFAULT_SCHEME;
    private String userName = DEFAULT_USER_NAME;
    private String password = DEFAULT_PASSWORD;
    private int numberOfShards = DEFAULT_NUMBER_OF_SHARDS;
    private int numberOfReplicas = DEFAULT_NUMBER_OF_REPLICAS;
    private BulkProcessor bulkProcessor;
    private int bulkActions = DEFAULT_BULK_ACTIONS;
    private long bulkSize = DEFAULT_BULK_SIZE_IN_MB;
    private int concurrentRequests = DEFAULT_CONCURRENT_REQUESTS;
    private long flushInterval = DEFAULT_FLUSH_INTERVAL;
    private String backoffPolicy = DEFAULT_BACKOFF_POLICY;
    private int backoffPolicyRetryNo = DEFAULT_BACKOFF_POLICY_RETRY_NO;
    private long backoffPolicyWaitTime = DEFAULT_BACKOFF_POLICY_WAIT_TIME;
    private int ioThreadCount = DEFAULT_IO_THREAD_COUNT;
    private String trustStorePass = DEFAULT_TRUSTSTORE_PASS;
    private String trustStorePath;
    private String trustStoreType = DEFAULT_TRUSTSTORE_TYPE;
    private boolean sslEnabled = DEFAULT_SSL_ENABLED;
    private int payloadIndexOfIndexName = DEFAULT_PAYLOAD_INDEX_OF_INDEX_NAME;
    private String listOfHostnames;
    private String definitionId;
    private Annotation storeAnnotation;
    private ElasticsearchSink elasticsearchSink;

    public ElasticsearchConfigs() { // calls from ElasticSearchEventTable

    }

    public ElasticsearchConfigs(ElasticsearchSink elasticsearchSink) { // calls from ElasticSearchSink
        this.elasticsearchSink = elasticsearchSink;
    }

    public void init(AbstractDefinition definition, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        this.definitionId = definition.getId();
        this.attributes = definition.getAttributeList();
        String type;
        BulkProcessorListener bulkProcessorListener;
        if (elasticsearchSink == null) { // to check whether a sink or table
            storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STORE,
                    definition.getAnnotations());
            type = "table";
            bulkProcessorListener = new BulkProcessorListener(definitionId);
        } else {
            storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_SINK,
                    definition.getAnnotations());
            type = "sink";
            bulkProcessorListener = new BulkProcessorListener(elasticsearchSink, definitionId);
        }
        if (storeAnnotation != null) {
            indexName = storeAnnotation.getElement(ANNOTATION_ELEMENT_INDEX_NAME);
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(
                    ANNOTATION_ELEMENT_PAYLOAD_INDEX_OF_INDEX_NAME))) {
                payloadIndexOfIndexName = Integer.parseInt(
                        storeAnnotation.getElement(ANNOTATION_ELEMENT_PAYLOAD_INDEX_OF_INDEX_NAME));
            } else {
                payloadIndexOfIndexName = Integer.parseInt(
                        configReader.readConfig(ANNOTATION_ELEMENT_PAYLOAD_INDEX_OF_INDEX_NAME,
                                String.valueOf(payloadIndexOfIndexName)));
            }
            indexName = ElasticsearchTableUtils.isEmpty(indexName) &&
                    payloadIndexOfIndexName == -1 ? definition.getId() : indexName;

            String indexNameInLowerCase = indexName.toLowerCase(Locale.ROOT);
            if (!indexName.equals(indexNameInLowerCase)) {
                logger.warn("Index name : " + indexName.replaceAll("[\r\n]", "") +
                        " must be in lower case in Siddhi application " +
                        siddhiAppContext.getName().replaceAll("[\r\n]", "") +
                        ", hence changing it to lower case. New index name is " +
                        indexNameInLowerCase.replaceAll("[\r\n]", ""));
                indexName = indexNameInLowerCase;
            }

            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_HOSTNAME))) {
                hostname = storeAnnotation.getElement(ANNOTATION_ELEMENT_HOSTNAME);
            } else {
                hostname = configReader.readConfig(ANNOTATION_ELEMENT_HOSTNAME, hostname);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_PORT))) {
                port = Integer.parseInt(storeAnnotation.getElement(ANNOTATION_ELEMENT_PORT));
            } else {
                port = Integer.parseInt(configReader.readConfig(ANNOTATION_ELEMENT_HOSTNAME, String.valueOf(port)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.
                    getElement(ANNOTATION_ELEMENT_INDEX_NUMBER_OF_SHARDS))) {
                numberOfShards = Integer.parseInt(storeAnnotation.getElement(
                        ANNOTATION_ELEMENT_INDEX_NUMBER_OF_SHARDS));
            } else {
                numberOfShards = Integer.parseInt(configReader.readConfig(ANNOTATION_ELEMENT_INDEX_NUMBER_OF_SHARDS,
                        String.valueOf(numberOfShards)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.
                    getElement(ANNOTATION_ELEMENT_INDEX_NUMBER_OF_REPLICAS))) {
                numberOfReplicas = Integer.parseInt(storeAnnotation.getElement(
                        ANNOTATION_ELEMENT_INDEX_NUMBER_OF_REPLICAS));
            } else {
                numberOfReplicas = Integer.parseInt(configReader.readConfig(ANNOTATION_ELEMENT_INDEX_NUMBER_OF_REPLICAS,
                        String.valueOf(numberOfReplicas)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_SCHEME))) {
                scheme = storeAnnotation.getElement(ANNOTATION_ELEMENT_SCHEME);
            } else {
                scheme = configReader.readConfig(ANNOTATION_ELEMENT_SCHEME, scheme);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_USER))) {
                userName = storeAnnotation.getElement(ANNOTATION_ELEMENT_USER);
            } else {
                userName = configReader.readConfig(ANNOTATION_ELEMENT_USER, userName);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_PASSWORD))) {
                password = storeAnnotation.getElement(ANNOTATION_ELEMENT_PASSWORD);
            } else {
                password = configReader.readConfig(ANNOTATION_ELEMENT_PASSWORD, password);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_INDEX_ALIAS))) {
                indexAlias = storeAnnotation.getElement(ANNOTATION_ELEMENT_INDEX_ALIAS);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_BULK_ACTIONS))) {
                bulkActions = Integer.parseInt(storeAnnotation.getElement(ANNOTATION_ELEMENT_BULK_ACTIONS));
            } else {
                bulkActions = Integer.parseInt(configReader.readConfig(ANNOTATION_ELEMENT_BULK_ACTIONS,
                        String.valueOf(bulkActions)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_BULK_SIZE))) {
                bulkSize = Long.parseLong(storeAnnotation.getElement(ANNOTATION_ELEMENT_BULK_SIZE));
            } else {
                bulkSize = Long.parseLong(configReader.readConfig(ANNOTATION_ELEMENT_BULK_SIZE,
                        String.valueOf(bulkSize)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_CONCURRENT_REQUESTS))) {
                concurrentRequests = Integer.parseInt(
                        storeAnnotation.getElement(ANNOTATION_ELEMENT_CONCURRENT_REQUESTS));
            } else {
                concurrentRequests = Integer.parseInt(configReader.readConfig(ANNOTATION_ELEMENT_CONCURRENT_REQUESTS,
                        String.valueOf(concurrentRequests)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_FLUSH_INTERVAL))) {
                flushInterval = Long.parseLong(
                        storeAnnotation.getElement(ANNOTATION_ELEMENT_FLUSH_INTERVAL));
            } else {
                flushInterval = Long.parseLong(configReader.readConfig(ANNOTATION_ELEMENT_FLUSH_INTERVAL,
                        String.valueOf(flushInterval)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(
                    ANNOTATION_ELEMENT_BACKOFF_POLICY))) {
                backoffPolicy = storeAnnotation.getElement(ANNOTATION_ELEMENT_BACKOFF_POLICY);
                backoffPolicy = backoffPolicy.equalsIgnoreCase(ANNOTATION_ELEMENT_BACKOFF_POLICY_CONSTANT_BACKOFF) ||
                        backoffPolicy.equalsIgnoreCase(ANNOTATION_ELEMENT_BACKOFF_POLICY_EXPONENTIAL_BACKOFF) ||
                        !backoffPolicy.equalsIgnoreCase(ANNOTATION_ELEMENT_BACKOFF_POLICY_DISABLE) ?
                        backoffPolicy : null;
            } else {
                backoffPolicy = configReader.readConfig(ANNOTATION_ELEMENT_BACKOFF_POLICY, backoffPolicy);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(
                    ANNOTATION_ELEMENT_BACKOFF_POLICY_RETRY_NO))) {
                backoffPolicyRetryNo = Integer.parseInt(
                        storeAnnotation.getElement(ANNOTATION_ELEMENT_BACKOFF_POLICY_RETRY_NO));
            } else {
                backoffPolicyRetryNo = Integer.parseInt(
                        configReader.readConfig(ANNOTATION_ELEMENT_BACKOFF_POLICY_RETRY_NO,
                                String.valueOf(backoffPolicyRetryNo)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(
                    ANNOTATION_ELEMENT_BACKOFF_POLICY_WAIT_TIME))) {
                backoffPolicyWaitTime = Long.parseLong(
                        storeAnnotation.getElement(ANNOTATION_ELEMENT_BACKOFF_POLICY_WAIT_TIME));
            } else {
                backoffPolicyWaitTime = Long.parseLong(
                        configReader.readConfig(ANNOTATION_ELEMENT_BACKOFF_POLICY_WAIT_TIME,
                                String.valueOf(backoffPolicyWaitTime)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(
                    ANNOTATION_ELEMENT_CLIENT_IO_THREAD_COUNT))) {
                ioThreadCount = Integer.parseInt(
                        storeAnnotation.getElement(ANNOTATION_ELEMENT_CLIENT_IO_THREAD_COUNT));
            } else {
                ioThreadCount = Integer.parseInt(
                        configReader.readConfig(ANNOTATION_ELEMENT_CLIENT_IO_THREAD_COUNT,
                                String.valueOf(ioThreadCount)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_SSL_ENABLED))) {
                sslEnabled = Boolean.parseBoolean(storeAnnotation.getElement(ANNOTATION_ELEMENT_SSL_ENABLED));
            } else {
                sslEnabled = Boolean.parseBoolean(configReader.readConfig(ANNOTATION_ELEMENT_SSL_ENABLED,
                        String.valueOf(sslEnabled)));
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_TRUSRTSTORE_PASS))) {
                trustStorePass = storeAnnotation.getElement(ANNOTATION_ELEMENT_TRUSRTSTORE_PASS);
            } else {
                trustStorePass = configReader.readConfig(ANNOTATION_ELEMENT_TRUSRTSTORE_PASS, trustStorePass);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_TRUSRTSTORE_PATH))) {
                trustStorePath = storeAnnotation.getElement(ANNOTATION_ELEMENT_TRUSRTSTORE_PATH);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_TRUSRTSTORE_TYPE))) {
                trustStoreType = storeAnnotation.getElement(ANNOTATION_ELEMENT_TRUSRTSTORE_TYPE);
            } else {
                trustStoreType = configReader.readConfig(ANNOTATION_ELEMENT_TRUSRTSTORE_TYPE, trustStoreType);
            }
            if (!ElasticsearchTableUtils.isEmpty(storeAnnotation.getElement(ANNOTATION_ELEMENT_MEMBER_LIST))) {
                listOfHostnames = storeAnnotation.getElement(ANNOTATION_ELEMENT_MEMBER_LIST);
            }
        } else {
            if (elasticsearchSink == null) {
                throw new SiddhiAppCreationException("Elasticsearch Store annotation list is not provided for " + type +
                        " id : '" + definition.getId() + "', required properties cannot be resolved without required " +
                        "parameters.");
            } else {
                throw new SiddhiAppCreationException("Elasticsearch Sink annotation list is not provided for " + type +
                        " id : '" + definition.getId() + "', required properties cannot be resolved without required " +
                        "parameters.");
            }
        }
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));
        HttpHost[] httpHostList;
        if (listOfHostnames != null) {
            String[] hostNameList = listOfHostnames.split(",");
            httpHostList = new HttpHost[hostNameList.length];
            for (int i = 0; i < httpHostList.length; i++) {
                try {
                    URL domain = new URL(hostNameList[i]);
                    httpHostList[i] = new HttpHost(domain.getHost(), domain.getPort(), domain.getProtocol());
                } catch (MalformedURLException e) {
                    throw new SiddhiAppCreationException("Provided elasticsearch hostname url(" + hostNameList[i] +
                            ") is malformed of " + type + " id : '" + definition.getId() + ".", e);
                }
            }
        } else {
            httpHostList = new HttpHost[1];
            httpHostList[0] = new HttpHost(hostname, port, scheme);
        }
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHostList).
                setHttpClientConfigCallback(httpClientBuilder -> {
                    httpClientBuilder.disableAuthCaching();
                    httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().
                            setIoThreadCount(ioThreadCount).build());
                    if (sslEnabled) {
                        try {
                            KeyStore trustStore = KeyStore.getInstance(trustStoreType);
                            if (trustStorePath == null) {
                                throw new ElasticsearchEventTableException("Please provide a valid path for trust " +
                                        "store location for " + type + " id : '" + definition.getId());
                            } else {
                                try (InputStream is = Files.newInputStream(Paths.get(trustStorePath))) {
                                    trustStore.load(is, trustStorePass.toCharArray());
                                }
                                SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
                                httpClientBuilder.setSSLContext(sslBuilder.build());
                            }
                        } catch (NoSuchAlgorithmException e) {
                            throw new ElasticsearchEventTableException("Algorithm used to check the integrity of the " +
                                    "trustStore cannot be found for when loading trustStore for " + type + " id : '" +
                                    definition.getId(), e);
                        } catch (KeyStoreException e) {
                            throw new ElasticsearchEventTableException("The trustStore type truststore.type = " +
                                    "" + trustStoreType + " defined is incorrect while creating " + type + " id : '" +
                                    definition.getId(), e);
                        } catch (CertificateException e) {
                            throw new ElasticsearchEventTableException("Any of the certificates in the keystore " +
                                    "could not be loaded when loading trustStore for table id : '" +
                                    definition.getId(), e);
                        } catch (IOException e) {
                            throw new ElasticsearchEventTableException("The trustStore password = " + trustStorePass +
                                    " or trustStore path " + trustStorePath + " defined is incorrect while creating " +
                                    "sslContext for table id : '" + definition.getId(), e);
                        } catch (KeyManagementException e) {
                            throw new ElasticsearchEventTableException("Error occurred while builing sslContext for " +
                                    "table id : '" + definition.getId(), e);
                        }
                    }
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }));
        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(
                (request, bulkListener) ->
                        restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                bulkProcessorListener);
        bulkProcessorBuilder.setBulkActions(bulkActions);
        bulkProcessorBuilder.setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB));
        bulkProcessorBuilder.setConcurrentRequests(concurrentRequests);
        bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueSeconds(flushInterval));
        if (backoffPolicy == null) {
            bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.noBackoff());
        } else if (backoffPolicy.equalsIgnoreCase(ANNOTATION_ELEMENT_BACKOFF_POLICY_CONSTANT_BACKOFF)) {
            bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.constantBackoff(
                    TimeValue.timeValueSeconds(backoffPolicyWaitTime), backoffPolicyRetryNo));
        } else {
            bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                    TimeValue.timeValueSeconds(backoffPolicyWaitTime), backoffPolicyRetryNo));
        }
        bulkProcessor = bulkProcessorBuilder.build();
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }

    public void setRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public String getHostname() {
        return hostname;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexAlias() {
        return indexAlias;
    }

    public int getPort() {
        return port;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public BulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    public int getPayloadIndexOfIndexName() {
        return payloadIndexOfIndexName;
    }

    public String getDefinitionId() {
        return definitionId;
    }

    public Annotation getStoreAnnotation() {
        return storeAnnotation;
    }

    static class BulkProcessorListener implements BulkProcessor.Listener {

        private ElasticsearchSink elasticsearchSink;
        private String elasticSearchID;

        private BulkProcessorListener(ElasticsearchSink elasticsearchSink, String elasticSearchID) {
            this.elasticsearchSink = elasticsearchSink;
            this.elasticSearchID = elasticSearchID;
        }

        private BulkProcessorListener(String elasticSearchID) {
            this.elasticSearchID = elasticSearchID;
        }

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            int numberOfActions = request.numberOfActions();
            if (logger.isDebugEnabled()) {
                logger.debug("Executing bulk [{" + executionId + "}] with {" + numberOfActions + "} requests");
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                logger.warn("Bulk [{}] executed with failures for executionId: " + executionId + ", failure : "
                        + response.buildFailureMessage() + ", status : " + response.status().getStatus());
                if (logger.isDebugEnabled()) {
                    for (BulkItemResponse itemResponse : response) {
                        if (itemResponse.isFailed()) {
                            logger.warn("Bulk [{}] executed with failures for executionId: " + executionId
                                    + ", item : " + itemResponse.getItemId() + ", response message: "
                                    + itemResponse.getFailureMessage() + ", failure : " + itemResponse.getFailure()
                                    + " message : " + itemResponse.getResponse().toString());
                        }
                    }
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Bulk [{" + executionId + "}] completed in {" + response.getTook().getMillis() +
                            "} milliseconds");
                    for (BulkItemResponse itemResponse : response) {
                        logger.trace("Bulk [{" + executionId + "}] completed for : " +
                                itemResponse.getResponse().toString());
                    }
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            if (request.requests().size() > 0) {
                if (failure instanceof Exception && request.requests().get(0) instanceof SiddhiIndexRequest) {
                    for (DocWriteRequest docWriteRequest : request.requests()) {
                        SiddhiIndexRequest siddhiIndexRequest = (SiddhiIndexRequest) docWriteRequest;
                        elasticsearchSink.onError(siddhiIndexRequest.getPayload(), siddhiIndexRequest
                                .getDynamicOptions(), (Exception) failure);
                    }
                }
            }
            if (elasticsearchSink == null) {
                logger.error("Failed to execute bulk request at Elasticsearch table :" + elasticSearchID
                        , failure);
            } else {
                logger.error("Failed to execute bulk request at Elasticsearch sink :" + elasticSearchID
                        , failure);
            }
        }
    }

}
