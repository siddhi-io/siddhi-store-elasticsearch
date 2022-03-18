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
package io.siddhi.extension.store.elasticsearch.sink;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.store.elasticsearch.ElasticsearchConfigs;
import io.siddhi.extension.store.elasticsearch.exceptions.ElasticsearchEventSinkException;
import io.siddhi.extension.store.elasticsearch.utils.SiddhiIndexRequest;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.SETTING_INDEX_NUMBER_OF_REPLICAS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.SETTING_INDEX_NUMBER_OF_SHARDS;

/**
 * This class contains the implementation for the indexing elasticsearch documents from the sink.
 */
@Extension(name = "elasticsearch", namespace = "sink",
        description = "" +
                "Elasticsearch sink implementation uses Elasticsearch indexing document for underlying " +
                "data storage. The events that are published from the sink will be converted into elasticsearch index" +
                " documents. The elasticsearch sink is connected to the Elastisearch server via the Elasticsearch " +
                "Java High Level REST Client library. By using this sink, we can customize the json document " +
                "before it's stored in the elasticsearch.",

        parameters = {
                @Parameter(name = "hostname",
                        description = "The hostname of the Elasticsearch server.",
                        type = {DataType.STRING}, optional = true, defaultValue = "localhost"),
                @Parameter(name = "port",
                        description = "The port of the Elasticsearch server.",
                        type = {DataType.INT}, optional = true, defaultValue = "9200"),
                @Parameter(name = "scheme",
                        description = "The scheme type of the Elasticsearch server connection.",
                        type = {DataType.STRING}, optional = true, defaultValue = "http"),
                @Parameter(name = "elasticsearch.member.list",
                        description = "The list of elasticsearch host names. in comma separated manner" +
                                "`https://hostname1:9200,https://hostname2:9200`",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "null"),
                @Parameter(name = "username",
                        description = "The username for the Elasticsearch server connection.",
                        type = {DataType.STRING}, optional = true, defaultValue = "elastic"),
                @Parameter(name = "password",
                        description = "The password for the Elasticsearch server connection.",
                        type = {DataType.STRING}, optional = true, defaultValue = "changeme"),
                @Parameter(name = "index.name",
                        description = "The name of the Elasticsearch index.This must be in lower case",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "The table name defined in the Siddhi App query."),
                @Parameter(name = "payload.index.of.index.name",
                        description = "The payload which is used to create the index. This can be used if the user " +
                                "needs to create index names dynamically. This must be in lower case. If this " +
                                "parameter is configured then respective elasticsearch table can be only used for " +
                                "insert operations because indices are created in the runtime dynamically.",
                        type = {DataType.INT}, optional = true,
                        defaultValue = "-1"),
                @Parameter(name = "index.alias",
                        description = "The alias of the Elasticsearch index.",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "null"),
                @Parameter(name = "index.number.of.shards",
                        description = "The number of shards allocated for the index in the Elasticsearch server.",
                        type = {DataType.INT}, optional = true, defaultValue = "3"),
                @Parameter(name = "index.number.of.replicas",
                        description = "The number of replicas for the index in the Elasticsearch server.",
                        type = {DataType.INT}, optional = true, defaultValue = "2"),
                @Parameter(name = "bulk.actions",
                        description = "The number of actions to be added to flush a new bulk request. " +
                                "Use -1 to disable it",
                        type = {DataType.INT}, optional = true, defaultValue = "1"),
                @Parameter(name = "bulk.size",
                        description = "The size of size of actions currently added to the bulk request to flush a " +
                                "new bulk request in MB. Use -1 to disable it",
                        type = {DataType.LONG}, optional = true, defaultValue = "1"),
                @Parameter(name = "concurrent.requests",
                        description = "The number of concurrent requests allowed to be executed. Use 0 to only allow " +
                                "the execution of a single request",
                        type = {DataType.INT}, optional = true, defaultValue = "0"),
                @Parameter(name = "flush.interval",
                        description = "The flush interval flushing any BulkRequest pending if the interval passes.",
                        type = {DataType.LONG}, optional = true, defaultValue = "10"),
                @Parameter(name = "backoff.policy.retry.no",
                        description = "The number of retries until backoff " +
                                "(The backoff policy defines how the bulk processor should handle retries of bulk " +
                                "requests internally in case they have failed due to resource constraints " +
                                "(i.e. a thread pool was full)).",
                        type = {DataType.INT}, optional = true, defaultValue = "3"),
                @Parameter(name = "backoff.policy.wait.time",
                        description = "The constant back off policy that initially waits until the next retry " +
                                "in seconds.",
                        type = {DataType.LONG}, optional = true, defaultValue = "1"),
                @Parameter(name = "ssl.enabled",
                        description = "SSL is enabled or not.",
                        type = {DataType.BOOL}, optional = true,
                        defaultValue = "null"),
                @Parameter(name = "trust.store.type",
                        description = "Trust store type.",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "jks"),
                @Parameter(name = "trust.store.path",
                        description = "Trust store path.",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "null"),
                @Parameter(name = "trust.store.pass",
                        description = "Trust store password.",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "wso2carbon"),
                @Parameter(name = "backoff.policy",
                        description = "Provides a backoff policy(eg: constantBackoff, exponentialBackoff, disable) " +
                                "for bulk requests, whenever a bulk request is rejected due to resource constraints. " +
                                "Bulk processor will wait before the operation is retried internally.",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "constantBackoff"),
                @Parameter(name = "backoff.policy.retry.no",
                        description = "The maximum number of retries. Must be a non-negative number.",
                        type = {DataType.INT}, optional = true,
                        defaultValue = "3"),
                @Parameter(name = "backoff.policy.wait.time",
                        description = "The delay defines how long to wait between retry attempts. Must not be null.",
                        type = {DataType.INT}, optional = true,
                        defaultValue = "1")
        },
        examples = {
                @Example(syntax = "" +
                        "@sink(type='elasticsearch', hostname='172.0.0.1', port='9200'," +
                        "index.name='stock_index', " +
                        "@map(type='json', @payload(\"\"\"{\n" +
                        "   \"Stock Data\":{\n" +
                        "      \"Symbol\":\"{{symbol}}\",\n" +
                        "      \"Price\":{{price}},\n" +
                        "      \"Volume\":{{volume}}\n" +
                        "   }\n" +
                        "}\"\"\")))" +
                        "define stream stock_stream(symbol string, price float, volume long);",
                        description = "This will create an index called 'stock_index' if it does not already exist " +
                                "in the elasticsearch server and saves the custom json document."
                )
        }
)

public class ElasticsearchSink extends Sink {

    private static final Logger logger = LogManager.getLogger(ElasticsearchSink.class);
    private ElasticsearchConfigs elasticsearchConfigs;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                                ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        elasticsearchConfigs = new ElasticsearchConfigs(this);
        elasticsearchConfigs.init(streamDefinition, sinkConfigReader, siddhiAppContext);
        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        SiddhiIndexRequest siddhiIndexRequest = new SiddhiIndexRequest(payload, dynamicOptions,
                elasticsearchConfigs.getIndexName());
        try {
            JsonElement parse = new JsonParser().parse((String) payload);
            if (parse.isJsonArray()) {
                JsonArray jsonArray = parse.getAsJsonArray();
                for (JsonElement jsonElement : jsonArray) {
                    siddhiIndexRequest.source(jsonElement.toString(), XContentType.JSON);
                    elasticsearchConfigs.getBulkProcessor().add(siddhiIndexRequest);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(payload + " has been successfully added.");
                }
            } else if (parse.isJsonObject()) {
                siddhiIndexRequest.source(parse.getAsJsonObject().toString(), XContentType.JSON);
                elasticsearchConfigs.getBulkProcessor().add(siddhiIndexRequest);
            }
        } catch (JsonSyntaxException e) {
            throw new ElasticsearchEventSinkException("Invalid json document, Please recheck the json mapping at" +
                    "the '@payload'.", e);
        }
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        if (elasticsearchConfigs.getIndexName() != null && !elasticsearchConfigs.getIndexName().isEmpty()) {
            createIndex(elasticsearchConfigs.getDefinitionId());
        }
    }

    @Override
    public void disconnect() {
        try {
            elasticsearchConfigs.getRestHighLevelClient().close();
        } catch (IOException e) {
            throw new ElasticsearchEventSinkException("An error occurred while closing the elasticsearch client.", e);
        } finally {
            elasticsearchConfigs.setRestHighLevelClient(null);
        }
    }

    @Override
    public void destroy() {
    }

    private void createIndex(String definitionId) throws ConnectionUnavailableException { //call at the connect
        try {
            if (elasticsearchConfigs.getRestHighLevelClient().indices().exists(
                    new GetIndexRequest(elasticsearchConfigs.getIndexName()), RequestOptions.DEFAULT)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Index: " + elasticsearchConfigs.getIndexName() + " has already being created for" +
                            " stream id: " + definitionId + ".");
                }
                return;
            }
        } catch (IOException e) {
            throw new ConnectionUnavailableException("Error while checking indices for stream id : '" +
                    definitionId, e);
        }
        CreateIndexRequest request = new CreateIndexRequest(elasticsearchConfigs.getIndexName());
        request.settings(Settings.builder()
                        .put(SETTING_INDEX_NUMBER_OF_SHARDS, elasticsearchConfigs.getNumberOfShards())
                        .put(SETTING_INDEX_NUMBER_OF_REPLICAS, elasticsearchConfigs.getNumberOfReplicas()));
        if (elasticsearchConfigs.getIndexAlias() != null) {
            request.alias(new Alias(elasticsearchConfigs.getIndexAlias()));
        }
        try {
            elasticsearchConfigs.getRestHighLevelClient().indices().create(request, RequestOptions.DEFAULT);
            if (logger.isDebugEnabled()) {
                logger.debug("A stream id: " + definitionId + " is created with the provided information.");
            }
        } catch (IOException e) {
            throw new ElasticsearchEventSinkException("Error while creating indices for stream id : '" +
                    definitionId, e);
        } catch (ElasticsearchStatusException e) {
            logger.error("Elasticsearch status exception occurred while creating index for stream id: " +
                    definitionId, e);
        }
    }
}
