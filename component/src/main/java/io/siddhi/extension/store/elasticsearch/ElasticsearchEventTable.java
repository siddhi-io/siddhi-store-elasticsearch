/*
 * Copyright (c)  2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.table.record.AbstractRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.elasticsearch.exceptions.ElasticsearchEventTableException;
import io.siddhi.extension.store.elasticsearch.exceptions.ElasticsearchServiceException;
import io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableUtils;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.ANNOTATION_TYPE_MAPPINGS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.MAPPING_PROPERTIES_ELEMENT;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.MAPPING_TYPE_ELEMENT;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.SETTING_INDEX_NUMBER_OF_REPLICAS;
import static io.siddhi.extension.store.elasticsearch.utils.ElasticsearchTableConstants.SETTING_INDEX_NUMBER_OF_SHARDS;

/**
 * This class contains the Event table implementation for Elasticsearch indexing document as underlying data storage.
 */
@Extension(
        name = "elasticsearch",
        namespace = "store",
        description = "Elasticsearch store implementation uses Elasticsearch indexing document for underlying " +
                "data storage. The events are converted to Elasticsearch index documents when the events are " +
                "inserted into the elasticsearch store. Elasticsearch indexing documents are converted to events when" +
                " the documents are read from Elasticsearch indexes. The internal store is connected to the " +
                "Elasticsearch server via the Elasticsearch Java High Level REST Client library.",
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
                @Example(
                        syntax = "@Store(type=\"elasticsearch\", hostname=\"localhost\", " +
                                "username=\"elastic\", password=\"changeme\", index.name=\"mystocktable\", " +
                                "field.length=\"symbol:100\", bulk.actions=\"5000\", bulk.size=\"1\", " +
                                "concurrent.requests=\"2\", flush.interval=\"1\", backoff.policy.retry.no=\"3\", " +
                                "backoff.policy.wait.time=\"1\")\n" +
                                "@PrimaryKey(\"symbol\")" +
                                "define table StockTable (symbol string, price float, volume long);",
                        description = "This example creates an index named 'mystocktable' in the Elasticsearch " +
                                "server if it does not already exist (with three attributes named 'symbol', 'price'," +
                                " and 'volume' of the types 'string', 'float' and 'long' respectively). " +
                                "The connection is made as specified by the parameters configured for the '@Store' " +
                                "annotation. The 'symbol' attribute is considered a unique field and an Elasticsearch" +
                                " index document ID is generated for it."
                ),
                @Example(
                        syntax = "@Store(type=\"elasticsearch\", hostname=\"localhost\", " +
                                "username=\"elastic\", password=\"changeme\", index.name=\"mystocktable\", " +
                                "field.length=\"symbol:100\", bulk.actions=\"5000\", bulk.size=\"1\", " +
                                "concurrent.requests=\"2\", flush.interval=\"1\", backoff.policy.retry.no=\"3\", " +
                                "backoff.policy.wait.time=\"1\", ssl.enabled=\"true\", trust.store.type=\"jks\", " +
                                "trust.store.path=\"/User/wso2/wso2sp/resources/security/client-truststore.jks\", " +
                                "trust.store.pass=\"wso2carbon\")\n" +
                                "@PrimaryKey(\"symbol\")" +
                                "define table StockTable (symbol string, price float, volume long);",
                        description = "This example uses SSL to connect to Elasticsearch."
                ),
                @Example(
                        syntax = "@Store(type=\"elasticsearch\", " +
                                "elasticsearch.member.list=\"https://hostname1:9200,https://hostname2:9200\", " +
                                "username=\"elastic\", password=\"changeme\", index.name=\"mystocktable\", " +
                                "field.length=\"symbol:100\", bulk.actions=\"5000\", bulk.size=\"1\", " +
                                "concurrent.requests=\"2\", flush.interval=\"1\", backoff.policy.retry.no=\"3\", " +
                                "backoff.policy.wait.time=\"1\")\n" +
                                "@PrimaryKey(\"symbol\")" +
                                "define table StockTable (symbol string, price float, volume long);",
                        description = "This example defined several elasticsearch members to publish data using " +
                                "elasticsearch.member.list parameter."
                )
        }
)

// for more information refer https://siddhi.io/en/v4.x/docs/query-guide/#event-table-types

public class ElasticsearchEventTable extends AbstractRecordTable {

    private static final Logger logger = LogManager.getLogger(ElasticsearchEventTable.class);
    private ElasticsearchConfigs elasticsearchConfigs;
    private Map<String, String> typeMappings = new HashMap<>();
    private List<String> primaryKeys;

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractRecordTable} configuration reader.
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {

        elasticsearchConfigs = new ElasticsearchConfigs();
        elasticsearchConfigs.init(tableDefinition, configReader, siddhiAppContext);
        List<Annotation> typeMappingsAnnotations = elasticsearchConfigs.getStoreAnnotation().getAnnotations(
                ANNOTATION_TYPE_MAPPINGS);
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        if (primaryKeyAnnotation != null) {
            this.primaryKeys = new ArrayList<>();
            List<Element> primaryKeyElements = primaryKeyAnnotation.getElements();
            primaryKeyElements.forEach(element -> {
                this.primaryKeys.add(element.getValue().trim());
            });
        }
        if (typeMappingsAnnotations.size() > 0) {
            for (Element element : typeMappingsAnnotations.get(0).getElements()) {
                validateTypeMappingAttribute(element.getKey());
                typeMappings.put(element.getKey(), element.getValue());
            }
        }
    }

    /**
     * Add records to the Table
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     */
    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {

        for (Object[] record : records) {
            if (elasticsearchConfigs.getPayloadIndexOfIndexName() != -1 &&
                    (elasticsearchConfigs.getIndexName() == null || !elasticsearchConfigs.getIndexName().equals(
                            record[elasticsearchConfigs.getPayloadIndexOfIndexName()]))) {
                elasticsearchConfigs.setIndexName((String) record[elasticsearchConfigs.getPayloadIndexOfIndexName()]);
                String indexNameInLowerCase = elasticsearchConfigs.getIndexName().toLowerCase(Locale.ROOT);
                if (!elasticsearchConfigs.getIndexName().equals(indexNameInLowerCase)) {
                    logger.warn("Dynamic Index name : " + elasticsearchConfigs.getIndexName() + " must be in lower " +
                            "case in Siddhi application " + siddhiAppContext.getName() + ", hence changing it to " +
                            "lower case. New index name is " + indexNameInLowerCase);
                    elasticsearchConfigs.setIndexName(indexNameInLowerCase);
                }
                createIndex();
            }
            IndexRequest indexRequest = new IndexRequest(elasticsearchConfigs.getIndexName());
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                String docId = ElasticsearchTableUtils.generateRecordIdFromPrimaryKeyValues(elasticsearchConfigs
                        .getAttributes(), record, primaryKeys);
                indexRequest.id(docId);
            }
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                for (int i = 0; i < record.length; i++) {
                    builder.field(elasticsearchConfigs.getAttributes().get(i).getName(), record[i]);
                }
                builder.endObject();
                indexRequest.source(builder);
                elasticsearchConfigs.getBulkProcessor().add(indexRequest);
            } catch (IOException e) {
                throw new ElasticsearchEventTableException("Error while generating content mapping for records : '" +
                        records.toString() + "' in table id: " + tableDefinition.getId(), e);
            }
        }
    }

    /**
     * Find records matching the compiled condition
     *
     * @param findConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                  compiled condition
     * @param compiledCondition         the compiledCondition against which records should be matched
     * @return RecordIterator of matching records
     */
    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        try {
            return findRecords(findConditionParameterMap, compiledCondition);
        } catch (ElasticsearchServiceException e) {
            throw new ConnectionUnavailableException("Error while performing the find operation " + e.getMessage(),
                    e);
        }
    }

    private ElasticsearchRecordIterator findRecords(Map<String, Object> findConditionParameterMap, CompiledCondition
            compiledCondition) throws ElasticsearchServiceException {
        String condition = ElasticsearchTableUtils.resolveCondition((ElasticsearchCompiledCondition) compiledCondition,
                findConditionParameterMap);
        return new ElasticsearchRecordIterator(elasticsearchConfigs.getIndexName(), condition, elasticsearchConfigs
                .getRestHighLevelClient(), elasticsearchConfigs.getAttributes());
    }

    /**
     * .
     * Check if matching record exist or not
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     */
    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        try {
            ElasticsearchRecordIterator elasticsearchRecordIterator = findRecords(containsConditionParameterMap,
                    compiledCondition);
            return elasticsearchRecordIterator.hasNext();
        } catch (ElasticsearchServiceException e) {
            throw new ElasticsearchEventTableException("Error while checking content mapping for '" +
                    "' table id: " + tableDefinition.getId(), e);
        }
    }

    /**
     * Delete all matching records
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     **/
    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        String docId = null;
        try {
            for (Map<String, Object> record : deleteConditionParameterMaps) {
                if (primaryKeys != null && !primaryKeys.isEmpty()) {
                    docId = ElasticsearchTableUtils.generateRecordIdFromPrimaryKeyValues(elasticsearchConfigs
                            .getAttributes(), record, primaryKeys);
                }
                DeleteRequest deleteRequest = new DeleteRequest(elasticsearchConfigs.getIndexName(),
                        docId != null ? docId : "1");
                elasticsearchConfigs.getBulkProcessor().add(deleteRequest);
            }
        } catch (Throwable throwable) {
            throw new ElasticsearchEventTableException("Error while deleting content mapping for records id: '" + docId
                    + "' in table id: " + tableDefinition.getId(), throwable);
        }
    }

    /**
     * Update all matching records
     *
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> list1)
            throws ConnectionUnavailableException {
        String docId = null;
        try {
            for (Map<String, Object> record : list1) {
                if (primaryKeys != null && !primaryKeys.isEmpty()) {
                    docId = ElasticsearchTableUtils.generateRecordIdFromPrimaryKeyValues(elasticsearchConfigs
                            .getAttributes(), record, primaryKeys);
                }
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                for (int i = 0; i < elasticsearchConfigs.getAttributes().size(); i++) {
                    builder.field(elasticsearchConfigs.getAttributes().get(i).getName(), record.get(
                            elasticsearchConfigs.getAttributes().get(i).getName()));
                }
                builder.endObject();
                UpdateRequest updateRequest = new UpdateRequest(elasticsearchConfigs.getIndexName(),
                        docId != null ? docId : "1").doc(builder);
                elasticsearchConfigs.getBulkProcessor().add(updateRequest);
            }
        } catch (Throwable throwable) {
            throw new ElasticsearchEventTableException("Error while updating content mapping for records id: '" + docId
                    + "' in table id: " + tableDefinition.getId(), throwable);
        }
    }

    /**
     * Try updating the records if they exist else add the records
     *
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the values for adding new records if the update condition did not match
     * @param list2             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> list1,
                               List<Object[]> list2) throws ConnectionUnavailableException {
        try {
            for (Object[] record : list2) {
                String docId = null;
                if (primaryKeys != null && !primaryKeys.isEmpty()) {
                    docId = ElasticsearchTableUtils
                            .generateRecordIdFromPrimaryKeyValues(elasticsearchConfigs.getAttributes(), record,
                                    primaryKeys);
                }
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                for (int i = 0; i < elasticsearchConfigs.getAttributes().size(); i++) {
                    builder.field(elasticsearchConfigs.getAttributes().get(i).getName(), record[i]);
                }
                builder.endObject();
                UpdateRequest updateRequest =
                        new UpdateRequest(elasticsearchConfigs.getIndexName(), docId != null ? docId : "1").
                                doc(builder);
                elasticsearchConfigs.getBulkProcessor().add(updateRequest);
            }
        } catch (Throwable throwable) {
            this.add(list2);
        }
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        if (elasticsearchConfigs.getPayloadIndexOfIndexName() != -1) {
            throw new ElasticsearchEventTableException("Elasticsearch table in Siddhi application:" +
                    siddhiAppContext.getName() + " with hostname: " + elasticsearchConfigs.getHostname() + " , port: " +
                    elasticsearchConfigs.getPort() +
                    " and index name: " + elasticsearchConfigs.getIndexName() +
                    " cannot be used for join operations since dynamic indices are " +
                    "created in the runtime.");
        }

        ElasticsearchConditionVisitor visitor = new ElasticsearchConditionVisitor();
        expressionBuilder.build(visitor);
        return new ElasticsearchCompiledCondition(visitor.returnCondition());
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        ElasticsearchExpressionVisitor visitor = new ElasticsearchExpressionVisitor();
        expressionBuilder.build(visitor);
        return new ElasticsearchCompiledCondition(visitor.returnExpression());
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void connect() throws ConnectionUnavailableException {
        if (elasticsearchConfigs.getIndexName() != null && !elasticsearchConfigs.getIndexName().isEmpty()) {
            createIndex();
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect.
     */
    @Override
    protected void disconnect() {

    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    protected void destroy() {

    }

    private void createIndex() {
        try {
            if (elasticsearchConfigs.getRestHighLevelClient().indices()
                    .exists(new GetIndexRequest(elasticsearchConfigs.getIndexName()), RequestOptions.DEFAULT)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Index: " + elasticsearchConfigs.getIndexName() + " has already being created for " +
                            "table id: " + tableDefinition.getId() + ".");
                }
                return;
            }
        } catch (IOException e) {
            throw new ElasticsearchEventTableException("Error while checking indices for table id : '" +
                    tableDefinition.getId(), e);
        }

        CreateIndexRequest request = new CreateIndexRequest(elasticsearchConfigs.getIndexName());
        request.settings(Settings.builder()
                .put(SETTING_INDEX_NUMBER_OF_SHARDS, elasticsearchConfigs.getNumberOfShards())
                .put(SETTING_INDEX_NUMBER_OF_REPLICAS, elasticsearchConfigs.getNumberOfReplicas()));
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject(MAPPING_PROPERTIES_ELEMENT);
                {
                    for (Attribute attribute : elasticsearchConfigs.getAttributes()) {
                        builder.startObject(attribute.getName());
                        {
                            if (typeMappings.containsKey(attribute.getName())) {
                                builder.field(MAPPING_TYPE_ELEMENT, typeMappings.get(attribute.getName()));
                            } else if (attribute.getType().equals(Attribute.Type.STRING)) {
                                builder.field(MAPPING_TYPE_ELEMENT, "text");
                                builder.startObject("fields");
                                {
                                    builder.startObject("keyword");
                                    {
                                        builder.field("type", "keyword");
                                        builder.field("ignore_above", 256);
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            } else if (attribute.getType().equals(Attribute.Type.INT)) {
                                builder.field(MAPPING_TYPE_ELEMENT, "integer");
                            } else if (attribute.getType().equals(Attribute.Type.LONG)) {
                                builder.field(MAPPING_TYPE_ELEMENT, "long");
                            } else if (attribute.getType().equals(Attribute.Type.FLOAT)) {
                                builder.field(MAPPING_TYPE_ELEMENT, "float");
                            } else if (attribute.getType().equals(Attribute.Type.DOUBLE)) {
                                builder.field(MAPPING_TYPE_ELEMENT, "double");
                            } else if (attribute.getType().equals(Attribute.Type.BOOL)) {
                                builder.field(MAPPING_TYPE_ELEMENT, "boolean");
                            } else {
                                builder.field(MAPPING_TYPE_ELEMENT, "object");
                            }
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping(builder);
        } catch (IOException e) {
            throw new ElasticsearchEventTableException("Error while generating mapping for table id : '" +
                    tableDefinition.getId(), e);
        }
        if (elasticsearchConfigs.getIndexAlias() != null) {
            request.alias(new Alias(elasticsearchConfigs.getIndexAlias()));
        }
        try {
            elasticsearchConfigs.getRestHighLevelClient().indices().create(request, RequestOptions.DEFAULT);
            if (logger.isDebugEnabled()) {
                logger.debug("A table id: " + tableDefinition.getId() + " is created with the provided information.");
            }
        } catch (IOException e) {
            throw new ElasticsearchEventTableException("Error while creating indices for table id : '" +
                    tableDefinition.getId(), e);
        } catch (ElasticsearchStatusException e) {
            logger.error("Elasticsearch status exception occurred while creating index for table id: " +
                    tableDefinition.getId(), e);
        }
    }

    private void validateTypeMappingAttribute(String typeMappingAttributeName) {

        boolean matchFound = false;
        for (Attribute storeAttribute : elasticsearchConfigs.getAttributes()) {
            if (storeAttribute.getName().equals(typeMappingAttributeName)) {
                matchFound = true;
            }
        }
        if (!matchFound) {
            throw new SiddhiAppCreationException("Invalid attribute name '" + typeMappingAttributeName
                    + "' found in " + ANNOTATION_TYPE_MAPPINGS + ". No such attribute found in Store definition.");
        }
    }
}
