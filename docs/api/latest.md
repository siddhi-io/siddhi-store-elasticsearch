# API Docs - v2.0.0-SNAPSHOT

## Store

### elasticsearch *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*

<p style="word-wrap: break-word">Elasticsearch store implementation uses Elasticsearch indexing document for underlying data storage. The events are converted to Elasticsearch index documents when the events are inserted into the elasticsearch store. Elasticsearch indexing documents are converted to events when the documents are read from Elasticsearch indexes. The internal store is connected to the Elastisearch server via the Elasticsearch Java High Level REST Client library.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@Store(type="elasticsearch", hostname="<STRING>", port="<INT>", scheme="<STRING>", elasticsearch.member.list="<STRING>", username="<STRING>", password="<STRING>", index.name="<STRING>", index.type="<STRING>", payload.index.of.index.name="<INT>", index.alias="<STRING>", index.number.of.shards="<INT>", index.number.of.replicas="<INT>", bulk.actions="<INT>", bulk.size="<LONG>", concurrent.requests="<INT>", flush.interval="<LONG>", backoff.policy.retry.no="<INT>", backoff.policy.wait.time="<LONG>", ssl.enabled="<BOOL>", trust.store.type="<STRING>", trust.store.path="<STRING>", trust.store.pass="<STRING>")
@PrimaryKey("PRIMARY_KEY")
@Index("INDEX")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">hostname</td>
        <td style="vertical-align: top; word-wrap: break-word">The hostname of the Elasticsearch server.</td>
        <td style="vertical-align: top">localhost</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">port</td>
        <td style="vertical-align: top; word-wrap: break-word">The port of the Elasticsearch server.</td>
        <td style="vertical-align: top">9200</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">scheme</td>
        <td style="vertical-align: top; word-wrap: break-word">The scheme type of the Elasticsearch server connection.</td>
        <td style="vertical-align: top">http</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">elasticsearch.member.list</td>
        <td style="vertical-align: top; word-wrap: break-word">The list of elasticsearch host names. in comma separated mannerhttps://hostname1:9200,https://hostname2:9200</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">The username for the Elasticsearch server connection.</td>
        <td style="vertical-align: top">elastic</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word">The password for the Elasticsearch server connection.</td>
        <td style="vertical-align: top">changeme</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">index.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name of the Elasticsearch index.</td>
        <td style="vertical-align: top">The table name defined in the Siddhi App query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">index.type</td>
        <td style="vertical-align: top; word-wrap: break-word">The the type of the index.</td>
        <td style="vertical-align: top">_doc</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">payload.index.of.index.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The payload which is used to create the index. This can be used if the user needs to create index names dynamically</td>
        <td style="vertical-align: top">-1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">index.alias</td>
        <td style="vertical-align: top; word-wrap: break-word">The alias of the Elasticsearch index.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">index.number.of.shards</td>
        <td style="vertical-align: top; word-wrap: break-word">The number of shards allocated for the index in the Elasticsearch server.</td>
        <td style="vertical-align: top">3</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">index.number.of.replicas</td>
        <td style="vertical-align: top; word-wrap: break-word">The number of replicas for the index in the Elasticsearch server.</td>
        <td style="vertical-align: top">2</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bulk.actions</td>
        <td style="vertical-align: top; word-wrap: break-word">The number of actions to be added to flush a new bulk request. Use -1 to disable it</td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bulk.size</td>
        <td style="vertical-align: top; word-wrap: break-word">The size of size of actions currently added to the bulk request to flush a new bulk request in MB. Use -1 to disable it</td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">concurrent.requests</td>
        <td style="vertical-align: top; word-wrap: break-word">The number of concurrent requests allowed to be executed. Use 0 to only allow the execution of a single request</td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">flush.interval</td>
        <td style="vertical-align: top; word-wrap: break-word">The flush interval flushing any BulkRequest pending if the interval passes.</td>
        <td style="vertical-align: top">10</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">backoff.policy.retry.no</td>
        <td style="vertical-align: top; word-wrap: break-word">The number of retries until backoff (The backoff policy defines how the bulk processor should handle retries of bulk requests internally in case they have failed due to resource constraints (i.e. a thread pool was full)).</td>
        <td style="vertical-align: top">3</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">backoff.policy.wait.time</td>
        <td style="vertical-align: top; word-wrap: break-word">The constant back off policy that initially waits until the next retry in seconds.</td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">ssl.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word">SSL is enabled or not.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">trust.store.type</td>
        <td style="vertical-align: top; word-wrap: break-word">Trust store type.</td>
        <td style="vertical-align: top">jks</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">trust.store.path</td>
        <td style="vertical-align: top; word-wrap: break-word">Trust store path.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">trust.store.pass</td>
        <td style="vertical-align: top; word-wrap: break-word">Trust store password.</td>
        <td style="vertical-align: top">wso2carbon</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@Store(type="elasticsearch", host="localhost", username="elastic", password="changeme", index.name="MyStockTable", field.length="symbol:100", bulk.actions="5000", bulk.size="1", concurrent.requests="2", flush.interval="1", backoff.policy.retry.no="3", backoff.policy.wait.time="1")
@PrimaryKey("symbol")define table StockTable (symbol string, price float, volume long);
```
<p style="word-wrap: break-word">This example creates an index named 'MyStockTable' in the Elasticsearch server if it does not already exist (with three attributes named 'symbol', 'price', and 'volume' of the types 'string', 'float' and 'long' respectively). The connection is made as specified by the parameters configured for the '@Store' annotation. The 'symbol' attribute is considered a unique field and an Elasticsearch index document ID is generated for it.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@Store(type="elasticsearch", host="localhost", username="elastic", password="changeme", index.name="MyStockTable", field.length="symbol:100", bulk.actions="5000", bulk.size="1", concurrent.requests="2", flush.interval="1", backoff.policy.retry.no="3", backoff.policy.wait.time="1", ssl.enabled="true", trust.store.type="jks", trust.store.path="/User/wso2/wso2sp/resources/security/client-truststore.jks", trust.store.pass="wso2carbon")
@PrimaryKey("symbol")define table StockTable (symbol string, price float, volume long);
```
<p style="word-wrap: break-word">This example uses SSL to connect to Elasticsearch.</p>

<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@Store(type="elasticsearch", elasticsearch.member.list="https://hostname1:9200,https://hostname2:9200", username="elastic", password="changeme", index.name="MyStockTable", field.length="symbol:100", bulk.actions="5000", bulk.size="1", concurrent.requests="2", flush.interval="1", backoff.policy.retry.no="3", backoff.policy.wait.time="1")
@PrimaryKey("symbol")define table StockTable (symbol string, price float, volume long);
```
<p style="word-wrap: break-word">This example defined several elasticsearch members to publish data using elasticsearch.member.list parameter.</p>

