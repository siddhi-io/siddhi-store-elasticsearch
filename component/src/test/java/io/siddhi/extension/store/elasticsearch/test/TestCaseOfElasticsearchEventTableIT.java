/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.store.elasticsearch.test;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.store.elasticsearch.test.utils.ElasticsearchUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test cases for Elasticsearch store.
 */
public class TestCaseOfElasticsearchEventTableIT {
    private static final Logger log = LogManager.getLogger(TestCaseOfElasticsearchEventTableIT.class);
    private static String hostname;
    private static String port;
    private static int inEventCount;
    private static int removeEventCount;
    private boolean eventArrived;


    @BeforeClass
    public static void startTest() {
        log.info("== Elasticsearch Table tests completed ==");
        hostname = ElasticsearchUtils.getIpAddressOfContainer();
        port = ElasticsearchUtils.getContainerPort();
        inEventCount = 0;
        removeEventCount = 0;
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Elasticsearch Table tests completed ==");
    }

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
    }

    @Test(testName = "elasticsearchTableDefinitionTest", description = "Testing table creation.", enabled = true)
    public void elasticsearchTableDefinitionTest() throws InterruptedException {
        log.info("elasticsearchTableDefinitionTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams =
                "define stream StockStream (symbol string, price float, volume long); \n" +
                        "\n" +
                        "@store(type='elasticsearch', hostname='" + hostname + "', port='" + port + "', " +
                        "index.name='stock_index')\n" +
                        "@primaryKey('symbol') \n" +
                        "define table stock_table(symbol string, price float, volume long);";

        String query = "from StockStream \n" +
                "select symbol, price, volume \n" +
                "insert into stock_table;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "elasticsearchRecordsInsertion", description = "Testing Records insertion.", enabled = true)
    public void elasticsearchRecordsInsertion() throws InterruptedException {
        log.info("elasticsearchRecordsInsertion");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams =
                "define stream StockStream (symbol string, price float, volume long); \n" +
                        "define stream TestStream(symbol string); \n" +
                        "\n" +
                        "@store(type='elasticsearch', hostname='" + hostname + "', port='" + port + "', " +
                        "index.name='stock_index')\n" +
                        "@primaryKey('symbol') \n" +
                        "define table stock_table(symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream \n" +
                "select symbol, price, volume \n" +
                "insert into stock_table;";
        String query2 = "" +
                "@info(name = 'query2')\n" +
                "from TestStream as a join stock_table as b on a.symbol == b.symbol\n" +
                "select a.symbol, price, volume\n" +
                "insert into AlertStream;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query + query2);
        InputHandler insertStockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler insertTestStream = siddhiAppRuntime.getInputHandler("TestStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 1005L});
                                break;
                            default:
                                Assert.assertEquals(1, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });
        siddhiAppRuntime.start();

        insertStockStream.send(new Object[]{"WSO2", 55.6F, 1005L});
        insertStockStream.send(new Object[]{"IBM", 75.6F, 1005L});
        insertStockStream.send(new Object[]{"MSFT", 57.6F, 1005L});
        Thread.sleep(1000);
        insertTestStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);
        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }


    @Test(testName = "elasticsearchRecordsUpdate", description = "Testing Records update.", enabled = true)
    public void elasticsearchRecordsUpdate() throws InterruptedException {
        log.info("elasticsearchRecordsUpdate");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams =
                "define stream StockStream (symbol string, price float, volume long); \n" +
                        "define stream UpdateStream (symbol string, price float, volume long); \n" +
                        "define stream TestStream(symbol string); \n" +
                        "\n" +
                        "@store(type='elasticsearch', hostname='" + hostname + "', port='" + port + "', " +
                        "index.name='stock_index')\n" +
                        "@primaryKey('symbol') \n" +
                        "define table stock_table(symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream \n" +
                "select symbol, price, volume \n" +
                "insert into stock_table;";
        String query2 = "" +
                "@info(name = 'query2')\n" +
                "from UpdateStream " +
                "select symbol, price, volume\n" +
                "update stock_table on stock_table.symbol == symbol;";
        String query3 = "" +
                "@info(name = 'query3')\n" +
                "from TestStream as a join stock_table as b on a.symbol == b.symbol\n" +
                "select a.symbol, price, volume\n" +
                "insert into AlertStream;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query + query2 + query3);
        InputHandler insertStockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler insertUpdateStream = siddhiAppRuntime.getInputHandler("UpdateStream");
        InputHandler insertTestStream = siddhiAppRuntime.getInputHandler("TestStream");
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 100.6F, 2005L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 100.6F, 2005L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 100.6F, 2005L});
                                break;
                            default:
                                Assert.assertEquals(3, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });
        siddhiAppRuntime.start();

        insertStockStream.send(new Object[]{"WSO2", 55.6F, 1005L});
        insertStockStream.send(new Object[]{"IBM", 75.6F, 1005L});
        insertStockStream.send(new Object[]{"MSFT", 57.6F, 1005L});
        Thread.sleep(1000);
        insertUpdateStream.send(new Object[]{"WSO2", 100.6F, 2005L});
        insertUpdateStream.send(new Object[]{"IBM", 100.6F, 2005L});
        insertUpdateStream.send(new Object[]{"MSFT", 100.6F, 2005L});
        Thread.sleep(1000);
        insertTestStream.send(new Object[]{"WSO2"});
        insertTestStream.send(new Object[]{"IBM"});
        insertTestStream.send(new Object[]{"MSFT"});
        Thread.sleep(1000);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "elasticsearchRecordsUpdateOrInsert", description = "Testing Records update or insert.",
            enabled = true)
    public void elasticsearchRecordsUpdateOrInsert() throws InterruptedException {
        log.info("elasticsearchRecordsUpdateOrInsert");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams =
                "define stream StockStream (symbol string, price float, volume long); \n" +
                        "define stream UpdateStream (symbol string, price float, volume long); \n" +
                        "define stream TestStream(symbol string); \n" +
                        "\n" +
                        "@store(type='elasticsearch', hostname='" + hostname + "', port='" + port + "', " +
                        "index.name='stock_index')\n" +
                        "@primaryKey('symbol') \n" +
                        "define table stock_table(symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream \n" +
                "select symbol, price, volume \n" +
                "insert into stock_table;";
        String query2 = "" +
                "@info(name = 'query2')\n" +
                "from UpdateStream " +
                "select symbol, price, volume\n" +
                "update or insert into stock_table on stock_table.symbol == symbol;";
        String query3 = "" +
                "@info(name = 'query3')\n" +
                "from TestStream as a join stock_table as b on a.symbol == b.symbol\n" +
                "select a.symbol, price, volume\n" +
                "insert into AlertStream;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query + query2 + query3);
        InputHandler insertStockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler insertUpdateStream = siddhiAppRuntime.getInputHandler("UpdateStream");
        InputHandler insertTestStream = siddhiAppRuntime.getInputHandler("TestStream");
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 100.6F, 2005L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 100.6F, 2005L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 100.6F, 2005L});
                                break;
                            default:
                                Assert.assertEquals(3, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });
        siddhiAppRuntime.start();

        insertStockStream.send(new Object[]{"WSO2", 55.6F, 1005L});
        insertStockStream.send(new Object[]{"IBM", 75.6F, 1005L});
        insertStockStream.send(new Object[]{"MSFT", 57.6F, 1005L});
        Thread.sleep(1000);
        insertUpdateStream.send(new Object[]{"WSO2", 100.6F, 2005L});
        insertUpdateStream.send(new Object[]{"IBM", 100.6F, 2005L});
        insertUpdateStream.send(new Object[]{"MSFT", 100.6F, 2005L});
        Thread.sleep(1000);
        insertTestStream.send(new Object[]{"WSO2"});
        insertTestStream.send(new Object[]{"IBM"});
        insertTestStream.send(new Object[]{"MSFT"});
        Thread.sleep(1000);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "elasticsearchRecordsDelete", description = "Testing Records delete.")
    public void elasticsearchRecordsDelete() throws InterruptedException {
        log.info("elasticsearchRecordsDelete");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams =
                "define stream StockStream (symbol string, price float, volume long); \n" +
                        "define stream UpdateStream (symbol string, price float, volume long); \n" +
                        "define stream TestStream(symbol string); \n" +
                        "\n" +
                        "@store(type='elasticsearch', hostname='" + hostname + "', port='" + port + "', " +
                        "index.name='stock_index')\n" +
                        "@primaryKey('symbol') \n" +
                        "define table stock_table(symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream \n" +
                "select symbol, price, volume \n" +
                "insert into stock_table;";
        String query2 = "" +
                "@info(name = 'query2')\n" +
                "from UpdateStream " +
                "select symbol, price, volume\n" +
                "delete stock_table on stock_table.symbol == symbol;";
        String query3 = "" +
                "@info(name = 'query3')\n" +
                "from TestStream as a join stock_table as b on a.symbol == b.symbol\n" +
                "select a.symbol, price, volume\n" +
                "insert into AlertStream;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query + query2 + query3);
        InputHandler insertStockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler insertDeleteStream = siddhiAppRuntime.getInputHandler("UpdateStream");
        InputHandler insertTestStream = siddhiAppRuntime.getInputHandler("TestStream");
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 1005L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.6F, 1005L});
                                break;
                            default:
                                Assert.assertEquals(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });
        siddhiAppRuntime.start();

        insertStockStream.send(new Object[]{"WSO2", 55.6F, 1005L});
        insertStockStream.send(new Object[]{"IBM", 75.6F, 1005L});
        insertStockStream.send(new Object[]{"MSFT", 57.6F, 1005L});
        Thread.sleep(1000);
        insertDeleteStream.send(new Object[]{"IBM", 75.6F, 1005L});
        Thread.sleep(1000);
        insertTestStream.send(new Object[]{"WSO2"});
        insertTestStream.send(new Object[]{"IBM"});
        insertTestStream.send(new Object[]{"MSFT"});
        Thread.sleep(1000);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "elasticsearchRecordsContain", description = "Testing Records contain.")
    public void elasticsearchRecordsContain() throws InterruptedException {
        log.info("elasticsearchRecordsContain");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams =
                "define stream StockStream (symbol string, price float, volume long); \n" +
                        "define stream UpdateStream (symbol string, price float, volume long); \n" +
                        "define stream TestStream(symbol string); \n" +
                        "\n" +
                        "@store(type='elasticsearch', hostname='" + hostname + "', port='" + port + "', " +
                        "index.name='stock_index')\n" +
                        "@primaryKey('symbol') \n" +
                        "define table stock_table(symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream \n" +
                "select symbol, price, volume \n" +
                "insert into stock_table;";
        String query2 = "" +
                "@info(name = 'query2')\n" +
                "from UpdateStream " +
                "select symbol, price, volume\n" +
                "delete stock_table on stock_table.symbol == symbol;";
        String query3 = "" +
                "@info(name = 'query3')\n" +
                "from TestStream[stock_table.symbol == symbol in stock_table]\n" +
                "select symbol \n" +
                "insert into AlertStream;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query + query2 + query3);
        InputHandler insertStockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler insertDeleteStream = siddhiAppRuntime.getInputHandler("UpdateStream");
        InputHandler insertTestStream = siddhiAppRuntime.getInputHandler("TestStream");
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2"});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT"});
                                break;
                            default:
                                Assert.assertEquals(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });
        siddhiAppRuntime.start();

        insertStockStream.send(new Object[]{"WSO2", 55.6F, 1005L});
        insertStockStream.send(new Object[]{"IBM", 75.6F, 1005L});
        insertStockStream.send(new Object[]{"MSFT", 57.6F, 1005L});
        Thread.sleep(1000);
        insertDeleteStream.send(new Object[]{"IBM", 75.6F, 1005L});
        Thread.sleep(1000);
        insertTestStream.send(new Object[]{"WSO2"});
        insertTestStream.send(new Object[]{"IBM"});
        insertTestStream.send(new Object[]{"MSFT"});
        Thread.sleep(1000);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

