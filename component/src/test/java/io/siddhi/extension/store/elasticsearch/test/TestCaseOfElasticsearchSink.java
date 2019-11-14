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
package io.siddhi.extension.store.elasticsearch.test;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.extension.store.elasticsearch.test.utils.ElasticsearchUtils;
import io.siddhi.extension.store.elasticsearch.test.utils.TestAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for elasticsearch sink.
 */
public class TestCaseOfElasticsearchSink {

    private static final Logger log = Logger.getLogger(TestCaseOfElasticsearchEventTableIT.class);
    private static String hostname;
    private static String port;

    @BeforeClass
    public static void startTest() {
        log.info("== Elasticsearch Table tests completed ==");
        hostname = ElasticsearchUtils.getIpAddressOfContainer();
        port = ElasticsearchUtils.getContainerPort();
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Elasticsearch Table tests completed ==");
    }


    @Test(testName = "elasticsearchSinkTestCase", description = "Testing Records insertion.", enabled = true)
    public void elasticsearchSinkTestCase01() throws InterruptedException {
        final TestAppender appender = new TestAppender();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        rootLogger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams =
                "@sink(type='elasticsearch', host='" + hostname + "', port='" + port + "', " +
                        "index.name='stock_index3', " +
                        "@map(type='json', @payload(\"\"\"{\n" +
                        "   \"Stock Data\":{\n" +
                        "      \"Symbol\":\"{{symbol}}\",\n" +
                        "      \"Price\":{{price}},\n" +
                        "      \"Volume\":{{volume}}\n" +
                        "   }\n" +
                        "}\"\"\"))) " +
                        "define stream stock_stream(symbol string, price float, volume long);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        InputHandler insertStockStream = siddhiAppRuntime.getInputHandler("stock_stream");
        siddhiAppRuntime.start();
        insertStockStream.send(new Object[]{"WSO2", 55.6F, 7000L});
        insertStockStream.send(new Object[]{"IBM2", 75.6F, 8000L});
        insertStockStream.send(new Object[]{"MSFT2", 57.6F, 9000L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            logMessages.add(message);
        }
        Assert.assertTrue(logMessages.contains("[{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"WSO2\",\n" +
                "      \"Price\":55.6,\n" +
                "      \"Volume\":7000\n" +
                "   }\n" +
                "}] has been successfully added."));
        Assert.assertTrue(logMessages.contains("[{\n" +
                "   \"Stock Data\":{\n" +
                "      \"Symbol\":\"MSFT2\",\n" +
                "      \"Price\":57.6,\n" +
                "      \"Volume\":9000\n" +
                "   }\n" +
                "}] has been successfully added."));
    }
}
