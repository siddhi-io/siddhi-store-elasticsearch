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

import io.siddhi.extension.store.elasticsearch.ElasticsearchCompiledCondition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * This class contains the utility methods required by the indexer service.
 */
public class ElasticsearchTableUtils {

    private static Log log = LogFactory.getLog(ElasticsearchTableUtils.class);

    public static String resolveCondition(ElasticsearchCompiledCondition compiledCondition,
                                          Map<String, Object> parameters) {
        String condition = compiledCondition.getCompiledQuery();
        if (log.isDebugEnabled()) {
            log.debug("compiled condition for collection : " + condition);
        }
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            String namePlaceholder = Pattern.quote("[" + name + "]");
            condition = condition.replaceAll(namePlaceholder, value.toString());
        }
        //set solr "select all" query if condition is not provided
        if (condition == null || condition.isEmpty()) {
            condition = "*:*";
        }
        if (log.isDebugEnabled()) {
            log.debug("Resolved condition for collection : " + condition);
        }
        return condition;
    }

    /**
     * Utility method which can be used to check if a given string instance is null or empty.
     *
     * @param field the string instance to be checked.
     * @return true if the field is null or empty.
     */
    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    /**
     * Utility method which can be used to UUID for a given record using primary key.
     *
     * @param attributes  the attribute list of the store.
     * @param record      the record object array.
     * @param primaryKeys the primary keys list.
     * @return the generated UUID for the record based on the primary keys as string.
     */
    public static String generateRecordIdFromPrimaryKeyValues(List<Attribute> attributes,
                                                              Object[] record, List<String> primaryKeys) {
        StringBuilder builder = new StringBuilder();
        for (String key : primaryKeys) {
            for (Attribute attribute : attributes) {
                if (attribute.getName().equals(key)) {
                    int ordinal = attributes.indexOf(attribute);
                    Object obj = record[ordinal];
                    if (obj != null) {
                        builder.append(obj.toString());
                    }
                }
            }
        }
        /* to make sure, we don't have an empty string */
        builder.append("");
        byte[] data = builder.toString().getBytes(Charset.forName("UTF-8"));
        return UUID.nameUUIDFromBytes(data).toString();
    }

    public static String generateRecordIdFromPrimaryKeyValues(List<Attribute> attributes,
                                                              Map<String, Object> record,
                                                              List<String> primaryKeys) {
        StringBuilder builder = new StringBuilder();
        for (String key : primaryKeys) {
            for (Attribute attribute : attributes) {
                if (attribute.getName().equals(key)) {
                    Object obj = record.get(key);
                    if (obj != null) {
                        builder.append(obj.toString());
                    }
                }
            }
        }
        /* to make sure, we don't have an empty string */
        builder.append("");
        byte[] data = builder.toString().getBytes(Charset.forName("UTF-8"));
        return UUID.nameUUIDFromBytes(data).toString();
    }
}
