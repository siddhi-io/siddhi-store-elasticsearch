package org.wso2.extension.siddhi.store.elasticsearch.exceptions;

import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

/**
 * This exception represents the SiddhiAppRuntimeException which might be thrown if there are any issues in the
 * elasticsearch event table
 */
public class ElasticsearchEventTableException extends SiddhiAppRuntimeException {
    public ElasticsearchEventTableException(String message) {
        super(message);
    }

    public ElasticsearchEventTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
