package org.apache.nifi.processors.bigquery.exception;

/**
 * This exception will be rise when there are problems on init
 * a {@link com.google.cloud.bigquery.BigQuery}
 */
public class BigQueryInitializationException extends Exception {

    public BigQueryInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigQueryInitializationException(String message) {
        super(message);
    }
}
