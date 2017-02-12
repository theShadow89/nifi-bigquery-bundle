package org.apache.nifi.processors.bigquery.utils;


import java.util.HashMap;

/**
 * In this type of map th null values are not permitted.
 * The key value pairs with a null value are not stored in the map
 */
public class NotNullValuesHashMap<K, V>
        extends HashMap<K, V> {

    @Override
    public V put(K key, V value) {
        return value != null ? super.put(key, value) : null;
    }
}