package org.apache.nifi.processors.bigquery.utils;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * Utils for parsing json
 */
public class JsonParserUtils {

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * Parse a JSON Stream
     *
     * @param jsonStream the stream representing the JSON to parse
     * @return {@link GenericJson} of Json Stream
     * @throws IOException if json can't be parsed from the stream
     */
    public static GenericJson fromStream(InputStream jsonStream) throws IOException {
        JsonObjectParser parser = new JsonObjectParser(JSON_FACTORY);
        GenericJson jsonContent = parser.parseAndClose(
                jsonStream, UTF_8, GenericJson.class);
        return jsonContent;
    }
}
