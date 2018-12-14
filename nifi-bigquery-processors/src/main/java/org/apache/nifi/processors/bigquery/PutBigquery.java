package org.apache.nifi.processors.bigquery;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.json.JsonObjectParser;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bigquery.utils.JsonParserUtils;
import org.apache.nifi.processors.bigquery.utils.NotNullValuesHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

@SupportsBatching
@Tags({"Google", "BigQuery", "Google Cloud", "Put", "Insert"})
@CapabilityDescription("Puts a JSON document as a row into a BigQuery Table. The JSON fields are mapped with the table's columns names."
        + "If a JSON field not match with a table's column name, it will be ignored."
        + " The FlowFile content must be JSON")
public class PutBigquery extends AbstractBigqueryProcessor {

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Bigquery Table")
            .description("The table id where store the data. The table must be exist on bigquery")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATASET = new PropertyDescriptor.Builder()
            .name("Bigquery Dataset")
            .description("The dataset id where find the table. The dataset must be exist on bigquery")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(SERVICE_ACCOUNT_CREDENTIALS_JSON, READ_TIMEOUT, CONNECTION_TIMEOUT, PROJECT, DATASET, TABLE));


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    
    private List<String> formatBigqueryErrors(List<BigQueryError> errors) {
    	List<String> errorsString = new ArrayList<>();
    	for (BigQueryError error : errors) { errorsString.add(error.toString()); }
    	return errorsString;
    }
    
    private String created_at() {
    	TimeZone tz = TimeZone.getTimeZone("UTC");
    	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    	df.setTimeZone(tz);
    	return df.format(new Date());
    }
    
    private JSONObject parseJson(InputStream jsonStream) throws JsonIOException, JsonSyntaxException, IOException {   	
    	return new JSONObject(JsonParserUtils.fromStream(jsonStream).toString());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(1000);
        
        List<InsertAllRequest.RowToInsert> rowsToInsert = new ArrayList<>();
        List<FlowFile> flowFilesToInsert = new ArrayList<>();
        List<JSONObject> listOfContent = new ArrayList<>();
        
        final String table = context.getProperty(TABLE).getValue();
        final String dataset = context.getProperty(DATASET).getValue();

    	for (FlowFile flowFile : flowFiles) {
    		try {
    			JSONObject jsonDocument = parseJson(session.read(flowFile));
        		InsertAllRequest.RowToInsert rowToInsert = InsertAllRequest.RowToInsert.of(jsonDocument.toMap());
        		rowsToInsert.add(rowToInsert);
        		flowFilesToInsert.add(flowFile);
        		listOfContent.add(jsonDocument);
    		} catch (IOException e) {
				getLogger().error("IOException while reading JSON item: " + e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
    		} catch (JsonIOException e) {
				getLogger().error("JsonIOException while reading JSON item: " + e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
			} catch (JsonSyntaxException e) {
				getLogger().error("JsonSyntaxException while reading JSON item: " + e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
			}
		}
//    	
    	if ( !rowsToInsert.isEmpty() ) {
    		InsertAllRequest insertAllRequest = InsertAllRequest.of(dataset, table, rowsToInsert);
    		session.transfer(flowFilesToInsert.get(0), REL_SUCCESS);
        	InsertAllResponse insertAllResponse = getBigQuery().insertAll(insertAllRequest);
        	
//        	for ( int index = 0; index < flowFilesToInsert.size(); index++ ) {
//        		List<BigQueryError> errors = insertAllResponse.getErrorsFor(index);
//        		FlowFile flowFile = flowFilesToInsert.get(index);
//        		
//        		if ( errors.isEmpty() ) {
//        			session.transfer(flowFile, REL_SUCCESS);
//        		} else {
//        			String content = listOfContent.get(index).toString();
//        			
//        			flowFile = session.write(flowFile, new OutputStreamCallback() {
//						@Override
//						public void process(OutputStream out) throws IOException {
//							JSONObject json = new JSONObject();
//							
//							json.put("errors", formatBigqueryErrors(errors));
//							json.put("content", content);
//							json.put("created_at", created_at());
//									
//							out.write(json.toString().getBytes());
//						}
//					});
//        			
//        			session.transfer(flowFile, REL_FAILURE);
//        		}
//        	}
    	}

    }
}
