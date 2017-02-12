package org.apache.nifi.processors.bigquery;

import com.google.api.client.json.GenericJson;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bigquery.exception.BigQueryInitializationException;
import org.apache.nifi.processors.bigquery.utils.JsonParserUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * AbstractBigqueryProcessor is a base class for Bigquery processors and contains logic and variables common to most
 * processors integrating with Google Bigquery.
 */
public abstract class AbstractBigqueryProcessor extends AbstractProcessor {

    static final PropertyDescriptor SERVICE_ACCOUNT_CREDENTIALS_JSON = new PropertyDescriptor.Builder()
            .name("Service Account Credentials Json")
            .description("Service Account Credentials permit to authenticate an application to the Google Services. The credentials should be "
                    + "provided to connect on bigquery service")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Timeout to set a connection with BigQuery")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Timeout to read data from an established connection with Bigquery Service")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor PROJECT = new PropertyDescriptor.Builder()
            .name("Google Cloud Project")
            .description("The project id where find dataset and table. If not provided will be used the project specified on service account credentials")
            .required(false)
            .addValidator(StandardValidators.StringLengthValidator.VALID)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to success relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to failure relationship").build();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));


    protected volatile BigQuery bigQuery;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @OnScheduled
    public void onSchedule(final ProcessContext context) throws BigQueryInitializationException {
        this.bigQuery = createBigquery(context);
    }

    protected BigQuery getBigQuery() {
        return bigQuery;
    }

    protected BigQuery createBigquery(ProcessContext context) throws BigQueryInitializationException {

        PropertyValue serviceAccountCredentialsJsonProperty = context.getProperty(SERVICE_ACCOUNT_CREDENTIALS_JSON);
        PropertyValue readTimeoutProperty = context.getProperty(READ_TIMEOUT);
        PropertyValue connectionTimeoutProperty = context.getProperty(CONNECTION_TIMEOUT);

        BigQueryOptions.Builder bigQueryOptionBuilder = BigQueryOptions.newBuilder();


        try(ByteArrayInputStream serviceAccountCredentialsJsonByteArray = new ByteArrayInputStream(serviceAccountCredentialsJsonProperty.getValue().getBytes(StandardCharsets.UTF_8))) {
            bigQueryOptionBuilder.setCredentials(ServiceAccountCredentials.fromStream(serviceAccountCredentialsJsonByteArray));
        } catch (IOException e) {
            throw new BigQueryInitializationException("fail to load service account credentials", e);
        }


        //load project id
       PropertyValue projectIdProperty = context.getProperty(PROJECT);
        if (projectIdProperty.isSet()) {
            bigQueryOptionBuilder.setProjectId(projectIdProperty.getValue());
        } else {
            try(ByteArrayInputStream serviceAccountCredentialsJsonByteArray = new ByteArrayInputStream(serviceAccountCredentialsJsonProperty.getValue().getBytes(StandardCharsets.UTF_8))){
                bigQueryOptionBuilder.setProjectId(getProjectId(serviceAccountCredentialsJsonByteArray));
            }catch(IOException e){
                throw new BigQueryInitializationException("A project id is required but could not be determined from the properties or Service Account Credentials", e);
            }
        }



        //read timeout is set?
        if (readTimeoutProperty.isSet()) {
            //set read
            bigQueryOptionBuilder.setReadTimeout(readTimeoutProperty.asInteger());
        }//otherwise use default

        //connection timeout is set?
        if (connectionTimeoutProperty.isSet()) {
            bigQueryOptionBuilder.setConnectTimeout(connectionTimeoutProperty.asInteger());
        }//otherwise use default

        return bigQueryOptionBuilder.build().getService();
    }

    /**
     * Return project id from a Service Account Credential Json
     *
     * @param serviceAccountCredentialsJson Json Stream of Service Account Credentials Stream
     * @return project id of the credentials
     *
     */
    private String getProjectId(InputStream serviceAccountCredentialsJson) throws IOException {
        GenericJson credential = JsonParserUtils.fromStream(serviceAccountCredentialsJson);

        String projectId = (String) credential.get("project_id");

        if (projectId == null) {
            throw new IOException("Project id not found in the JSON stream");
        }

        return projectId;
    }

}
