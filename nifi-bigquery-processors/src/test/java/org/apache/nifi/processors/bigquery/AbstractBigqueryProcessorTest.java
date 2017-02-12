package org.apache.nifi.processors.bigquery;

import com.google.cloud.bigquery.BigQuery;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bigquery.exception.BigQueryInitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;


public class AbstractBigqueryProcessorTest {
    MockAbstractBigqueryProcessor processor;
    private TestRunner testRunner;


    @Before
    public void setUp() throws Exception {
        processor = new MockAbstractBigqueryProcessor();
        testRunner = TestRunners.newTestRunner(processor);
    }


    @Test
    public void testCustomValidate() throws Exception {
        //credentials are required
        testRunner.setProperty(AbstractBigqueryProcessor.SERVICE_ACCOUNT_CREDENTIALS_JSON, "");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractBigqueryProcessor.SERVICE_ACCOUNT_CREDENTIALS_JSON, "{\n" +
                "  \"type\": \"service_account\",\n" +
                "  \"project_id\": \"test\",\n" +
                "  \"private_key_id\": \"projectkey\",\n" +
                "  \"private_key\": \"test_private_key\",\n" +
                "  \"client_email\": \"test-compute@developer.gserviceaccount.com\",\n" +
                "  \"client_id\": \"idtest\",\n" +
                "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
                "  \"token_uri\": \"https://accounts.google.com/o/oauth2/token\",\n" +
                "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
                "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/test-compute%40developer.gserviceaccount.com\"\n" +
                "}");
        testRunner.assertValid();

        //connect timeout must be an integer value
        testRunner.setProperty(AbstractBigqueryProcessor.CONNECTION_TIMEOUT, "2a2");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractBigqueryProcessor.CONNECTION_TIMEOUT, "2.2");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractBigqueryProcessor.CONNECTION_TIMEOUT, "2,2");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractBigqueryProcessor.CONNECTION_TIMEOUT, "22");
        testRunner.assertValid();

        //read timeout must be an integer value
        testRunner.setProperty(AbstractBigqueryProcessor.READ_TIMEOUT, "2a2");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractBigqueryProcessor.READ_TIMEOUT, "2.2");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractBigqueryProcessor.READ_TIMEOUT, "2,2");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractBigqueryProcessor.READ_TIMEOUT, "22");
        testRunner.assertValid();
    }

    @Test
    public void shouldCreateABigQueryClientOnProcessSchedulation() throws Exception {

        MockAbstractBigqueryProcessor spyProcessor = spy(processor);

        processor.setBigQuery(null);

        assertNull(processor.getBigQuery());

        spyProcessor.onSchedule(testRunner.getProcessContext());

        //verify that bigquery is created
        verify(spyProcessor).createBigquery(testRunner.getProcessContext());

        assertNotNull(spyProcessor.getBigQuery());

    }

    /**
     * Provides a stubbed processor instance for testing
     */
    public static class MockAbstractBigqueryProcessor extends AbstractBigqueryProcessor {

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Arrays.asList(SERVICE_ACCOUNT_CREDENTIALS_JSON, READ_TIMEOUT, CONNECTION_TIMEOUT);
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }

        @Override
        protected BigQuery createBigquery(ProcessContext context) throws BigQueryInitializationException {
            BigQuery mockBigQuery = mock(BigQuery.class);
            return mockBigQuery;
        }

        public void setBigQuery(BigQuery newBigQuery) {
            this.bigQuery = newBigQuery;
        }

    }
}
