package org.apache.nifi.processors.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PutBigqueryTest {

    protected PutBigquery putBigquery;


    @Before
    public void setUp() {
        final BigQuery mockDynamoDB = mock(BigQuery.class);

        putBigquery = new PutBigquery() {
            @Override
            protected BigQuery getBigQuery() {
                return mockDynamoDB;
            }
        };

    }

    @Test
    public void shouldUseDefaultBatchSizeWhenNotProvided() throws Exception {
        final TestRunner putRunner = TestRunners.newTestRunner(putBigquery);

        int batchSize = putRunner.getProcessContext().getProperty(PutBigquery.BATCH_SIZE).asInteger();

        Assert.assertEquals("Default Batch Size should be 500",500,batchSize);
    }


    @Test
    public void shouldSuccessfulWhenDataAreWriteCorrectly() {

        // Inject a mock BigQuery to insert data
        final BigQuery mockBigQuery = Mockito.mock(BigQuery.class);

        //mock row of data to insert
        Map<String, Integer> row = new HashMap<>();
        row.put("test_col", 2);
        //mock insert bigquery insert request
        List<InsertAllRequest.RowToInsert> rowsToInsert = new ArrayList<>();
        rowsToInsert.add(InsertAllRequest.RowToInsert.of(row));
        InsertAllRequest insertAllRequest = InsertAllRequest.of("test_dataset", "test_table", rowsToInsert);
        // mock success writing
        InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
        
        when(insertAllResponse.getErrorsFor(0)).thenReturn(new ArrayList<BigQueryError>());
        when(mockBigQuery.insertAll(insertAllRequest)).thenReturn(insertAllResponse);

        putBigquery = new PutBigquery() {
            @Override
            protected BigQuery getBigQuery() {
                return mockBigQuery;
            }
        };

        final TestRunner putRunner = TestRunners.newTestRunner(putBigquery);

        putRunner.setProperty(AbstractBigqueryProcessor.SERVICE_ACCOUNT_CREDENTIALS_JSON, "{}");
        putRunner.setProperty(PutBigquery.TABLE, "test_table");
        putRunner.setProperty(PutBigquery.DATASET, "test_dataset");

        String document = "{\"test_col\": 2}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1, true, false);

        putRunner.assertAllFlowFilesTransferred(AbstractBigqueryProcessor.REL_SUCCESS, 1);
    }

    @Test
    public void shouldFailWhenFlowFileIsNotAValidJson() {

        final TestRunner putRunner = TestRunners.newTestRunner(putBigquery);

        putRunner.setProperty(AbstractBigqueryProcessor.SERVICE_ACCOUNT_CREDENTIALS_JSON, "{}");
        putRunner.setProperty(PutBigquery.TABLE, "test_table");
        putRunner.setProperty(PutBigquery.DATASET, "test_dataset");

        String document = "{\"test_col\": 2 \"object\": {\"obj:3\"}}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1,true,false);

        putRunner.assertAllFlowFilesTransferred(AbstractBigqueryProcessor.REL_FAILURE, 1);


    }
//
    @Test
    public void shouldFailWhenErrorsOccursDuringSavingOnBigQuery() {

        // Inject a mock BigQuery to insert data
        final BigQuery mockBigQuery = Mockito.mock(BigQuery.class);


        //mock row of data to insert
        Map<String, Integer> row = new HashMap<>();
        row.put("test_col", 2);
        InsertAllRequest.RowToInsert rowToInsert = InsertAllRequest.RowToInsert.of(row);
        //mock insert bigquery insert request
        InsertAllRequest insertAllRequest = InsertAllRequest.of("test_dataset", "test_table", rowToInsert);
        // mock wrong writing
        InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
        List<BigQueryError> errors = new ArrayList<>();
        errors.add(new BigQueryError("reason", "location", "message"));
        when(insertAllResponse.getErrorsFor(0)).thenReturn(errors);
        when(mockBigQuery.insertAll(insertAllRequest)).thenReturn(insertAllResponse);

        putBigquery = new PutBigquery() {
            @Override
            protected BigQuery getBigQuery() {
                return mockBigQuery;
            }
        };

        final TestRunner putRunner = TestRunners.newTestRunner(putBigquery);

        putRunner.setProperty(AbstractBigqueryProcessor.SERVICE_ACCOUNT_CREDENTIALS_JSON, "{}");
        putRunner.setProperty(PutBigquery.TABLE, "test_table");
        putRunner.setProperty(PutBigquery.DATASET, "test_dataset");

        String document = "{\"test_col\": 2}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1,true,false);

        putRunner.assertAllFlowFilesTransferred(AbstractBigqueryProcessor.REL_FAILURE, 1);

    }
}
