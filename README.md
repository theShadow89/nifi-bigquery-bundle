# NiFi Bigquery Bundle

Bigquery bundle for Apache NiFi 

### Processors

#### PutBigQuery

Save Flow File content on a BigQuery table. The format of data must be JSON.

##### Required Properties

- Google Account Service Credential: The Google Service credentials JSON. To obtain it, see https://developers.google.com/identity/protocols/OAuth2ServiceAccount
- Dataset: the name of BigQuery dataset
- Table: the name of BigQuery Table on the Dataset

##### Required Properties

- Read Time Out: the time to wait for a response from BigQuery service
- Connection Time Out: the time to wait during connection establishment with BigQuery service
- Project Id: Google CLoud project id. If not specified, the process try to obtain it from provided credentials mentioned above.

### Deploy Bundle

Clone this repository

```
    git clone https://github.com/theShadow89/nifi-bigquery-bundle
```

Build the bundle

```
    cd nifi-bigquery-bundle
    mvn clean install
```

Copy Nar file to NIFI_HOME/lib

```
    cp nifi-bigquery-nar/target/nifi-bigquery-nar-0.1.nar NIFI_HOME/lib/
```

Start/Restart Nifi

```
    NIFI_HOME/bin/nifi.sh start
```


TODO

- Add Get Processor