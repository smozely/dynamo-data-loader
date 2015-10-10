package com.smozely.dynamoloader.testsupport

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.TableCollection
import com.amazonaws.services.dynamodbv2.model.ListTablesResult
import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Specification

public abstract class BaseLocalDynamoSpec extends Specification {

    protected DynamoDB dynamoDB;
    protected AlternatorSupport support;
    protected ObjectMapper mapper = new ObjectMapper();

    public void setup() {
        setupDynamoDB();
        deleteAllTables();
        support = new AlternatorSupport(dynamoDB);

        setupData();
    }

    private void setupDynamoDB() {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
        client.setEndpoint("http://localhost:8000");
        dynamoDB = new DynamoDB(client);
    }

    private void deleteAllTables() {
        TableCollection<ListTablesResult> tables = dynamoDB.listTables();

        tables.forEach({ it ->
            it.delete();
        });
    }

    protected abstract void setupData();

}
