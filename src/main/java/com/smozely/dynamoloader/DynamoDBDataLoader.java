package com.smozely.dynamoloader;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.smozely.dynamoloader.internal.ClearOperation;
import com.smozely.dynamoloader.internal.DataLoaderOperation;
import com.smozely.dynamoloader.internal.LoadOperation;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DynamoDBDataLoader {

    private final DynamoDB dynamo;

    private final List<DataLoaderOperation> operations = Lists.newLinkedList();

    private final ObjectMapper mapper = new ObjectMapper();

    public static DynamoDBDataLoader with(DynamoDB dynamo) {
        return new DynamoDBDataLoader(dynamo);
    }

    private DynamoDBDataLoader(DynamoDB dynamo) {
        this.dynamo = dynamo;
    }

    public DynamoDBDataLoader clear(String tableName) {
        return clear(this.dynamo.getTable(tableName));
    }

    public DynamoDBDataLoader clear(Table table) {
        operations.add(new ClearOperation(table));
        return this;
    }

    public DynamoDBDataLoader load(String tableName, File jsonFile) {
        return load(dynamo.getTable(tableName), jsonFile);
    }

    public DynamoDBDataLoader load(Table table, File jsonFile) {
        try {
            operations.add(new LoadOperation(table, mapper.readTree(jsonFile)));
        } catch (IOException e) {
            throw new RuntimeException("IO Exception reading jsonFile", e);
        }
        return this;
    }

    public void execute() {
        operations.forEach(DataLoaderOperation::execute);
        operations.clear();
    }

}
