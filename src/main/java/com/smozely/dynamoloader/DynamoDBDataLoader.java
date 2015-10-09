package com.smozely.dynamoloader;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.collect.Lists;
import com.smozely.dynamoloader.internal.ClearOperation;
import com.smozely.dynamoloader.internal.DataLoaderOperation;

import java.util.List;

public class DynamoDBDataLoader {

    private final DynamoDB dynamo;

    private final List<DataLoaderOperation> operations = Lists.newLinkedList();

    public static DynamoDBDataLoader with(DynamoDB dynamo) {
        return new DynamoDBDataLoader(dynamo);
    }

    private DynamoDBDataLoader(DynamoDB dynamo) {
        this.dynamo = dynamo;
    }

    public DynamoDBDataLoader clear(String tableName) {
        operations.add(new ClearOperation(this.dynamo, tableName));
        return this;
    }

    public void execute() {
        operations.forEach(DataLoaderOperation::execute);
        operations.clear();
    }

}
