package com.smozely.dynamoloader.internal;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class BaseDataLoaderOperation {

    private final DynamoDB dynamoDB;

    public BaseDataLoaderOperation(DynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }

    public DynamoDB dynamoDB() {
        return dynamoDB;
    }
}
