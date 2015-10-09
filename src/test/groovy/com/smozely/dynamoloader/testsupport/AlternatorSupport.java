package com.smozely.dynamoloader.testsupport;


import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlternatorSupport {

    private DynamoDB dynamoDb;

    private ProvisionedThroughput provisionedThroughput;

    public AlternatorSupport(DynamoDB db) {
        this.dynamoDb = db;

        provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L);
        provisionedThroughput.setReadCapacityUnits(10L);
        provisionedThroughput.setWriteCapacityUnits(10L);
    }

    public AttributeDefinition createStringAttributeDefinition(String name) {
        return createAttributeDefinition(name, ScalarAttributeType.S);
    }

    public AttributeDefinition createAttributeDefinition(
            String name,
            ScalarAttributeType type) {

        AttributeDefinition attr = new AttributeDefinition();
        attr.setAttributeName(name);
        attr.setAttributeType(type);
        return attr;
    }

    public List<KeySchemaElement> createKeySchema(
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr) {

        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        if (hashAttr != null) {
            keySchema.add(
                    new KeySchemaElement()
                            .withAttributeName(hashAttr.getAttributeName())
                            .withKeyType(KeyType.HASH)
            );
        }
        if (rangeAttr != null) {
            keySchema.add(
                    new KeySchemaElement()
                            .withAttributeName(rangeAttr.getAttributeName())
                            .withKeyType(KeyType.RANGE)
            );
        }
        return keySchema;
    }

    public List<AttributeDefinition> createKeyAttributes(
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr) {

        List<AttributeDefinition> attrList = new ArrayList<AttributeDefinition>();
        if (hashAttr != null) {
            attrList.add(hashAttr);
        }
        if (rangeAttr != null) {
            attrList.add(rangeAttr);
        }
        return attrList;
    }

    public KeySchemaElement getHashKeyElement(List<KeySchemaElement> schema) {
        KeySchemaElement hashElement = new KeySchemaElement().withAttributeName("");
        for (KeySchemaElement element : schema) {
            if (element.getKeyType().equals(KeyType.HASH.toString())) {
                hashElement = element;
                break; // out of 'for'
            }
        }
        return hashElement;
    }

    public KeySchemaElement getRangeKeyElement(List<KeySchemaElement> schema) {
        KeySchemaElement rangeElement = new KeySchemaElement().withAttributeName("");
        for (KeySchemaElement element : schema) {
            if (element.getKeyType().equals(KeyType.RANGE.toString())) {
                rangeElement = element;
                break; // out of 'for'
            }
        }
        return rangeElement;
    }

    public Map<String, AttributeValue> createItemKey(
            String hashName,
            AttributeValue hashValue) {

        return createItemKey(hashName, hashValue, null, null);
    }

    public Map<String, AttributeValue> createItemKey(
            String hashName,
            AttributeValue hashValue,
            String rangeName,
            AttributeValue rangeValue) {

        Map<String, AttributeValue> attrMap = new HashMap<String, AttributeValue>();
        if (hashName != null) {
            attrMap.put(hashName, hashValue);
        }
        if (rangeName != null) {
            attrMap.put(rangeName, rangeValue);
        }
        return attrMap;
    }

    public TableDescription createTable(
            String name,
            AttributeDefinition hashAttr) {
        return createTable(name, hashAttr, null);
    }

    public TableDescription createTable(
            String name,
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr) {
        return createTable(name, hashAttr, rangeAttr, provisionedThroughput);
    }

    public TableDescription createTable(
            String name,
            AttributeDefinition hashAttr,
            AttributeDefinition rangeAttr,
            ProvisionedThroughput throughput) {

        CreateTableRequest request =
                new CreateTableRequest()
                        .withTableName(name)
                        .withKeySchema(createKeySchema(hashAttr, rangeAttr))
                        .withAttributeDefinitions(createKeyAttributes(hashAttr, rangeAttr))
                        .withProvisionedThroughput(throughput);
        Table result = dynamoDb.createTable(request);
        TableDescription tableDesc = result.getDescription();
        return tableDesc;
    }

    public void createGenericTable(String tableName, String hashKeyName) {
        AttributeDefinition hashAttr = createStringAttributeDefinition(hashKeyName);
        createTable(tableName, hashAttr);
    }

    public void createGenericHashRangeTable(String tableName, String hashKeyName, String rangeKeyName) {
        AttributeDefinition hashAttr = createStringAttributeDefinition(hashKeyName);
        AttributeDefinition rangeAttr = createStringAttributeDefinition(rangeKeyName);
        createTable(tableName, hashAttr, rangeAttr);
    }

}