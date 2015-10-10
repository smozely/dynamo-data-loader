package com.smozely.dynamoloader.internal;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import java.util.List;

public class ClearOperation extends BaseDataLoaderOperation implements DataLoaderOperation {

    private final Table table;

    public ClearOperation(DynamoDB dynamoDB, String tableName) {
        this(dynamoDB, dynamoDB.getTable(tableName));
    }

    public ClearOperation(DynamoDB dynamoDB, Table table) {
        super(dynamoDB);
        this.table = table;
    }

    @Override
    public void execute() {
        TableDescription description = table.describe();

        KeySchemaElement hashKey = getHashKeyElement(description.getKeySchema());
        KeySchemaElement rangeKey = getRangeKeyElement(description.getKeySchema());

        table.scan().forEach(it -> {

            if (rangeKey.getAttributeName().equals("")) {
                table.deleteItem(hashKey.getAttributeName(), it.get(hashKey.getAttributeName()));
            } else {
                table.deleteItem(hashKey.getAttributeName(), it.get(hashKey.getAttributeName()),
                        rangeKey.getAttributeName(), it.get(rangeKey.getAttributeName()));
            }
        });
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
}
