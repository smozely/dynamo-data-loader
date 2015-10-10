package com.smozely.dynamoloader.internal;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import java.util.List;
import java.util.Optional;

public class ClearOperation implements DataLoaderOperation {

    private final Table table;

    public ClearOperation(Table table) {
        this.table = table;
    }

    @Override
    public void execute() {
        TableDescription description = table.describe();

        KeySchemaElement hashKey = getHashKeyElement(description.getKeySchema());
        Optional<KeySchemaElement> rangeKey = getRangeKeyElement(description.getKeySchema());

        table.scan().forEach(it -> {

            if (rangeKey.isPresent()) {
                table.deleteItem(hashKey.getAttributeName(), it.get(hashKey.getAttributeName()),
                        rangeKey.get().getAttributeName(), it.get(rangeKey.get().getAttributeName()));
            } else {
                table.deleteItem(hashKey.getAttributeName(), it.get(hashKey.getAttributeName()));
            }
        });
    }

    public KeySchemaElement getHashKeyElement(List<KeySchemaElement> schema) {
        return schema.stream().filter(it -> it.getKeyType().equals(KeyType.HASH.toString())).findFirst().get();
    }

    public Optional<KeySchemaElement> getRangeKeyElement(List<KeySchemaElement> schema) {
        return schema.stream().filter(it -> it.getKeyType().equals(KeyType.RANGE.toString())).findFirst();
    }
}
