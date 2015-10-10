package com.smozely.dynamoloader.internal;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;

public class LoadOperation implements DataLoaderOperation {

    private final Table table;

    private final JsonNode topJson;

    public LoadOperation(Table table, JsonNode topJson) {
        this.table = table;
        this.topJson = topJson;
    }

    @Override
    public void execute() {
        if (topJson.isArray()) {
            topJson.iterator().forEachRemaining( it -> {
                putItem(it);
            });
        } else {
            putItem(topJson);
        }
    }

    private void putItem(JsonNode node) {
        Item item = new Item();
        populateItem(node, item);
        table.putItem(item);
    }

    private void populateItem(JsonNode node, Item item) {
        node.fields().forEachRemaining( it -> {
            if (it.getValue().isTextual()) {
                item.withString(it.getKey(), it.getValue().asText());

            } else if (it.getValue().isNumber()) {
                item.withNumber(it.getKey(), new BigDecimal(it.getValue().asText()));

            } else if (it.getValue().isBoolean()) {
                item.withBoolean(it.getKey(), it.getValue().asBoolean());

            } else if (it.getValue().isArray()) {


            }
        });
    }
}
