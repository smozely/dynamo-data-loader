package com.smozely.dynamoloader.internal;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
            topJson.iterator().forEachRemaining(it -> {
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
        node.fields().forEachRemaining(it -> item.with(it.getKey(), getValueFromNode(it.getValue())));
    }

    private Object getValueFromNode(JsonNode node) {
        if (node.isTextual()) {
            return node.textValue();

        } else if (node.isNumber()) {
            return new BigDecimal(node.asText());

        } else if (node.isBoolean()) {
            return node.asBoolean();

        } else if (node.isArray()) {
            List<Object> list = Lists.newArrayList();
            node.forEach(it -> list.add(getValueFromNode(it)));

            if (list.stream().allMatch(it -> it instanceof String) || list.stream().allMatch(it -> it instanceof Number)) {
                return new HashSet(list);
            }

            return list;
        } else if (node.isObject()) {
            Map<String, Object> map = Maps.newHashMap();
            node.fields().forEachRemaining(it -> map.put(it.getKey(), getValueFromNode(it.getValue())));
            return map;
        } else {
            throw new IllegalArgumentException("Could not handle Node Type : " + node.getNodeType());
        }
    }
}
