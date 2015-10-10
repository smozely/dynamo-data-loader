package com.smozely.dynamoloader.internal

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.smozely.dynamoloader.testsupport.BaseLocalDynamoSpec

class ClearOperationSpec extends BaseLocalDynamoSpec {

    public static final String TABLE_WITH_HASH_KEY = "TEST_HASH_KEYED_TABLE"
    public static final String TABLE_WITH_HASH_RANGE_KEY = "TEST_HASH_RANGE_KEYED_TABLE"

    private ClearOperation underTest

    @Override
    public void setupData() {
        support.createGenericTable(TABLE_WITH_HASH_KEY, "id")
        support.createGenericHashRangeTable(TABLE_WITH_HASH_RANGE_KEY, "id", "range")
    }

    def "Clears Table With hash key correctly"() {
        given:
        Table table = dynamoDB.getTable(TABLE_WITH_HASH_KEY)
        table.putItem(new Item().with("id", "123"))
        table.putItem(new Item().with("id", "456"))

        when:
        underTest = new ClearOperation(dynamoDB.getTable(TABLE_WITH_HASH_KEY))
        underTest.execute()

        then:
        def scan = table.scan()
        scan.forEach({})
        scan.totalCount == 0
    }

    def "Clears Table With hash and range key correctly"() {
        given:
        Table table = dynamoDB.getTable(TABLE_WITH_HASH_RANGE_KEY)
        table.putItem(new Item().with("id", "123").with("range", "456"))
        table.putItem(new Item().with("id", "456").with("range", "123"))

        when:
        underTest = new ClearOperation(dynamoDB.getTable(TABLE_WITH_HASH_RANGE_KEY))
        underTest.execute()

        then:
        def scan = table.scan()
        scan.forEach({})
        scan.totalCount == 0
    }
}