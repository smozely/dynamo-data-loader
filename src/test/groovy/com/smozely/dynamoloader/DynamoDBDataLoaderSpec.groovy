package com.smozely.dynamoloader

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.smozely.dynamoloader.testsupport.BaseLocalDynamoSpec

/**
 * Super simple test to show the loader hangs together, most testing is done on the internal classes.
 */
class DynamoDBDataLoaderSpec extends BaseLocalDynamoSpec {

    public static final String TABLE1 = "TABLE1"
    public static final String TABLE2 = "TABLE2"

    @Override
    public void setupData() {
        support.createGenericTable(TABLE1, "id")
        support.createGenericTable(TABLE2, "id")
    }

    def "Loader Executes Provided Operations"() {
        given:
        Table table1 = dynamoDB.getTable(TABLE1)
        table1.putItem(new Item().with("id", "123"))
        table1.putItem(new Item().with("id", "456"))

        Table table2 = dynamoDB.getTable(TABLE2)
        table2.putItem(new Item().with("id", "123"))
        table2.putItem(new Item().with("id", "456"))

        when:
        DynamoDBDataLoader
                .with(dynamoDB)
                .clear(TABLE1)
                .clear(table2)
                .execute()

        then:
        def scan1 = table1.scan()
        scan1.forEach({})
        scan1.totalCount == 0

        def scan2 = table2.scan()
        scan2.forEach({})
        scan2.totalCount == 0
    }

}