package com.smozely.dynamoloader.internal

import com.smozely.dynamoloader.testsupport.BaseLocalDynamoSpec

class LoadOperationTest extends BaseLocalDynamoSpec {

    public static final String TABLE_FOR_LOADING = "TABLE_FOR_LOADING"

    private LoadOperation underTest

    @Override
    public void setupData() {
        support.createGenericHashRangeTable(TABLE_FOR_LOADING, "id", "range")
    }

    def "Loads Data from JSON File Correctly"() {
        given:
        def json = """
                    [
                        {
                         "id": "123",
                         "range": "456",
                         "number": 123.99,
                         "boolean1": true,
                         "boolean2": false
                         }
                    ]
                   """

        when:
        underTest = new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json))
        underTest.execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123", "range", "456")

        item != null

        item.getBOOL("boolean1") == true
        item.getBOOL("boolean2") == false

        item.getDouble("number") == 123.99


    }

}
