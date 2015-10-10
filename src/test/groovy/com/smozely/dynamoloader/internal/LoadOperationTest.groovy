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
                         "number_double": 123.99,
                         "number_int": 123,

                         "boolean1": true,
                         "boolean2": false,
                         "string_array": ["a", "b", "c"],
                         "mixed_array": ["a", 123, false, 123.45]
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

        item.getDouble("number_double") == 123.99
        item.getInt("number_int") == 123

        item.getStringSet("string_array") != null
        item.getStringSet("string_array").contains("a")
        item.getStringSet("string_array").contains("b")
        item.getStringSet("string_array").contains("c")

        item.getList("mixed_array") != null
        item.getList("mixed_array").get(0).equals("a")
        item.getList("mixed_array").get(1).equals(new BigDecimal("123"))
        item.getList("mixed_array").get(2).equals(false)
        item.getList("mixed_array").get(3).equals(new BigDecimal("123.45"))

    }

}
