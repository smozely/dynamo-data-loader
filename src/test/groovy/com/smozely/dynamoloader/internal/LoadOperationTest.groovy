package com.smozely.dynamoloader.internal

import com.smozely.dynamoloader.testsupport.BaseLocalDynamoSpec

class LoadOperationTest extends BaseLocalDynamoSpec {

    public static final String TABLE_FOR_LOADING = "TABLE_FOR_LOADING"

    @Override
    public void setupData() {
        support.createGenericTable(TABLE_FOR_LOADING, "id")
    }

    def "Loads Data from JSON Object Correctly"() {
        given:
        def json = """ {"id": "123"} """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")

        item != null
        item.getString("id") == "123"
    }

    def "Loads Data from JSON List Correctly"() {
        given:
        def json = """ [ {"id": "123"}, {"id": "456"} ] """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item1 = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")
        def item2 = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "456")

        item1 != null
        item1.getString("id") == "123"

        item2 != null
        item2.getString("id") == "456"
    }

    def "Loads Data with different standard types correctly"() {
        given:
        def json = """
                        {
                         "id": "123",
                         "string": "Some String",
                         "number_double": 123.99,
                         "number_int": 123,
                         "boolean_true": true,
                         "boolean_false": false
                         }
                   """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")

        item != null

        item.getString("string") == "Some String"
        item.getDouble("number_double") == 123.99
        item.getInt("number_int") == 123
        item.getBoolean("boolean_true") ==  true
        item.getBoolean("boolean_false") == false
    }

    def "Loads Data With List Correctly"() {
        given:
        def json = """
                        {
                         "id": "123",
                         "mixed_array": ["a", 123, false, 123.45]
                         }
                   """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")

        item != null

        item.get("mixed_array") instanceof List

        def array = item.getList("mixed_array")
        array.get(0).equals("a")
        array.get(1).equals(new BigDecimal("123"))
        array.get(2).equals(false)
        array.get(3).equals(new BigDecimal("123.45"))
    }

    def "Loads Data With String Set Correctly"() {
        given:
        def json = """
                        {
                         "id": "123",
                         "string_array": ["a", "b", "c", "d"]
                         }
                   """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")

        item != null

        item.get("string_array") instanceof Set


        def stringSet = item.getStringSet("string_array")
        stringSet.contains("a")
        stringSet.contains("b")
        stringSet.contains("c")
        stringSet.contains("d")
    }

    def "Loads Data With Number Set Correctly"() {
        given:
        def json = """
                        {
                         "id": "123",
                         "number_array": [1, 2, 3, 4]
                         }
                   """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")

        item != null

        item.get("number_array") instanceof Set

        def numberSet = item.getNumberSet("number_array")
        numberSet.contains(new BigDecimal(1))
        numberSet.contains(new BigDecimal(2))
        numberSet.contains(new BigDecimal(3))
        numberSet.contains(new BigDecimal(4))
    }

    def "Loads Data With Nested Objects Correctly"() {
        given:
        def json = """
                        {
                         "id": "123",
                         "obj": {
                                    "string": "string_field",
                                    "number": 123,
                                    "boolean": true,
                                    "mixed_array": ["a", 123, true],
                                    "string_set": ["a", "b", "c"]
                                }
                         }
                   """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")

        item != null

        item.get("obj") instanceof Map

        def map = item.getMap("obj")
        map.get("string").equals("string_field")
        map.get("number").equals(new BigDecimal("123"))
        map.get("boolean").equals(true)

        map.get("mixed_array").get(0).equals("a")
        map.get("mixed_array").get(1).equals(new BigDecimal("123"))
        map.get("mixed_array").get(2).equals(true)

        map.get("string_set").contains("a")
        map.get("string_set").contains("b")
        map.get("string_set").contains("c")
    }

    def "Loads Data With Deeply Nested Objects Correctly"() {
        given:
        def json = """
                        {
                         "id": "123",
                         "obj": {
                                    "obj2": {
                                        "obj3": [{"id": 456}]
                                    }
                                }
                         }
                   """

        when:
        new LoadOperation(dynamoDB.getTable(TABLE_FOR_LOADING), mapper.readTree(json)).execute()

        then:
        def item = dynamoDB.getTable(TABLE_FOR_LOADING).getItem("id", "123")

        item != null
        item.get("obj") instanceof Map
        item.getMap("obj").get("obj2").get("obj3").get(0).get("id") == new BigDecimal("456")
    }

}
