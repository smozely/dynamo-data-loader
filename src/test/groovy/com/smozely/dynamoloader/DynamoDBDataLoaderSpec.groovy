package com.smozely.dynamoloader

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.michelboudreau.alternator.AlternatorDB
import com.michelboudreau.alternatorv2.AlternatorDBClientV2
import com.smozely.dynamoloader.testsupport.AlternatorSupport
import spock.lang.Specification

/**
 * Super simple test to show the loader hangs together, most testing is done on the internal classes.
 */
class DynamoDBDataLoaderSpec extends Specification {

    public static final String TABLE_WITH_HASH_KEY = "TEST_HASH_KEYED_TABLE"

    private AlternatorDB db
    private DynamoDB dynamoDB
    private AlternatorSupport support


    def setup() {
        db = new AlternatorDB().start()
        dynamoDB = new DynamoDB(new AlternatorDBClientV2())
        support = new AlternatorSupport(dynamoDB)
        makeSomeData()

    }

    def cleanup() {
        db.stop()
    }

    def makeSomeData() {
        support.createGenericTable(TABLE_WITH_HASH_KEY, "id")
    }

    def "Loader Executes Provided Operations"() {
        given:
        Table table = dynamoDB.getTable(TABLE_WITH_HASH_KEY)
        table.putItem(new Item().with("id", "123"))
        table.putItem(new Item().with("id", "456"))

        when:
        DynamoDBDataLoader
                .with(dynamoDB)
                .clear(TABLE_WITH_HASH_KEY)
                .execute()

        then:
        def scan = table.scan()
        scan.forEach({})
        scan.totalCount == 0
    }

}