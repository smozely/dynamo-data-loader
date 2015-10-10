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

    public static final String TABLE1 = "TABLE1"
    public static final String TABLE2 = "TABLE2"

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