# Dynamo Data Loader
Java Library to Load Data into Dynamo DB. Perfect for seeding data for integration tests.

## Getting
Versions of this module are being published to BinTray
[ ![Download](https://api.bintray.com/packages/smozely/maven/dynamo-data-loader/images/download.svg) ](https://bintray.com/smozely/maven/dynamo-data-loader/_latestVersion)

## Building
To build and use locally, clone this repo, compile/test with gradle and install to your local maven.
`./gradlew build publishToMavenLocal`

## Usage
Use the fluent API from the `DynamoDBDataLoader` class 

For example clearing and loading data into tables "table1" and table2.

```
    private DynamoDB dynamoDB;
    private Table table2;
    
    ...
    
    DynamoDBDataLoader
            .with(dynamoDB)
            .clear("table1")
            .clear(table2)
            .load("table1", new File("table1.json"))
            .load(table2,   new File("table2.json"))
            .execute();
```
JSON files can have either a Object (for a single item) or a Array (for multiple items) as the top level element. 

e.g.
```
    [
        {"id": "SOME-ID1"},
        {"id": "SOME-ID2", "number_field": 1234, "boolean_field": false}
    ]
```

## Limitations
... Write about List vs Set handling here ...

## Completed Features
* Clear Tables
* Load simple JSON structure  
* Load JSON which includes Arrays

## TODO
* Load JSON which includes Sets  
* Load JSON which includes Objects
* Support templated string values i.e. `${some_value}`