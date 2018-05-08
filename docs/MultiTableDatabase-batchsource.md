# MultiTableDatabase Batch Source

Description
-----------

Reads from multiple tables within a database using JDBC. Often used in conjunction with the DynamicMultiFileset sink
to perform dumps from multiple tables to HDFS files in a single pipeline. The source will output a record for each
row in the tables it reads, with each record containing an additional field that holds the name of the table the
record came from. In addition, for each table that will be read, this plugin will set pipeline arguments where the
key is 'multisink.[tablename]' and the value is the schema of the table. This is to make it work with the
DynamicMultiFileset.

Properties
----------

**referenceName**: This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**connectionString**: The JDBC connection string to the database. For example: jdbc:mysql://HOST/DATABASE.

**user**: The username to use when connecting to the database.

**password**: The password to use when connecting to the database.

**jdbcPluginName**: The name of the JDBC plugin to use.

**enableAutoCommit**: Whether to enable auto commit for queries run by this source. Defaults to false.
This setting should only matter if you are using a jdbc driver that does not support a false value for
auto commit, or a driver that does not support the commit call. For example, the Hive jdbc driver will throw
an exception whenever a commit is called. For drivers like that, this should be set to true.

**tableNamePattern**: A pattern that defines which tables should be read from.
Any table whose name matches the pattern will read. If not specified, all tables will be read.
Pattern syntax is specific to the type of database that is being connected to.

**schemaNamePattern**: A pattern that defines which schemas should be used to list the tables.
Any schema whose name matches the pattern will read. If not specified, all schema will be read.
Pattern syntax is specific to the type of database that is being connected to.

**tableNameField**: The name of the field that holds the table name.
Must not be the name of any table column that will be read. Defaults to 'tablename'.

**whiteList**: Used in conjunction with tableNamePattern, this configuration specifies tables to be read.
If no value is specified in the whiteList all tables matching the tableNamePattern will be read.
By default reads all tables matching the tableNamePattern.

**blackList**: Used in conjunction with tableNamePattern, this configuration specifies the tables to be skipped.
By default the black list is empty which means no tables will be skipped.

**dateFormat**: Format date, timestamp and time fields using the specified dateFormat.
By default Date fields in DB are converted to long.


Example
-------

This example reads from all tables in the 'customers' database on host 'host123.example.net':

    {
        "name": "MultiTableDatabase",
        "type": "batchsource",
        "properties": {
            "connectionString": "jdbc:mysql://host123.example.net/customers",
            "jdbcPluginName": "mysql"
        }
    }

Suppose you have two tables in the 'customers' database. The first table is named 'accounts' and contains:

    +-----+----------+------------------+
    | id  | name     | email            |
    +-----+----------+------------------+
    | 0   | Samuel   | sjax@example.net |
    | 1   | Alice    | a@example.net    |
    +-----+----------+------------------+

The second is named 'activity' and contains:

    +--------+----------+--------+
    | userid | item     | action |
    +--------+----------+--------+
    | 0      | shirt123 | view   |
    | 0      | carxyz   | view   |
    | 0      | shirt123 | buy    |
    | 0      | coffee   | view   |
    | 1      | cola     | buy    |
    +--------+----------+--------+

The output of the the source will be the following records:

    +-----+----------+------------------+-----------+
    | id  | name     | email            | tablename |
    +-----+----------+------------------+-----------+
    | 0   | Samuel   | sjax@example.net | accounts  |
    | 1   | Alice    | a@example.net    | accounts  |
    +-----+----------+------------------+-----------+
    +--------+----------+--------+-----------+
    | userid | item     | action | tablename |
    +--------+----------+--------+-----------+
    | 0      | shirt123 | view   | activity  |
    | 0      | carxyz   | view   | activity  |
    | 0      | shirt123 | buy    | activity  |
    | 0      | coffee   | view   | activity  |
    | 1      | cola     | buy    | activity  |
    +--------+----------+--------+-----------+
