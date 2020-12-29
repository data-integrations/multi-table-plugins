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

**Reference Name**: This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**JDBC Connection String**: The JDBC connection string to the database. For example: jdbc:mysql://HOST/DATABASE.

**Database User Name**: The username to use when connecting to the database.

**Database User Password**: The password to use when connecting to the database.

**JDBC Plugin Name**: The name of the JDBC plugin to use.

**Enable Auto Commit**: Whether to enable auto commit for queries run by this source. Defaults to false.
This setting should only matter if you are using a jdbc driver that does not support a false value for
auto commit, or a driver that does not support the commit call. For example, the Hive jdbc driver will throw
an exception whenever a commit is called. For drivers like that, this should be set to true.

**Table Name Pattern**: A pattern that defines which tables should be read from.
Any table whose name matches the pattern will read. If not specified, all tables will be read.
Pattern syntax is specific to the type of database that is being connected to.

**Where Clause**: Filters which records needs to be consumed from each table: 
i.e. ```where updated_at > '2018-08-20 00:00:00'```.
The ```where``` clause will be applied to every table that is being read. Therefore, all the columns that are mentioned 
in the ```where``` clause should be present in each table.

**Schema Name Pattern**: A pattern that defines which schemas should be used to list the tables.
Any schema whose name matches the pattern will read. If not specified, all schema will be read.
Pattern syntax is specific to the type of database that is being connected to.

**Table Name Field**: The name of the field that holds the table name.
Must not be the name of any table column that will be read. Defaults to 'tablename'.

**White List of Table Names**: Used in conjunction with tableNamePattern, this configuration specifies tables to be read.
If no value is specified in the whiteList all tables matching the tableNamePattern will be read.
By default reads all tables matching the tableNamePattern.

**Black List of Table Names**: Used in conjunction with tableNamePattern, this configuration specifies the tables to be skipped.
By default the black list is empty which means no tables will be skipped.

**Splits Per Table**: The number of splits per table. By default is 1.

**Transaction Isolation Level:** The transaction isolation level for queries run by this sink.
Defaults to TRANSACTION_SERIALIZABLE. See java.sql.Connection#setTransactionIsolation for more details.
The Phoenix jdbc driver will throw an exception if the Phoenix database does not have transactions enabled
and this setting is set to true. For drivers like that, this should be set to TRANSACTION_NONE.

Example
-------

This example reads from all tables in the 'customers' database on host 'host123.example.net':

    {
        "name": "MultiTableDatabase",
        "type": "batchsource",
        "properties": {
            "connectionString": "jdbc:mysql://host123.example.net/customers",
            "jdbcPluginName": "mysql",
            "splitsPerTable": "2"
        }
    }

Suppose you have two tables in the 'customers' database, where `ID` column is the primary key in both tables. 
The first table is named 'accounts' and contains:

    +-----+----------+------------------+
    | ID  | name     | email            |
    +-----+----------+------------------+
    | 0   | Samuel   | sjax@example.net |
    | 1   | Alice    | a@example.net    |
    | 2   | Bob      | b@example.net    |
    | 3   | John     | j@example.net    |
    +-----+----------+------------------+

The second is named 'activity' and contains:

    +-----+--------+----------+--------+
    | ID  | userid | item     | action |
    +-----+--------+----------+--------+
    | 0   | 0      | shirt123 | view   |
    | 1   | 0      | carxyz   | view   |
    | 2   | 0      | shirt123 | buy    |
    | 3   | 0      | coffee   | view   |
    | 4   | 1      | cola     | buy    |
    | 5   | 1      | pepsi    | buy    |
    +-----+--------+----------+--------+

You will have 4 splits (2 per each table) with such queries:

    SELECT * FROM accounts WHERE ( ID >= 0 ) AND ( ID < 1 )
    SELECT * FROM accounts WHERE ( ID >= 2 ) AND ( ID <= 3 )
    SELECT * FROM activity WHERE ( ID >= 0 ) AND ( ID < 3 )
    SELECT * FROM activity WHERE ( ID >= 3 ) AND ( ID <= 5 )

The output of the the source will be the following records:

    +-----+----------+------------------+-----------+
    | ID  | name     | email            | tablename |
    +-----+----------+------------------+-----------+
    | 0   | Samuel   | sjax@example.net | accounts  |
    | 1   | Alice    | a@example.net    | accounts  |
    | 2   | Bob      | b@example.net    | accounts  |
    | 3   | John     | j@example.net    | accounts  |
    +-----+----------+------------------+-----------+
    +-----+--------+----------+--------+-----------+
    | ID  | userid | item     | action | tablename |
    +-----+--------+----------+--------+-----------+
    | 0   | 0      | shirt123 | view   | activity  |
    | 1   | 0      | carxyz   | view   | activity  |
    | 2   | 0      | shirt123 | buy    | activity  |
    | 3   | 0      | coffee   | view   | activity  |
    | 4   | 1      | cola     | buy    | activity  |
    | 5   | 1      | pepsi    | buy    | activity  |
    +-----+--------+----------+--------+-----------+
