# DynamicMultiFileset Batch Sink

Description
-----------

This plugin is normally used in conjunction with the MultiTableDatabase batch source to write records from multiple
databases into multiple filesets in text format. Each fileset it writes to will contain a single 'ingesttime' partition,
which will contain the logical start time of the pipeline run. The plugin expects that the filsets it needs to write
to will be set as pipeline arguments, where the key is 'multisink.[fileset]' and the value is the fileset schema.
Normally, you rely on the MultiTableDatabase source to set those pipeline arguments, but they can also be manually
set or set by an Action plugin in your pipeline. The sink will expect each record to contain a special split field
that will be used to determine which records are written to each fileset. For example, suppose the
the split field is 'tablename'. A record whose 'tablename' field is set to 'activity' will be written to the 'activity'
fileset.

Properties
----------

**splitField:** The name of the field that will be used to determine which fileset to write to. Defaults to 'tablename'.

**delimiter:** The delimiter used to separate record fields. Defaults to the tab character..

Example
-------

This example uses a comma to delimit record fields:

    {
        "name": "DynamicMultiFileset",
        "type": "batchsink",
        "properties": {
            "delimiter": ","
        }
    }

Suppose the input records are:

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

The plugin will expect two pipeline arguments to tell it to write the first two records to an 'accounts' fileset and
the last records to an 'activity' fileset:

    multisink.accounts =
     {
       "type": "record",
       "name": "accounts",
       "fields": [
         { "name": "id", "type": "long" } ,
         { "name": "name", "type": "string" },
         { "name": "email", "type": [ "string", "null" ] }
       ]
     }
    multisink.activity =
     {
       "type": "record",
       "name": "activity",
       "fields": [
         { "name": "userid", "type": "long" } ,
         { "name": "item", "type": "string" },
         { "name": "action", "type": "string" }
       ]
     }
