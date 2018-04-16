/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.plugin.format;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.plugin.DriverCleanup;
import co.cask.plugin.Drivers;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Input format that reads from multiple tables in a database using JDBC. Similar to Hadoop's DBInputFormat.
 */
public class  MultiTableDBInputFormat extends InputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTableDBInputFormat.class);
  private static final String CONF_FIELD = "multi.db.plugin.conf";
  private static final String DRIVER_FIELD = "multi.db.jdbc.connection";
  private static final String SPLITS_FIELD = "multi.db.jdbc.splits";
  private static final Type SPLITS_TYPE = new TypeToken<List<DBTableSplit>>() { }.getType();
  private static final Gson GSON = new Gson();

  /**
   * Configure the input format to read tables from a database. Should be called from the mapreduce client.
   *
   * @param hConf the job configuration
   * @param dbConf the database conf
   * @param driverClass the JDBC driver class used to communicate with the database
   * @return Collection of TableInfo containing DB, table and schema.
   */
  public static Collection<TableInfo> setInput(Configuration hConf, MultiTableConf dbConf,
                                               Class<? extends Driver> driverClass)
    throws SQLException, InstantiationException, IllegalAccessException {

    hConf.set(CONF_FIELD, GSON.toJson(dbConf));
    hConf.set(DRIVER_FIELD, driverClass.getName());

    DriverCleanup cleanup = Drivers.ensureJDBCDriverIsAvailable(driverClass, dbConf.getConnectionString());

    try (Connection connection = dbConf.getConnection()) {
      DatabaseMetaData dbMeta = connection.getMetaData();
      ResultSet tables = dbMeta.getTables(null, dbConf.getSchemaNamePattern(), dbConf.getTableNamePattern(),
          new String[] {"TABLE", "TABLE_SCHEM"});
      Collection<TableInfo> tableInfos = new ArrayList<TableInfo>();
      List<DBTableSplit> splits = new ArrayList<>();
      List<String> whiteList = dbConf.getWhiteList();
      List<String> blackList = dbConf.getBlackList();
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        String db = tables.getString("TABLE_SCHEM");
        // If the table name exists in blacklist or when the whiteList is not empty and does not contain table name
        // the table should not be read
        if (!blackList.contains(tableName) && (whiteList.isEmpty() || whiteList.contains(tableName))) {
          long numRows = getTableRowCount(db, tableName, connection);
          Schema schema = getTableSchema(db, tableName, connection);
          tableInfos.add(new TableInfo(db, tableName, schema));
          splits.add(new DBTableSplit(db, tableName, numRows));
        }
      }
      hConf.set(SPLITS_FIELD, GSON.toJson(splits));
      return tableInfos;
    } finally {
      cleanup.destroy();
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    List<InputSplit> splits = GSON.fromJson(conf.get(SPLITS_FIELD), SPLITS_TYPE);
    return splits;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    DBTableSplit dbTableSplit = (DBTableSplit) split;
    MultiTableConf dbConf = GSON.fromJson(conf.get(CONF_FIELD), MultiTableConf.class);
    String driverClassname = conf.get(DRIVER_FIELD);
    try {
      Class<? extends Driver> driverClass = (Class<? extends Driver>) conf.getClassLoader().loadClass(driverClassname);
      DriverCleanup driverCleanup = Drivers.ensureJDBCDriverIsAvailable(driverClass, dbConf.getConnectionString());
      return new DBTableRecordReader(dbConf, dbTableSplit.getDb(), dbTableSplit.getTableName(),
          dbConf.getTableNameField(), driverCleanup);
    } catch (ClassNotFoundException e) {
      LOG.error("Could not load jdbc driver class {}", driverClassname);
      throw new IOException(e);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Could not register jdbc driver {}", driverClassname);
      throw new IOException(e);
    }
  }

  private static long getTableRowCount(String db, String tableName, Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + db + "." + tableName)) {
        results.next();
        return results.getLong(1);
      }
    }
  }

  private static Schema getTableSchema(String db, String tableName, Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet results = statement.executeQuery("SELECT * FROM " + db + "." + tableName + " WHERE 1 = 0")) {
        return Schema.recordOf(tableName, DBTypes.getSchemaFields(results));
      }
    }
  }

  public static class TableInfo {

    public String tableName;
    public String db;
    public Schema schema;

    public TableInfo(String db, String tableName, Schema schema) {
      this.db = db;
      this.tableName = tableName;
      this.schema = schema;
    }
  }
}