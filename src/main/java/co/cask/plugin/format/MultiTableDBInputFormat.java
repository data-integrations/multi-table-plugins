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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Input format that reads from multiple tables in a database using JDBC. Similar to Hadoop's DBInputFormat.
 */
public class MultiTableDBInputFormat extends InputFormat<NullWritable, StructuredRecord> {
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
   * @return mapping from table name to table schema for all tables that will be read
   */
  public static Map<String, Schema> setInput(Configuration hConf, MultiTableConf dbConf,
                                             Class<? extends Driver> driverClass)
    throws SQLException, InstantiationException, IllegalAccessException {

    hConf.set(CONF_FIELD, GSON.toJson(dbConf));
    hConf.set(DRIVER_FIELD, driverClass.getName());

    DriverCleanup cleanup = Drivers.ensureJDBCDriverIsAvailable(driverClass, dbConf.getConnectionString());

    try (Connection connection = dbConf.getConnection()) {
      DatabaseMetaData dbMeta = connection.getMetaData();
      ResultSet tables = dbMeta.getTables(null, null, dbConf.getTableNamePattern(), new String[] {"TABLE"});
      Map<String, Schema> tableSchemas = new HashMap<>();
      List<DBTableSplit> splits = new ArrayList<>();
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        long numRows = getTableRowCount(tableName, connection);
        Schema schema = getTableSchema(tableName, connection);
        tableSchemas.put(tableName, schema);
        splits.add(new DBTableSplit(tableName, numRows));
      }
      hConf.set(SPLITS_FIELD, GSON.toJson(splits));
      return tableSchemas;
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
      return new DBTableRecordReader(dbConf, dbTableSplit.getTableName(), dbConf.getTableNameField(), driverCleanup);
    } catch (ClassNotFoundException e) {
      LOG.error("Could not load jdbc driver class {}", driverClassname);
      throw new IOException(e);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Could not register jdbc driver {}", driverClassname);
      throw new IOException(e);
    }
  }

  private static long getTableRowCount(String tableName, Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
        results.next();
        return results.getLong(1);
      }
    }
  }

  private static Schema getTableSchema(String tableName, Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet results = statement.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0")) {
        return Schema.recordOf(tableName, DBTypes.getSchemaFields(results));
      }
    }
  }
}