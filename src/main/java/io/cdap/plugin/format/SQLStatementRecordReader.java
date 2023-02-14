/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.DriverCleanup;
import io.cdap.plugin.common.db.DBUtils;
import io.cdap.plugin.format.error.collector.ErrorCollectingMultiSQLStatementInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Record reader that executes a supplied SQL statement against a database.
 */
public class SQLStatementRecordReader extends RecordReader<NullWritable, RecordWrapper> {
  private static final Logger LOG = LoggerFactory.getLogger(SQLStatementRecordReader.class);

  private final MultiTableConf dbConf;
  private final String tableNameField;
  private final DriverCleanup driverCleanup;
  private SQLStatementSplit split;
  private int pos;
  private ResultSetMetaData resultMeta;
  private List<Schema.Field> tableFields;
  private String tableName;
  private Schema schema;
  private Connection connection;
  private Statement statement;
  private ResultSet results;

  SQLStatementRecordReader(MultiTableConf dbConf,
                           String tableNameField,
                           DriverCleanup driverCleanup) {
    this.dbConf = dbConf;
    this.tableNameField = tableNameField;
    this.driverCleanup = driverCleanup;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    this.split = (SQLStatementSplit) split;
    this.pos = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    try {
      if (results == null) {
        connection = dbConf.getConnection();
        statement = connection.createStatement();
        if (dbConf.getQueryTimeoutSeconds() != null) {
          statement.setQueryTimeout(dbConf.getQueryTimeoutSeconds());
        }
        results = statement.executeQuery(split.getSqlStatement());
        resultMeta = results.getMetaData();
        tableName = getTableName(split, resultMeta);
        tableFields =
                DBUtils.getSchemaReader(connection.getMetaData().getDatabaseProductName(),
                                        BatchSource.PLUGIN_TYPE, null)
                        .getSchemaFields(results, null, null);
        List<Schema.Field> schemaFields = new ArrayList<>(tableFields);
        schemaFields.add(Schema.Field.of(tableNameField, Schema.of(Schema.Type.STRING)));
        schema = Schema.recordOf(tableName, schemaFields);
      }
      if (!results.next()) {
        return false;
      }

      pos++;
    } catch (SQLException e) {
      throw new IOException("SQLException in nextKeyValue", e);
    }
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public RecordWrapper getCurrentValue() throws IOException {
    try {
      String dbProductName = results.getStatement().getConnection().getMetaData().getDatabaseProductName();
      StructuredRecord.Builder recordBuilder =
              DBUtils.getRecordReaderHelper(dbProductName).getRecordBuilder(results, schema);
      recordBuilder.set(tableNameField, tableName);
      return new RecordWrapper(recordBuilder.build());
    } catch (SQLException e) {
      throw new IOException("Error decoding row from statement : '%s'", e);
    }
  }

  @Override
  public float getProgress() throws IOException {
    return pos / (float) split.getLength();
  }

  @Override
  public void close() throws IOException {
    SQLException exception = null;
    if (results != null) {
      try {
        results.close();
      } catch (SQLException e) {
        exception = e;
      }
    }

    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        if (exception != null) {
          exception.addSuppressed(e);
        } else {
          exception = e;
        }
      }
    }

    if (connection != null) {
      try {
        connection.commit();
        connection.close();
      } catch (SQLException e) {
        if (exception != null) {
          exception.addSuppressed(e);
        } else {
          exception = e;
        }
      }
    }

    driverCleanup.destroy();

    if (exception != null) {
      throw new IOException(exception);
    }
  }

  protected static String getTableName(SQLStatementSplit split, ResultSetMetaData resultMeta) throws SQLException {
    // Use table alias if available.
    if (!(split.getTableAlias() == null || split.getTableAlias().isEmpty())) {
      return split.getTableAlias();
    }

    try {
      // Try to determine table alias from the returned columns.
      // This feature might not be available with all SQL drivers.
      String metaTableName = buildTableName(resultMeta);
      if (!metaTableName.isEmpty()) {
        return metaTableName;
      }
    } catch (SQLException e) {
      LOG.warn("Unable to determine table names from query results. " +
                 "Use Table Aliases if possible.", e);
    }

    // Use the fallback if all else fails.
    return split.getFallbackTableName();
  }

  protected static String buildTableName(ResultSetMetaData resultMeta) throws SQLException {
    // LinkedHashSet is used to keep the order in which we encounter distinct tables in the result set.
    Set<String> set = new LinkedHashSet<>();

    for (int i = 1; i <= resultMeta.getColumnCount(); i++) {
      set.add(resultMeta.getTableName(i));
    }

    return String.join("_", set);
  }
}
