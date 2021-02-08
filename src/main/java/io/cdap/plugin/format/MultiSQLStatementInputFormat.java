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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.plugin.DriverCleanup;
import io.cdap.plugin.Drivers;
import io.cdap.plugin.format.error.collector.ErrorCollectingRecordReader;
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
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Input format that reads from multiple tables in a database using JDBC. Similar to Hadoop's DBInputFormat.
 */
public class MultiSQLStatementInputFormat extends InputFormat<NullWritable, RecordWrapper> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiSQLStatementInputFormat.class);
  private static final String STATEMENT_REFERENCE_NAME_FORMAT = "Statement #%d";
  private static final String FALLBACK_TABLE_NAME_FORMAT = "sql_statement_%d";

  /**
   * Configure the input format to read tables from a database. Should be called from the mapreduce client.
   *
   * @param hConf       the job configuration
   * @param dbConf      the database conf
   * @param driverClass the JDBC driver class used to communicate with the database
   */
  public static void setInput(Configuration hConf, MultiTableConf dbConf,
                              Class<? extends Driver> driverClass) {
    MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(hConf);
    multiTableDBConf.setPluginConfiguration(dbConf);
    multiTableDBConf.setDriver(driverClass.getName());
    multiTableDBConf.setSqlStatements(dbConf.getSqlStatements());
    multiTableDBConf.setTableAliases(dbConf.getTableAliases());
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    MultiTableDBConfiguration conf = new MultiTableDBConfiguration(context.getConfiguration());

    return getSplits(conf);
  }

  @VisibleForTesting
  protected List<InputSplit> getSplits(MultiTableDBConfiguration conf) {
    List<String> sqlStatements = conf.getSqlStatements();
    List<String> tableAliases = conf.getTableAliases();
    List<InputSplit> resultSplits = new ArrayList<>();

    //Handle the case where there are not enough table aliases for all SQL statements.
    //In this case, we use as many aliases as possible and then fall back to the default logiv.
    if (tableAliases.size() < sqlStatements.size()) {
      tableAliases = new LinkedList<>(tableAliases);
      while (tableAliases.size() < sqlStatements.size()) {
        tableAliases.add("");
      }
    }


    for (int i = 0; i < sqlStatements.size(); i++) {
      SQLStatementSplit split = new SQLStatementSplit(String.format(STATEMENT_REFERENCE_NAME_FORMAT, i + 1),
                                                      sqlStatements.get(i),
                                                      tableAliases.get(i),
                                                      String.format(FALLBACK_TABLE_NAME_FORMAT, i + 1));
      resultSplits.add(split);
    }

    return resultSplits;
  }

  @Override
  public RecordReader<NullWritable, RecordWrapper> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(context.getConfiguration());
    MultiTableConf dbConf = multiTableDBConf.getPluginConf();
    String driverClassname = multiTableDBConf.getDriverName();
    SQLStatementSplit sqlStatementSplit = (SQLStatementSplit) split;
    try {
      Class<? extends Driver> driverClass = (Class<? extends Driver>)
        multiTableDBConf.getConf().getClassLoader().loadClass(driverClassname);
      DriverCleanup driverCleanup = Drivers.ensureJDBCDriverIsAvailable(driverClass, dbConf.getConnectionString());
      return new ErrorCollectingRecordReader(
        multiTableDBConf.getPluginConf().getReferenceName(),
        new SQLStatementRecordReader(dbConf,
                                     dbConf.getTableNameField(),
                                     driverCleanup),
        sqlStatementSplit.getTableAlias());
    } catch (ClassNotFoundException e) {
      LOG.error("Could not load jdbc driver class {}", driverClassname);
      throw new IOException(e);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Could not register jdbc driver {}", driverClassname);
      throw new IOException(e);
    }
  }
}
