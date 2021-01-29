/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.TransactionIsolationLevel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configuration for the {@link MultiTableDBInputFormat}.
 */
public class MultiTableConf extends PluginConfig {
  public static final String ERROR_HANDLING_SKIP_TABLE = "skip-table";
  public static final String ERROR_HANDLING_SEND_TO_ERROR_PORT = "send-to-error-port";
  public static final String ERROR_HANDLING_FAIL_PIPELINE = "fail-pipeline";

  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private String referenceName;

  @Macro
  @Description("JDBC connection string including database name. For example: jdbc:mysql://HOST/DATABASE.")
  private String connectionString;

  @Macro
  @Nullable
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  private String user;

  @Macro
  @Nullable
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  private String password;

  @Nullable
  @Description("Name of the JDBC plugin to use. This is the value of the 'name' key defined in the JSON file " +
    "for the JDBC plugin.")
  private String jdbcPluginName;

  @Macro
  @Nullable
  @Description("Whether to enable auto commit for queries run by this source. Defaults to false. " +
    "This setting should only matter if you are using a jdbc driver that does not support a false value for " +
    "auto commit, or a driver that does not support the commit call. For example, the Hive jdbc driver will throw " +
    "an exception whenever a commit is called. For drivers like that, this should be set to true.")
  private Boolean enableAutoCommit;

  @Macro
  @Nullable
  @Description("A schema name pattern to read all the tables. By default all the schemas will " +
    "be used in the listing call.")
  private String schemaNamePattern;

  @Macro
  @Nullable
  @Description("A pattern that defines which tables should be read from. " +
    "Any table whose name matches the pattern will read. If not specified, all tables will be read.")
  private String tableNamePattern;

  @Nullable
  @Description("The name of the field that holds the table name. " +
    "Must not be the name of any table column that will be read. Defaults to 'tablename'.")
  private String tableNameField;

  @Macro
  @Nullable
  @Description("The where clause.")
  private String whereClause;

  @Macro
  @Nullable
  @Description("List of tables to fetch from the database. By default all the tables will be white listed")
  private String whiteList;

  @Macro
  @Nullable
  @Description("List of tables NOT to fetch from the database. By default NONE of the tables will be black listed")
  private String blackList;

  @Macro
  @Nullable
  @Description("The number of splits per table to generate.")
  private Integer splitsPerTable;

  @Nullable
  @Description("The transaction isolation level for queries run by this sink. " +
    "Defaults to TRANSACTION_SERIALIZABLE. See java.sql.Connection#setTransactionIsolation for more details. " +
    "The Phoenix jdbc driver will throw an exception if the Phoenix database does not have transactions enabled " +
    "and this setting is set to true. For drivers like that, this should be set to TRANSACTION_NONE.")
  @Macro
  public String transactionIsolationLevel;

  @Nullable
  @Description("How to handle errors in table processing.")
  public String errorHandlingMode;

  @Nullable
  @Description("How many tables should experience errors before failing the pipeline when \"Fail Pipeline\" is selected " +
    "as the Error Handling Mode.")
  public Integer failedTableThreshold;

  @Nullable
  @Description("Query Timeout in Seconds.")
  public Integer queryTimeoutSeconds;

  public MultiTableConf() {
    enableAutoCommit = false;
    tableNameField = "tablename";
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  @Nullable
  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

  @Nullable
  public Boolean getEnableAutoCommit() {
    return enableAutoCommit;
  }

  @Nullable
  public String getTableNamePattern() {
    return tableNamePattern;
  }

  @Nullable
  public String getSchemaNamePattern() {
    return schemaNamePattern;
  }

  @Nullable
  public String getTableNameField() {
    return tableNameField;
  }

  @Nullable
  public Integer getSplitsPerTable() {
    return splitsPerTable;
  }

  @Nullable
  public String getWhereClause() {
    return whereClause;
  }

  @Nullable
  public String getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  public String getErrorHandlingMode() {
    return errorHandlingMode != null ? errorHandlingMode : ERROR_HANDLING_FAIL_PIPELINE;
  }

  public Integer getFailedTableThreshold() {
    return failedTableThreshold != null ? failedTableThreshold : 0;
  }

  public Integer getQueryTimeoutSeconds() {
    return queryTimeoutSeconds != null ? queryTimeoutSeconds : 600;
  }

  public List<String> getWhiteList() {
    if (whiteList != null && !whiteList.isEmpty()) {
      return Arrays.asList(whiteList.split(","));
    }
    return new ArrayList<>();
  }


  public List<String> getBlackList() {
    if (blackList != null && !blackList.isEmpty()) {
      return Arrays.asList(blackList.split(","));
    }
    return new ArrayList<>();
  }

  /**
   * @return the JDBC Connection. Assumes the JDBC driver class has already been registered.
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    Connection conn = user == null ?
      DriverManager.getConnection(connectionString) : DriverManager.getConnection(connectionString, user, password);
    conn.setAutoCommit(enableAutoCommit);
    conn.setTransactionIsolation(TransactionIsolationLevel.getLevel(transactionIsolationLevel));
    return conn;
  }
}
