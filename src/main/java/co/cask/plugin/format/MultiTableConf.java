package co.cask.plugin.format;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configuration for the {@link MultiTableDBInputFormat}.
 */
public class MultiTableConf extends PluginConfig {

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
  @Description("A schema name to list all the tables from. By default all the schemas will be used in the listing call.")
  private String schemaName;

  @Nullable
  @Description("The name of the field that holds the table name. " +
    "Must not be the name of any table column that will be read. Defaults to 'tablename'.")
  private String tableNameField;


  @Nullable
  @Description("List of tables to fetch from the database. By default all the tables will be white listed")
  private String whiteList;

  @Nullable
  @Description("List of tables NOT to fetch from the database. By default NONE of the tables will be black listed")
  private String blackList;

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
  public String getSchemaName() {
    return schemaName;
  }

  @Nullable
  public String getTableNameField() {
    return tableNameField;
  }

  public List<String> getWhiteList() {
    if (whiteList != null && !whiteList.isEmpty()) {
      return Arrays.asList(whiteList.split(","));
    }
    return Lists.newArrayList();
  }


  public List<String> getBlackList() {
    if (blackList != null && !blackList.isEmpty()) {
      return Arrays.asList(blackList.split(","));
    }
    return Lists.newArrayList();
  }

  /**
   * @return the JDBC Connection. Assumes the JDBC driver class has already been registered.
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    Connection conn = user == null ?
      DriverManager.getConnection(connectionString) : DriverManager.getConnection(connectionString, user, password);
    conn.setAutoCommit(enableAutoCommit);
    return conn;
  }
}
