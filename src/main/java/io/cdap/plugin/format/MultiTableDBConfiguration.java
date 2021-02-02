package io.cdap.plugin.format;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Allows to specify and access connection configuration properties of {@link Configuration}.
 */
public class MultiTableDBConfiguration extends DBConfiguration {
  private static final String PLUGIN_CONF_FIELD = "multi.db.plugin.conf";
  private static final String DRIVER_FIELD = "multi.db.jdbc.connection";
  private static final String INFO_FIELD = "multi.db.jdbc.dbinfo";
  private static final String SQL_STATEMENTS_FIELD = "multi.db.jdbc.sql_statements";

  private static final Type PLUGIN_CONF_TYPE = new TypeToken<MultiTableConf>() { }.getType();
  private static final Type INFO_TYPE = new TypeToken<List<DBTableInfo>>() { }.getType();
  private static final Type SQL_STATEMENTS_TYPE = new TypeToken<List<String>>() { }.getType();

  private static final Gson GSON = new Gson();

  public MultiTableDBConfiguration(Configuration job) {
    super(job);
  }

  public void setPluginConfiguration(MultiTableConf conf) {
    set(PLUGIN_CONF_FIELD, GSON.toJson(conf));
  }

  public void setDriver(String value) {
    set(DRIVER_FIELD, value);
  }

  public void setTableInfos(List<DBTableInfo> infoList) {
    set(INFO_FIELD, GSON.toJson(infoList));
  }

  public void setSqlStatements(List<String> sqlStatements) {
    set(SQL_STATEMENTS_FIELD, GSON.toJson(sqlStatements));
  }

  public MultiTableConf getPluginConf() {
    return GSON.fromJson(getConf().get(PLUGIN_CONF_FIELD), PLUGIN_CONF_TYPE);
  }

  public String getDriverName() {
    return getConf().get(DRIVER_FIELD);
  }

  public List<DBTableInfo> getTableInfos() {
    return GSON.fromJson(getConf().get(INFO_FIELD), INFO_TYPE);
  }

  public List<String> getSqlStatements() {
    return GSON.fromJson(getConf().get(SQL_STATEMENTS_FIELD), SQL_STATEMENTS_TYPE);
  }

  private void set(String key, String value) {
    getConf().set(key, value);
  }
}
