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
  public static final String PLUGIN_CONF_FIELD = "multi.db.plugin.conf";
  public static final String DRIVER_FIELD = "multi.db.jdbc.connection";
  public static final String INFO_FIELD = "multi.db.jdbc.dbinfo";
  public static final String SQL_STATEMENTS_FIELD = "multi.db.jdbc.custom_sql.sql_statements";
  public static final String TABLE_ALIASES_FIELD = "multi.db.jdbc.custom_sql.table_aliases";

  private static final Type PLUGIN_CONF_TYPE = new TypeToken<MultiTableConf>() { }.getType();
  private static final Type INFO_TYPE = new TypeToken<List<DBTableInfo>>() { }.getType();
  private static final Type SQL_STATEMENTS_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Type TABLE_ALIASES_TYPE = new TypeToken<List<String>>() { }.getType();

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

  public void setTableAliases(List<String> tableAliases) {
    set(TABLE_ALIASES_FIELD, GSON.toJson(tableAliases));
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

  public List<String> getTableAliases() {
    return GSON.fromJson(getConf().get(TABLE_ALIASES_FIELD), TABLE_ALIASES_TYPE);
  }

  private void set(String key, String value) {
    getConf().set(key, value);
  }
}
