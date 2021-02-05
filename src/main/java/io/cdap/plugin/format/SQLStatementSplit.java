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

import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.elasticsearch.common.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split representing data in a database table.
 */
public class SQLStatementSplit extends DBInputFormat.DBInputSplit {
  private String statementReferenceName;
  private String sqlStatement;
  private String tableAlias;
  private String fallbackTableName;

  // used by mapreduce
  public SQLStatementSplit() {
  }

  public SQLStatementSplit(String statementReferenceName,
                           String sqlStatement,
                           String tableAlias,
                           String fallbackTableName) {
    this.statementReferenceName = statementReferenceName;
    this.sqlStatement = sqlStatement;
    this.tableAlias = tableAlias;
    this.fallbackTableName = fallbackTableName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(statementReferenceName);
    out.writeUTF(sqlStatement);
    out.writeUTF(tableAlias);
    out.writeUTF(fallbackTableName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.statementReferenceName = in.readUTF();
    this.sqlStatement = in.readUTF();
    this.tableAlias = in.readUTF();
    this.fallbackTableName = in.readUTF();
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  @Override
  public long getLength() {
    return 0;
  }

  public String getSqlStatement() {
    return sqlStatement;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public String getFallbackTableName() {
    return fallbackTableName;
  }

  /**
   * Get a user-friendly statement identifier.
   *
   * If the user has specified a Table Alias, we use this value.
   * Otherwise, the StatementIdentifier is the position on the statement in the list of supplied
   * SQL statements.
   * @return the statement identifier.
   */
  public String getId() {
    if (!Strings.isNullOrEmpty(tableAlias)) {
      return tableAlias;
    }

    return statementReferenceName;
  }
}
