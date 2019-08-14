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

package io.cdap.plugin.format;

import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split representing data in a database table.
 */
public class DBTableSplit extends DataDrivenDBInputFormat.DataDrivenDBInputSplit {
  private static final String DEFAULT_CLAUSE = "1=1";

  private DBTableName tableName;

  // used by mapreduce
  public DBTableSplit() {
  }

  public DBTableSplit(DBTableName tableName) {
    this(tableName, DEFAULT_CLAUSE, DEFAULT_CLAUSE);
  }

  public DBTableSplit(DBTableName tableName, String lower, String upper) {
    super(getClauseOrDefault(lower), getClauseOrDefault(upper));
    this.tableName = tableName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (tableName.getDb() == null) {
      out.writeUTF("");
    } else {
      out.writeUTF(tableName.getDb());
    }
    out.writeUTF(tableName.getTable());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    String db = in.readUTF();
    if (db.isEmpty()) {
      db = null;
    }
    String table = in.readUTF();
    tableName = new DBTableName(db, table);
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public DBTableName getTableName() {
    return tableName;
  }

  public String getWhereClause() {
    return String.format("(( %s ) AND ( %s ))", getClauseOrDefault(getLowerClause()),
                         getClauseOrDefault(getUpperClause()));
  }

  private static String getClauseOrDefault(String value) {
    return value != null ? value : DEFAULT_CLAUSE;
  }
}
