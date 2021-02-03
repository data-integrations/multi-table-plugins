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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split representing data in a database table.
 */
public class SQLStatementSplit extends DBInputFormat.DBInputSplit {
  private String statementId;
  private String sqlStatement;

  // used by mapreduce
  public SQLStatementSplit() {
  }

  public SQLStatementSplit(String statementName, String sqlStatement) {
    this.statementId = statementName;
    this.sqlStatement = sqlStatement;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(statementId);
    out.writeUTF(sqlStatement);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.statementId = in.readUTF();
    this.sqlStatement = in.readUTF();
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  @Override
  public long getLength() {
    return 0;
  }

  public String getStatementId() {
    return statementId;
  }

  public String getSqlStatement() {
    return sqlStatement;
  }
}
