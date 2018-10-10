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

package co.cask.plugin.format;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split representing data in a database table.
 */
public class DBTableSplit extends InputSplit implements Writable {
  private String tableName;
  private long length;

  // used by mapreduce
  public DBTableSplit() {
  }

  public DBTableSplit(String tableName, long length) {
    this.tableName = tableName;
    this.length = length;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeUTF(tableName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    length = in.readLong();
    tableName = in.readUTF();
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public String getTableName() {
    return tableName;
  }

}
