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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Information about a DB table.
 */
public class DBTableInfo {
  private final DBTableName dbTableName;
  private final Schema schema;
  private final List<String> primaryKey;

  public DBTableInfo(DBTableName dbTableName, Schema schema, List<String> primaryKey) {
    this.dbTableName = dbTableName;
    this.schema = schema;
    this.primaryKey = new ArrayList<>(primaryKey);
  }

  public DBTableName getDbTableName() {
    return dbTableName;
  }

  public Schema getSchema() {
    return schema;
  }

  public List<String> getPrimaryKey() {
    return Collections.unmodifiableList(primaryKey);
  }
}
