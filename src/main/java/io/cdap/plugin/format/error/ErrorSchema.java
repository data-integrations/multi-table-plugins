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

package io.cdap.plugin.format.error;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.plugin.format.RecordWrapper;

import javax.annotation.Nullable;

/**
 * Definition of the Schema used to represent errors when reading tables or establishing database connections.
 */
public class ErrorSchema {
  public static final String SCHEMA_NAME = "multi_db_source_error";
  public static final String ERROR_MESSAGE = "error_message";
  public static final String TABLE_NAME = "table_name";
  public static final String EXCEPTION_CLASS_NAME = "exception_class_name";

  public static Schema getSchema() {
    return Schema.recordOf(
      SCHEMA_NAME,
      Schema.Field.of(ERROR_MESSAGE, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(EXCEPTION_CLASS_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(TABLE_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
  }

  public static RecordWrapper errorRecordWrapper(String errorMessage,
                                                 String exceptionClassName,
                                                 @Nullable String tableName) {
    StructuredRecord.Builder builder = StructuredRecord.builder(ErrorSchema.getSchema());
    builder.set(ErrorSchema.ERROR_MESSAGE, errorMessage);
    builder.set(ErrorSchema.EXCEPTION_CLASS_NAME, exceptionClassName);
    builder.set(ErrorSchema.TABLE_NAME, tableName);
    return new RecordWrapper(new InvalidEntry<>(0, errorMessage, builder.build()));
  }
}
