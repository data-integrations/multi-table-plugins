package io.cdap.plugin.format.error;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import javax.annotation.Nullable;

/**
 * TODO:add
 */
public class ErrorSchema {
  public static final String SCHEMA_NAME = "--io.cdap.cdap.api.data.schema.Schema.ErrorSchema";
  public static final String ERROR_MESSAGE = "error_message";
  public static final String TABLE_NAME = "table_name";
  public static final String EXCEPTION_CLASS_NAME = "exception_class_name";

  public static Schema getSchema() {
    return Schema.recordOf(
      SCHEMA_NAME,
      Schema.Field.of(ERROR_MESSAGE, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(TABLE_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(EXCEPTION_CLASS_NAME, Schema.of(Schema.Type.STRING))
    );
  }

  public static StructuredRecord errorRecord(String errorMessage,
                                             String exceptionClassName,
                                             @Nullable String tableName) {
    StructuredRecord.Builder builder = StructuredRecord.builder(ErrorSchema.getSchema());
    builder.set(ErrorSchema.ERROR_MESSAGE, errorMessage);
    builder.set(ErrorSchema.EXCEPTION_CLASS_NAME, exceptionClassName);
    builder.set(ErrorSchema.TABLE_NAME, tableName);
    return builder.build();
  }
}
