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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Methods for converting database types and values
 */
public class DBTypes {

  /**
   * Given the result set, get the metadata of the result set and return
   * list of {@link co.cask.cdap.api.data.schema.Schema.Field},
   * where name of the field is same as column name and type of the field is obtained using {@link DBTypes#getType(int)}
   *
   * @param resultSet   result set of executed query
   * @return list of schema fields
   * @throws SQLException
   */
  public static List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      int columnSqlType = metadata.getColumnType(i);
      Schema columnSchema = getSchema(columnSqlType);
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  // given a sql type return schema type
  private static Schema getSchema(int sqlType) throws SQLException {
    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema schema = Schema.of(Schema.Type.STRING);
    switch (sqlType) {
      case Types.NULL:
        return Schema.of(Schema.Type.NULL);

      case Types.BOOLEAN:
      case Types.BIT:
        return Schema.of(Schema.Type.BOOLEAN);

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        return Schema.of(Schema.Type.INT);

      case Types.BIGINT:
        return Schema.of(Schema.Type.LONG);

      case Types.REAL:
      case Types.FLOAT:
        return Schema.of(Schema.Type.FLOAT);

      case Types.NUMERIC:
      case Types.DECIMAL:
      case Types.DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);

      case Types.DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case Types.TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case Types.TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        return Schema.of(Schema.Type.BYTES);

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.ROWID:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return schema;
  }

  public static StructuredRecord.Builder setValue(StructuredRecord.Builder record, int sqlColumnType,
                                                  ResultSet resultSet, String fieldName) throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlColumnType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return record.set(fieldName, ((Number) original).intValue());
        case Types.NUMERIC:
        case Types.DECIMAL:
          return record.set(fieldName, ((BigDecimal) original).doubleValue());
        case Types.DATE:
          return record.setDate(fieldName, resultSet.getDate(fieldName).toLocalDate());
        case Types.TIME:
          return record.setTime(fieldName, resultSet.getTime(fieldName).toLocalTime());
        case Types.TIMESTAMP:
          Instant instant = resultSet.getTimestamp(fieldName).toInstant();
          return record.setTimestamp(fieldName, instant.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
        case Types.BLOB:
          Blob blob = (Blob) original;
          try {
            return record.set(fieldName, blob.getBytes(1, (int) blob.length()));
          } finally {
            blob.free();
          }
        case Types.CLOB:
          Clob clob = (Clob) original;
          try {
            return record.set(fieldName, clob.getSubString(1, (int) clob.length()));
          } finally {
            clob.free();
          }
      }
    }
    return record.set(fieldName, original);
  }
}
