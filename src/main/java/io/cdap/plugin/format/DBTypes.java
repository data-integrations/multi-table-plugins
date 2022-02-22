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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Methods for converting database types and values
 */
public class DBTypes {

  /**
   * Given the result set, get the metadata of the result set and return
   * list of {@link io.cdap.cdap.api.data.schema.Schema.Field},
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
      Schema columnSchema = getSchema(metadata, i);
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  /**
   * Given the metadata and column number, return the Schema type corresponding to the SQL type.
   *
   * Note that the BIGINT SQL type can be signed or unsigned. An unsigned BIGINT will be mapped to a Decimal type while
   * a signed BIGINT will be mapped to a Long. This distinction has to be made for backward compatibility reasons.
   *
   * @throws SQLException for unsupported types.
   */
  private static Schema getSchema(ResultSetMetaData metadata, int column) throws SQLException {
    int sqlType = metadata.getColumnType(column);
    String typeName = metadata.getColumnTypeName(column);
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
        if (typeName.toLowerCase().contains("unsigned")) {
          return getDecimalSafe(metadata, column, typeName);
        } else {
          return Schema.of(Schema.Type.LONG);
        }

      case Types.REAL:
      case Types.FLOAT:
        return Schema.of(Schema.Type.FLOAT);

      case Types.NUMERIC:
      case Types.DECIMAL:
        return getDecimalSafe(metadata, column, typeName);
      case Types.DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);

      case Types.DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case Types.TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case Types.TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

      case Types.VARCHAR:
      case Types.CHAR:
      case Types.CLOB:
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NCLOB:
      case Types.NVARCHAR:
        return Schema.of(Schema.Type.STRING);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        return Schema.of(Schema.Type.BYTES);

      // Unsupported Types: ARRAY, DATALINK, DISTINCT, JAVA_OBJECT, OTHER, REF, ROWID, SQLXML, STRUCT
      default:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType + " for column '"
                                                              + metadata.getColumnName(column) + "' with type '"
                                                              + metadata.getColumnTypeName(column) + "'"));
    }
  }

  private static Schema getDecimalSafe(ResultSetMetaData metadata, int column, String typeName) throws SQLException {
    int precision = metadata.getPrecision(column);
    int scale = metadata.getScale(column);
    // decimal type with precision 0 is not supported
    if (precision == 0) {
      throw new SQLException(new UnsupportedTypeException(
          String.format("Column %s has unsupported SQL Type: '%s' with precision: '%s'", column,
              typeName, precision)));
    }
    return Schema.decimalOf(precision, scale);
  }

  public static StructuredRecord.Builder setValue(StructuredRecord.Builder record, int sqlColumnType,
                                                  ResultSet resultSet, String fieldName,
                                                  int precision, int scale) throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlColumnType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return record.set(fieldName, ((Number) original).intValue());
        case Types.NUMERIC:
        case Types.DECIMAL:
          return record.setDecimal(fieldName, resultSet.getBigDecimal(fieldName, scale));
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
