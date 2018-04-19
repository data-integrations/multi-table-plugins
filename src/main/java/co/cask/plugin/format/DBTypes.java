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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
   * @param resultSet result set of executed query
   * @param convertDate if set to true return String type for date fields
   * @return list of schema fields
   * @throws SQLException
   */
  public static List<Schema.Field> getSchemaFields(ResultSet resultSet, boolean convertDate) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      int columnSqlType = metadata.getColumnType(i);
      Schema columnSchema = Schema.of(getType(columnSqlType, convertDate));
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  // given a sql type return schema type
  private static Schema.Type getType(int sqlType, boolean convertDate) throws SQLException {
    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        type = Schema.Type.INT;
        break;

      case Types.BIGINT:
        type = Schema.Type.LONG;
        break;

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        if (convertDate) {
          type = Schema.Type.STRING;
        } else {
          type = Schema.Type.LONG;
        }
        break;

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

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

    return type;
  }

  private static String formatDate(long time, String format) {
		DateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(new Date(time));
	}

  @Nullable
  public static Object transformValue(int sqlColumnType, ResultSet resultSet, String fieldName, String dateFormat)
			throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlColumnType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) original).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL:
          return ((BigDecimal) original).doubleValue();
        case Types.DATE:
					return dateFormat == null ? resultSet.getDate(fieldName).getTime() :
							formatDate(resultSet.getDate(fieldName).getTime(), dateFormat);
        case Types.TIME:
					return dateFormat == null ? resultSet.getTime(fieldName).getTime() :
							formatDate(resultSet.getTime(fieldName).getTime(), dateFormat);
        case Types.TIMESTAMP:
        	return dateFormat == null ? resultSet.getTimestamp(fieldName).getTime() :
							formatDate(resultSet.getTimestamp(fieldName).getTime(), dateFormat);
        case Types.BLOB:
          Blob blob = (Blob) original;
          try {
            return blob.getBytes(1, (int) blob.length());
          } finally {
            blob.free();
          }
        case Types.CLOB:
          Clob clob = (Clob) original;
          try {
            return clob.getSubString(1, (int) clob.length());
          } finally {
            clob.free();
          }
      }
    }
    return original;
  }

}
