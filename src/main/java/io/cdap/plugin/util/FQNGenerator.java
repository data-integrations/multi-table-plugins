/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.util;

import io.opentracing.contrib.jdbc.ConnectionInfo;
import io.opentracing.contrib.jdbc.parser.URLParser;

/**
 * Generate FQN from DB URL Connection.
 * TODO: CDAP-20456 Reuse the class from hydrator/database-plugins module
 */
public final class FQNGenerator {

  private static final String POSTGRESQL_TAG = "postgresql";
  private static final String POSTGRESQL_DEFAULT_SCHEMA = "public";

  private FQNGenerator() { }

  public static String constructFQN(String jdbcUrl, String tableName) {
    // dbtype, host, port, db from the connection string
    // table is the reference name
    ConnectionInfo connectionInfo = URLParser.parse(jdbcUrl);
    // DB type as set by library after extraction
    if (POSTGRESQL_TAG.equals(connectionInfo.getDbType())) {
      // FQN for Postgresql
      return String.format("%s://%s/%s.%s.%s", connectionInfo.getDbType(), connectionInfo.getDbPeer(),
                           connectionInfo.getDbInstance(), getPostgresqlSchema(jdbcUrl), tableName);
    } else {
      // FQN for MySQL, Oracle, SQLServer
      return String.format("%s://%s/%s.%s", connectionInfo.getDbType(), connectionInfo.getDbPeer(),
                           connectionInfo.getDbInstance(), tableName);
    }
  }

  private static String getPostgresqlSchema(String url) {
    /**
     * Extract schema for PostgresSQL URL strings which can be of the following formats
     * jdbc:postgresql://{host}:{port}/{db}?currentSchema={schema}
     * jdbc:postgresql://{host}:{port}/{db}?searchpath={schema}
     */
    String dbSchema;
    int offset =  0;
    int startIndex = url.indexOf("connectionSchema=");
    offset = 17;
    if (startIndex == -1) {
      startIndex = url.indexOf("searchpath=");
      offset = 11;
    }

    int endIndex = url.indexOf("&", startIndex);
    if (endIndex == -1) {
      endIndex = url.length();
    }
    if (startIndex != -1 && endIndex != -1 && startIndex <= endIndex) {
      dbSchema = url.substring(startIndex + offset, endIndex);
    } else {
      dbSchema = POSTGRESQL_DEFAULT_SCHEMA;
    }
    return dbSchema;
  }
}
