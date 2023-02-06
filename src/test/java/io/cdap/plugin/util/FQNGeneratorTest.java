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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for FQNGenerator.
 */
public class FQNGeneratorTest {

  @Test
  public void testMySQLFQN() {
    // Testcases consist of Connection URL, Table Name, Expected FQN String
    String[][] testCases  = {{"jdbc:mysql://localhost:1111/db", "table1", "mysql://localhost:1111/db.table1"},
      {"jdbc:mysql://34.35.36.37/db?useSSL=false", "table2",
        "mysql://34.35.36.37:3306/db.table2"}};
    for (int i = 0; i < testCases.length; i++) {
      String fqn = FQNGenerator.constructFQN(testCases[i][0], testCases[i][1]);
      Assert.assertEquals(testCases[i][2], fqn);
    }
  }

  @Test
  public void testSQLServerFQN() {
    // Testcases consist of Connection URL, Table Name, Expected FQN String
    String[][] testCases  = {{"jdbc:sqlserver://;serverName=127.0.0.1;databaseName=DB",
      "table1", "sqlserver://127.0.0.1:1433/DB.table1"},
      {"jdbc:sqlserver://localhost:1111;databaseName=DB;encrypt=true;user=user;password=pwd;",
        "table2", "sqlserver://localhost:1111/DB.table2"}};
    for (int i = 0; i < testCases.length; i++) {
      String fqn = FQNGenerator.constructFQN(testCases[i][0], testCases[i][1]);
      Assert.assertEquals(testCases[i][2], fqn);
    }
  }

  @Test
  public void testOracleFQN() {
    // Testcases consist of Connection URL, Table Name, Expected FQN String
    String[][] testCases  = {{"jdbc:oracle:thin:@localhost:db", "table1", "oracle://localhost:1521/db.table1"},
      {"jdbc:oracle:thin:@test.server:1111/db",
        "table2", "oracle://test.server:1111/db.table2"}};
    for (int i = 0; i < testCases.length; i++) {
      String fqn = FQNGenerator.constructFQN(testCases[i][0], testCases[i][1]);
      Assert.assertEquals(testCases[i][2], fqn);
    }
  }

  @Test
  public void testPostgresqlFQN() {
    // Testcases consist of Connection URL, Table Name, Expected FQN String
    String[][] testCases  = {{"jdbc:postgresql://34.35.36.37/test?user=user&password=secret&ssl=true",
      "table1", "postgresql://34.35.36.37:5432/test.public.table1"},
      {"jdbc:postgresql://localhost/test?connectionSchema=schema",
        "table2", "postgresql://localhost:5432/test.schema.table2"},
      {"jdbc:postgresql://localhost/test?searchpath=schema",
        "table3", "postgresql://localhost:5432/test.schema.table3"}};
    for (int i = 0; i < testCases.length; i++) {
      String fqn = FQNGenerator.constructFQN(testCases[i][0], testCases[i][1]);
      Assert.assertEquals(testCases[i][2], fqn);
    }
  }
}
