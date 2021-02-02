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

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MultiTableConfTest {
  @Test
  public void testSplitSQLStatements() {
    String input = ";select * from a;" +
      "select \"a\", b,c,d from a;   " +
      "select a,b,c from b    ;;; " +
      "select \"\\;\\;\\;\" from c;";

    List<String> statements = MultiTableConf.splitSqlStatements(input);

    Assert.assertEquals(4, statements.size());
    Assert.assertEquals("select * from a", statements.get(0));
    Assert.assertEquals("select \"a\", b,c,d from a", statements.get(1));
    Assert.assertEquals("select a,b,c from b", statements.get(2));
    Assert.assertEquals("select \";;;\" from c", statements.get(3));
  }
}
