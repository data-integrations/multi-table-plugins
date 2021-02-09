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

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MultiSQLStatementInputFormatTest {

  MultiTableDBConfiguration conf;
  MultiSQLStatementInputFormat inputFormat;

  @Before
  public void before() {
    conf = mock(MultiTableDBConfiguration.class);
    inputFormat = spy(MultiSQLStatementInputFormat.class);
  }

  @Test
  public void getSplitsWithEmptyStatements() {
    when(conf.getSqlStatements()).thenReturn(Collections.emptyList());
    when(conf.getTableAliases()).thenReturn(Collections.emptyList());

    Assert.assertEquals(0, inputFormat.getSplits(conf).size());
  }

  @Test
  public void getSplitsWithStatementsAndNJoAliases() {
    when(conf.getSqlStatements()).thenReturn(Arrays.asList("Select 1", "Select 2"));
    when(conf.getTableAliases()).thenReturn(Collections.emptyList());

    List<InputSplit> splits = inputFormat.getSplits(conf);
    Assert.assertEquals(2, splits.size());
    Assert.assertTrue(splits.get(0) instanceof SQLStatementSplit);
    Assert.assertTrue(splits.get(1) instanceof SQLStatementSplit);

    SQLStatementSplit split0 = (SQLStatementSplit) splits.get(0);
    Assert.assertEquals("Statement #1", split0.getId());
    Assert.assertEquals("Select 1", split0.getSqlStatement());
    Assert.assertEquals("", split0.getTableAlias());
    Assert.assertEquals("sql_statement_1", split0.getFallbackTableName());

    SQLStatementSplit split1 = (SQLStatementSplit) splits.get(1);
    Assert.assertEquals("Statement #2", split1.getId());
    Assert.assertEquals("Select 2", split1.getSqlStatement());
    Assert.assertEquals("", split1.getTableAlias());
    Assert.assertEquals("sql_statement_2", split1.getFallbackTableName());
  }

  @Test
  public void getSplitsWithStatementsAndAliases() {
    when(conf.getSqlStatements()).thenReturn(Arrays.asList("Select 1", "Select 2"));
    when(conf.getTableAliases()).thenReturn(Arrays.asList("table1", "table2"));

    List<InputSplit> splits = inputFormat.getSplits(conf);
    Assert.assertEquals(2, splits.size());
    Assert.assertTrue(splits.get(0) instanceof SQLStatementSplit);
    Assert.assertTrue(splits.get(1) instanceof SQLStatementSplit);

    SQLStatementSplit split0 = (SQLStatementSplit) splits.get(0);
    Assert.assertEquals("table1", split0.getId());
    Assert.assertEquals("Select 1", split0.getSqlStatement());
    Assert.assertEquals("table1", split0.getTableAlias());
    Assert.assertEquals("sql_statement_1", split0.getFallbackTableName());

    SQLStatementSplit split1 = (SQLStatementSplit) splits.get(1);
    Assert.assertEquals("table2", split1.getId());
    Assert.assertEquals("Select 2", split1.getSqlStatement());
    Assert.assertEquals("table2", split1.getTableAlias());
    Assert.assertEquals("sql_statement_2", split1.getFallbackTableName());
  }

  @Test
  public void getSplitsWithStatementsAndLessAliases() {
    when(conf.getSqlStatements()).thenReturn(Arrays.asList("Select 1", "Select 2"));
    when(conf.getTableAliases()).thenReturn(Collections.singletonList("table1"));

    List<InputSplit> splits = inputFormat.getSplits(conf);
    Assert.assertEquals(2, splits.size());
    Assert.assertTrue(splits.get(0) instanceof SQLStatementSplit);
    Assert.assertTrue(splits.get(1) instanceof SQLStatementSplit);

    SQLStatementSplit split0 = (SQLStatementSplit) splits.get(0);
    Assert.assertEquals("table1", split0.getId());
    Assert.assertEquals("Select 1", split0.getSqlStatement());
    Assert.assertEquals("table1", split0.getTableAlias());
    Assert.assertEquals("sql_statement_1", split0.getFallbackTableName());

    SQLStatementSplit split1 = (SQLStatementSplit) splits.get(1);
    Assert.assertEquals("Statement #2", split1.getId());
    Assert.assertEquals("Select 2", split1.getSqlStatement());
    Assert.assertEquals("", split1.getTableAlias());
    Assert.assertEquals("sql_statement_2", split1.getFallbackTableName());
  }

  @Test
  public void getSplitsWithStatementsAndMoreAliases() {
    when(conf.getSqlStatements()).thenReturn(Arrays.asList("Select 1", "Select 2"));
    when(conf.getTableAliases()).thenReturn(Arrays.asList("table1", "table2", "table3"));

    List<InputSplit> splits = inputFormat.getSplits(conf);
    Assert.assertEquals(2, splits.size());
    Assert.assertTrue(splits.get(0) instanceof SQLStatementSplit);
    Assert.assertTrue(splits.get(1) instanceof SQLStatementSplit);

    SQLStatementSplit split0 = (SQLStatementSplit) splits.get(0);
    Assert.assertEquals("table1", split0.getId());
    Assert.assertEquals("Select 1", split0.getSqlStatement());
    Assert.assertEquals("table1", split0.getTableAlias());
    Assert.assertEquals("sql_statement_1", split0.getFallbackTableName());

    SQLStatementSplit split1 = (SQLStatementSplit) splits.get(1);
    Assert.assertEquals("table2", split1.getId());
    Assert.assertEquals("Select 2", split1.getSqlStatement());
    Assert.assertEquals("table2", split1.getTableAlias());
    Assert.assertEquals("sql_statement_2", split1.getFallbackTableName());
  }
}
