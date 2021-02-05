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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SQLStatementRecordReaderTest {

  @Test
  public void testGetTableNameWithValidAlias() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("a");

    SQLStatementSplit split = new SQLStatementSplit("ref", "select 1;", "alias", "fallback");

    Assert.assertEquals("alias", SQLStatementRecordReader.getTableName(split, meta));
    verify(meta, times(0)).getColumnCount();
    verify(meta, times(0)).getTableName(anyInt());
  }

  @Test
  public void testGetTableNameWithComputedTableName() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("a");

    SQLStatementSplit split = new SQLStatementSplit("ref", "select 1;", "", "fallback");

    Assert.assertEquals("a", SQLStatementRecordReader.getTableName(split, meta));
    verify(meta, times(2)).getColumnCount();
    verify(meta, times(1)).getTableName(anyInt());
  }

  @Test
  public void testGetTableNameWithFallback() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("");

    SQLStatementSplit split = new SQLStatementSplit("ref", "select 1;", "", "fallback");

    Assert.assertEquals("fallback", SQLStatementRecordReader.getTableName(split, meta));
    verify(meta, times(2)).getColumnCount();
    verify(meta, times(1)).getTableName(anyInt());
  }

  @Test
  public void testBuildTableName1() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("a");
    when(meta.getTableName(2)).thenReturn("a");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("a", SQLStatementRecordReader.buildTableName(meta));
  }

  @Test
  public void testBuildTableName2() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("a");
    when(meta.getTableName(2)).thenReturn("a");
    when(meta.getTableName(3)).thenReturn("b");

    Assert.assertEquals("a_b", SQLStatementRecordReader.buildTableName(meta));
  }

  @Test
  public void testBuildTableName3() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("a");
    when(meta.getTableName(2)).thenReturn("b");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("a_b", SQLStatementRecordReader.buildTableName(meta));
  }

  @Test
  public void testBuildTableName4() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("b");
    when(meta.getTableName(2)).thenReturn("a");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("b_a", SQLStatementRecordReader.buildTableName(meta));
  }

  @Test
  public void testBuildTableName5() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("c");
    when(meta.getTableName(2)).thenReturn("b");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("c_b_a", SQLStatementRecordReader.buildTableName(meta));
  }
}
