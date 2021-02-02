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

import static io.cdap.plugin.format.SQLStatementRecordReader.buildTableName;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SQLStatementRecordReaderTest {

  @Test
  public void testBuildTableName1() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("a");
    when(meta.getTableName(2)).thenReturn("a");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("a", buildTableName(meta));
  }

  @Test
  public void testBuildTableName2() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("a");
    when(meta.getTableName(2)).thenReturn("a");
    when(meta.getTableName(3)).thenReturn("b");

    Assert.assertEquals("a_b", buildTableName(meta));
  }

  @Test
  public void testBuildTableName3() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("a");
    when(meta.getTableName(2)).thenReturn("b");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("a_b", buildTableName(meta));
  }

  @Test
  public void testBuildTableName4() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("b");
    when(meta.getTableName(2)).thenReturn("a");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("b_a", buildTableName(meta));
  }

  @Test
  public void testBuildTableName5() throws SQLException {
    ResultSetMetaData meta = mock(ResultSetMetaData.class);

    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("c");
    when(meta.getTableName(2)).thenReturn("b");
    when(meta.getTableName(3)).thenReturn("a");

    Assert.assertEquals("c_b_a", buildTableName(meta));
  }
}