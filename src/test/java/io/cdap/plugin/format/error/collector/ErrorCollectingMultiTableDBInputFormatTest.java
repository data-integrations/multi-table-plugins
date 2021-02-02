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

package io.cdap.plugin.format.error.collector;

import io.cdap.plugin.format.DBTableName;
import io.cdap.plugin.format.DBTableSplit;
import io.cdap.plugin.format.RecordWrapper;
import io.cdap.plugin.format.error.emitter.ErrorEmittingInputSplit;
import io.cdap.plugin.format.error.emitter.ErrorEmittingRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorCollectingMultiTableDBInputFormatTest {
  JobContext ctx;
  ErrorCollectingMultiTableDBInputFormat inputFormat;
  InputFormat<NullWritable, RecordWrapper> delegate;
  InputSplit is;
  TaskAttemptContext taskCtx;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    ctx = mock(JobContext.class);
    delegate = mock(InputFormat.class);
    inputFormat = new ErrorCollectingMultiTableDBInputFormat(delegate);
    is = mock(InputSplit.class);
    taskCtx = mock(TaskAttemptContext.class);
  }

  @Test
  public void testGetSplits() throws IOException, InterruptedException {
    when(delegate.getSplits(ctx)).thenReturn(Collections.emptyList());

    List<InputSplit> result = inputFormat.getSplits(ctx);

    Assert.assertEquals(Collections.EMPTY_LIST, result);
    Mockito.verify(delegate, Mockito.times(1)).getSplits(ctx);
  }

  @Test
  public void testGetSplitsThrowsException() throws IOException, InterruptedException {
    when(delegate.getSplits(ctx)).thenThrow(new IOException("error"));

    List<InputSplit> result = inputFormat.getSplits(ctx);

    Mockito.verify(delegate, Mockito.times(1)).getSplits(ctx);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0) instanceof ErrorEmittingInputSplit);
  }

  @Test
  public void testCreateRecordReaderWithErrorEmittingInputSplit() throws IOException, InterruptedException {
    is = new ErrorEmittingInputSplit("referenceName", "error", "className");

    RecordReader<NullWritable, RecordWrapper> result = inputFormat.createRecordReader(is, taskCtx);

    Assert.assertTrue(result instanceof ErrorEmittingRecordReader);
    Mockito.verify(delegate, Mockito.times(0)).createRecordReader(any(), any());
  }

  @Test
  public void testCreateRecordReader() throws IOException, InterruptedException {
    is = new DBTableSplit(new DBTableName("somedatabase", "sometable"));

    RecordReader<NullWritable, RecordWrapper> result = inputFormat.createRecordReader(is, taskCtx);

    Assert.assertTrue(result instanceof ErrorCollectingRecordReader);
    Mockito.verify(delegate, Mockito.times(1)).createRecordReader(any(), any());
  }

  @Test
  public void testCreateRecordReaderHandlesException() throws IOException, InterruptedException {
    when(delegate.createRecordReader(any(), any())).thenThrow(new RuntimeException("error"));

    is = new DBTableSplit(new DBTableName("somedatabase", "sometable"));

    RecordReader<NullWritable, RecordWrapper> result = inputFormat.createRecordReader(is, taskCtx);

    Assert.assertTrue(result instanceof ErrorEmittingRecordReader);
    Mockito.verify(delegate, Mockito.times(1)).createRecordReader(any(), any());
  }
}
