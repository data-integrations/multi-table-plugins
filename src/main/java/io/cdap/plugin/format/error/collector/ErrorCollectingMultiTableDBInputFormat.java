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

package io.cdap.plugin.format.error.collector;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.format.DBTableSplit;
import io.cdap.plugin.format.MultiTableDBInputFormat;
import io.cdap.plugin.format.error.emitter.ErrorEmittingInputSplit;
import io.cdap.plugin.format.error.emitter.ErrorEmittingRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * TODO: add
 */
public class ErrorCollectingMultiTableDBInputFormat extends InputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorCollectingMultiTableDBInputFormat.class);

  MultiTableDBInputFormat delegate = new MultiTableDBInputFormat();

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    try {
      return delegate.getSplits(context);
    } catch (IOException e) {
      // If there was an exception creating the splits, we create a single ErrorEmittingInputSplit in order to
      LOG.error("Exception creating splits", e);
      InputSplit errorSplit = new ErrorEmittingInputSplit("Exception creating splits",
                                                          e.getClass().getCanonicalName());
      return Collections.singletonList(errorSplit);
    }
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context) {
    // Handle the scenario where the Input Split is already an Error Emitting Input Split.
    // In this case, we supply the error message and exception class name to the ErrorEmittingRecordReader.
    if (split instanceof ErrorEmittingInputSplit) {
      ErrorEmittingInputSplit errorSplit = (ErrorEmittingInputSplit) split;
      return new ErrorEmittingRecordReader(errorSplit.getErrorMessage(),
                                           errorSplit.getExceptionClassName());
    }

    try {
      //Get table name from Input Split
      DBTableSplit dbTableSplit = (DBTableSplit) split;
      String tableName = dbTableSplit.getTableName().fullTableName();

      //Delegate record reader creation
      RecordReader<NullWritable, StructuredRecord> reader = delegate.createRecordReader(split, context);

      //Wrap record reader in the error collecting record reader.
      return new ErrorCollectingRecordReader(reader, tableName);
    } catch (Exception e) {
      return getErrorEmittingRecordReader(split, context, e);
    }
  }

  protected RecordReader<NullWritable, StructuredRecord> getErrorEmittingRecordReader(InputSplit split,
                                                                                      TaskAttemptContext context,
                                                                                      Exception e) {
    DBTableSplit dbTableSplit = (DBTableSplit) split;

    String errorMessage = String.format("Error creating splits for table '%s'.",
                                        dbTableSplit.getTableName().fullTableName());

    LOG.error(errorMessage, e);

    return new ErrorEmittingRecordReader(errorMessage,
                                         dbTableSplit.getTableName().fullTableName(),
                                         e.getClass().getCanonicalName());
  }
}
