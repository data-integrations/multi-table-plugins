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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.plugin.format.MultiSQLStatementInputFormat;
import io.cdap.plugin.format.MultiTableDBConfiguration;
import io.cdap.plugin.format.RecordWrapper;
import io.cdap.plugin.format.SQLStatementSplit;
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
 * Input Format that handles exceptions in the delegate MultiTableDBInputFormat and routes them through an emitter.
 * This allows us to send errors via the Error Emitter for further processing.
 */
public class ErrorCollectingMultiSQLStatementInputFormat extends InputFormat<NullWritable, RecordWrapper> {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorCollectingMultiSQLStatementInputFormat.class);

  private final InputFormat<NullWritable, RecordWrapper> delegate;

  public ErrorCollectingMultiSQLStatementInputFormat() {
    this.delegate = new MultiSQLStatementInputFormat();
  }

  @VisibleForTesting
  public ErrorCollectingMultiSQLStatementInputFormat(InputFormat<NullWritable, RecordWrapper> delegate) {
    this.delegate = delegate;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    try {
      return delegate.getSplits(context);
    } catch (IOException | InterruptedException e) {
      // If there was an exception creating the splits, we create a single ErrorEmittingInputSplit in order to
      LOG.error("Exception creating splits", e);

      MultiTableDBConfiguration conf = new MultiTableDBConfiguration(context.getConfiguration());
      InputSplit errorSplit = new ErrorEmittingInputSplit(conf.getPluginConf().getReferenceName(),
                                                          "Exception creating splits",
                                                          e.getClass().getCanonicalName());
      return Collections.singletonList(errorSplit);
    }
  }

  @Override
  public RecordReader<NullWritable, RecordWrapper> createRecordReader(InputSplit split, TaskAttemptContext context) {
    try {
      //Get configuration
      MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(context.getConfiguration());

      //Get table name from Input Split
      SQLStatementSplit sqlStatementSplit = (SQLStatementSplit) split;
      String statementRef = sqlStatementSplit.getId();

      //Delegate record reader creation
      RecordReader<NullWritable, RecordWrapper> reader = delegate.createRecordReader(split, context);

      //Wrap record reader in the error collecting record reader.
      return new ErrorCollectingRecordReader(multiTableDBConf.getPluginConf().getReferenceName(), reader, statementRef);
    } catch (Exception e) {
      return getErrorEmittingRecordReader(split, context, e);
    }
  }

  protected RecordReader<NullWritable, RecordWrapper> getErrorEmittingRecordReader(InputSplit split,
                                                                                   TaskAttemptContext context,
                                                                                   Exception e) {
    MultiTableDBConfiguration multiTableDBConf = new MultiTableDBConfiguration(context.getConfiguration());
    SQLStatementSplit sqlStatementSplit = (SQLStatementSplit) split;

    String errorMessage = String.format("Error creating record reader for statement '%s'.",
                                        sqlStatementSplit.getId());
    LOG.error(errorMessage, e);

    return new ErrorEmittingRecordReader(multiTableDBConf.getPluginConf().getReferenceName(),
                                         errorMessage,
                                         e.getClass().getCanonicalName());
  }
}
