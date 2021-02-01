package io.cdap.plugin.format.error.collector;

import io.cdap.plugin.format.RecordWrapper;
import io.cdap.plugin.format.error.ErrorSchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Record Reader that collects exceptions in the delegate RecordReader.
 * If an exception is thrown in this delegate, the exception is captured and an Error Record is emitted instead.
 */
public class ErrorCollectingRecordReader extends RecordReader<NullWritable, RecordWrapper> {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorCollectingRecordReader.class);

  RecordReader<NullWritable, RecordWrapper> delegate;
  String tableName;
  RecordWrapper errorRecordWrapper;

  public ErrorCollectingRecordReader(RecordReader<NullWritable, RecordWrapper> delegate, String tableName) {
    this.delegate = delegate;
    this.tableName = tableName;
    this.errorRecordWrapper = null;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    delegate.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (errorRecordWrapper != null) {
      return false;
    }

    try {
      return delegate.nextKeyValue();
    } catch (Exception e) {
      LOG.error("Unable to fetch row.", e);
      errorRecordWrapper = ErrorSchema.errorRecordWrapper("Unable to fetch row.",
                                                               e.getClass().getCanonicalName(),
                                                               tableName);
      return true;
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    if (errorRecordWrapper != null) {
      return NullWritable.get();
    }

    return delegate.getCurrentKey();
  }

  @Override
  public RecordWrapper getCurrentValue() throws IOException, InterruptedException {
    if (errorRecordWrapper != null) {
      return errorRecordWrapper;
    }

    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (errorRecordWrapper != null) {
      return 100;
    }

    return delegate.getProgress();
  }

  @Override
  public void close() throws IOException {
    try {
      delegate.close();
    } catch (Exception e) {
      LOG.error("Exception when closing record reader.", e);
    }
  }
}
