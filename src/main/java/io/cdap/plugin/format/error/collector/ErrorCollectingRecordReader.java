package io.cdap.plugin.format.error.collector;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.format.error.ErrorSchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * TODO:add
 */
public class ErrorCollectingRecordReader extends RecordReader<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorCollectingRecordReader.class);

  RecordReader<NullWritable, StructuredRecord> delegate;
  String tableName;
  StructuredRecord errorRecord;

  public ErrorCollectingRecordReader(RecordReader<NullWritable, StructuredRecord> delegate, String tableName) {
    this.delegate = delegate;
    this.tableName = tableName;
    this.errorRecord = null;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    delegate.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (errorRecord != null) {
      return false;
    }

    try {
      return delegate.nextKeyValue();
    } catch (Exception e) {
      LOG.error("Unable to fetch row.", e);
      this.errorRecord = ErrorSchema.errorRecord("Unable to fetch row.",
                                                 e.getClass().getCanonicalName(),
                                                 tableName);
      return true;
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    if (this.errorRecord != null) {
      return NullWritable.get();
    }

    return delegate.getCurrentKey();
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
    if (this.errorRecord != null) {
      return errorRecord;
    }

    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (this.errorRecord != null) {
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
