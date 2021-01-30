package io.cdap.plugin.format.error.emitter;

import io.cdap.plugin.format.RecordWrapper;
import io.cdap.plugin.format.error.ErrorSchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Record Reader that emits one RecordWrapper with an Invalid Entry.
 * The contents of the emitted Invalid Entry are taken as parameters to this class.
 */
public class ErrorEmittingRecordReader extends RecordReader<NullWritable, RecordWrapper> {

  boolean emitted = false;
  String errorMessage;
  String tableName;
  String exceptionClassName;

  public ErrorEmittingRecordReader(String errorMessage, String exceptionClassName) {
    this.errorMessage = errorMessage;
    this.exceptionClassName = exceptionClassName;
  }

  public ErrorEmittingRecordReader(String errorMessage, String tableName, String exceptionClassName) {
    this.errorMessage = errorMessage;
    this.tableName = tableName;
    this.exceptionClassName = exceptionClassName;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    //no-op
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!emitted) {
      emitted = true;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public RecordWrapper getCurrentValue() throws IOException, InterruptedException {
    return ErrorSchema.errorRecordWrapper(this.errorMessage, this.exceptionClassName, this.tableName);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return emitted ? 100 : 0;
  }

  @Override
  public void close() throws IOException {
    //no-op
  }
}
