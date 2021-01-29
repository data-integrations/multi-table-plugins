package io.cdap.plugin.format.error.emitter;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.format.error.ErrorSchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ErrorEmittingInputFormat extends InputFormat<NullWritable, StructuredRecord> {
  private static final String PREFIX = "io.cdap.plugin.format.error.emitter.ErrorEmittingInputFormat.";
  public static final String ERROR_MESSAGE = PREFIX + ErrorSchema.ERROR_MESSAGE;
  public static final String EXCEPTION_CLASS_NAME = PREFIX + ErrorSchema.EXCEPTION_CLASS_NAME;

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    String errorMessage = context.getConfiguration().get(ERROR_MESSAGE);
    String exceptionClassName = context.getConfiguration().get(EXCEPTION_CLASS_NAME);

    InputSplit errorSplit = new ErrorEmittingInputSplit(errorMessage, exceptionClassName);
    return Collections.singletonList(errorSplit);
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    ErrorEmittingInputSplit errorSplit = (ErrorEmittingInputSplit) split;
    return new ErrorEmittingRecordReader(errorSplit.getErrorMessage(),
                                         errorSplit.getExceptionClassName());
  }
}
