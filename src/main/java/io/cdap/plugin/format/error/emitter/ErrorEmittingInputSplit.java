package io.cdap.plugin.format.error.emitter;

import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;

/**
 * InputSplit that holds information for an errorMessage and exceptionClassName.
 * This can be used to set up an {@link io.cdap.plugin.format.error.emitter.ErrorEmittingRecordReader}
 */
public class ErrorEmittingInputSplit extends InputSplit {
  String errorMessage;
  String exceptionClassName;

  public ErrorEmittingInputSplit(String errorMessage, String exceptionClassName) {
    this.errorMessage = errorMessage;
    this.exceptionClassName = exceptionClassName;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getExceptionClassName() {
    return exceptionClassName;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }
}

