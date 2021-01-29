package io.cdap.plugin.format.error;

public class TableFailureThresholdExceededException extends RuntimeException {
  public TableFailureThresholdExceededException(int threshold, int numFailures) {
    super(String.format("Table failures exceed threshold. Failures: %d. Threshold: %d", numFailures, threshold));
  }
}
