package io.cdap.plugin.format.error;

/**
 * Exception used to signal that the number of failures in a MultiTableDBSource exceeds the predefined threshold.
 */
public class TableFailureException extends RuntimeException {
  public TableFailureException() {
    super("Multi Table Plugin was not able to fetch all tables.");
  }
}
