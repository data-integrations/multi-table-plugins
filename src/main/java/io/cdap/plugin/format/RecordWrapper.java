package io.cdap.plugin.format;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;

public class RecordWrapper {
  private final StructuredRecord record;
  private final InvalidEntry<StructuredRecord> invalidEntry;

  public RecordWrapper(StructuredRecord record) {
    this.record = record;
    this.invalidEntry = null;
  }

  public RecordWrapper(InvalidEntry<StructuredRecord> invalidEntry) {
    this.record = null;
    this.invalidEntry = invalidEntry;
  }

  public StructuredRecord getRecord() {
    return this.record;
  }

  public InvalidEntry<StructuredRecord> getInvalidEntry() {
    return this.invalidEntry;
  }

  public boolean isError() {
    return this.invalidEntry != null;
  }
}
