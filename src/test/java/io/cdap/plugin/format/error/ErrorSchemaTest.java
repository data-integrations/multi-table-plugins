package io.cdap.plugin.format.error;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.plugin.format.RecordWrapper;
import org.junit.Assert;
import org.junit.Test;

public class ErrorSchemaTest {

  @Test
  public void testCreateErrorRecord() {
    RecordWrapper wrapper = ErrorSchema.errorRecordWrapper("errorMessage", "exceptionClass", "tableName");

    InvalidEntry<StructuredRecord> invalidEntry = wrapper.getInvalidEntry();
    StructuredRecord invalidRecord = invalidEntry.getInvalidRecord();

    Assert.assertEquals("errorMessage", invalidRecord.get(ErrorSchema.ERROR_MESSAGE));
    Assert.assertEquals("exceptionClass", invalidRecord.get(ErrorSchema.EXCEPTION_CLASS_NAME));
    Assert.assertEquals("tableName", invalidRecord.get(ErrorSchema.TABLE_NAME));
  }

  @Test
  public void testCreateErrorRecordWithNullTable() {
    RecordWrapper wrapper = ErrorSchema.errorRecordWrapper("errorMessage", "exceptionClass", null);

    InvalidEntry<StructuredRecord> invalidEntry = wrapper.getInvalidEntry();
    StructuredRecord invalidRecord = invalidEntry.getInvalidRecord();

    Assert.assertEquals("errorMessage", invalidRecord.get(ErrorSchema.ERROR_MESSAGE));
    Assert.assertEquals("exceptionClass", invalidRecord.get(ErrorSchema.EXCEPTION_CLASS_NAME));
    Assert.assertNull(invalidRecord.get(ErrorSchema.TABLE_NAME));
  }

}