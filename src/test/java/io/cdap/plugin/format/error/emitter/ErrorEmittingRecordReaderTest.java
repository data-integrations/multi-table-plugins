package io.cdap.plugin.format.error.emitter;

import io.cdap.plugin.format.RecordWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ErrorEmittingRecordReaderTest {
  ErrorEmittingRecordReader recordReader;

  @Before
  public void setUp() {
    recordReader = new ErrorEmittingRecordReader("errorMessage", "exceptionClass");
  }

  @Test
  public void testNextKeyValue() throws IOException, InterruptedException {
    Assert.assertTrue(recordReader.nextKeyValue());
    //The second time this method is called it will return false.
    Assert.assertFalse(recordReader.nextKeyValue());
  }

  @Test
  public void testGetCurrentValue() throws IOException, InterruptedException {
    RecordWrapper wrapper = recordReader.getCurrentValue();

    //This should be an error record.
    Assert.assertTrue(wrapper.isError());
  }
}
