package io.cdap.plugin.format.error.collector;

import io.cdap.plugin.format.RecordWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorCollectingRecordReaderTest {
  RecordReader<NullWritable, RecordWrapper> delegate;
  ErrorCollectingRecordReader reader;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    delegate = mock(RecordReader.class);
    reader = new ErrorCollectingRecordReader(delegate, "mytable");
  }

  @Test
  public void testNextKeyValue() throws IOException, InterruptedException {
    when(delegate.nextKeyValue()).thenReturn(true);

    Assert.assertTrue(reader.nextKeyValue());
    Mockito.verify(delegate, Mockito.times(1)).nextKeyValue();
  }

  @Test
  public void testNextKeyValueThrowsException() throws IOException, InterruptedException {
    when(delegate.nextKeyValue()).thenThrow(new RuntimeException("error"));

    Assert.assertTrue(reader.nextKeyValue());
    Mockito.verify(delegate, Mockito.times(1)).nextKeyValue();
    Assert.assertTrue(reader.getCurrentValue().isError());

    //Next time this method is called it should return false.
    Assert.assertFalse(reader.nextKeyValue());
  }
}