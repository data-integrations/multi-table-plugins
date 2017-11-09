/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.plugin.format;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * An OutputFormat that filters records before sending them to a delegate. Currently only supports TextOutputFormat
 * as the delegate, but we may want to add other formats in the future.
 */
public class RecordFilterOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  public static final String FILTER_FIELD = "record.filter.field";
  public static final String PASS_VALUE = "record.filter.val";
  public static final String DELIMITER = "record.filter.delimiter";

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    TextOutputFormat<NullWritable, Text> textOutputFormat = new TextOutputFormat<>();
    RecordWriter<NullWritable, Text> delegate = textOutputFormat.getRecordWriter(context);

    String filterField = conf.get(FILTER_FIELD);
    String passthroughVal = conf.get(PASS_VALUE);
    String delimiter = conf.get(DELIMITER);

    return new FilterRecordWriter(delegate, filterField, passthroughVal, delimiter);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    TextOutputFormat<NullWritable, Text> textOutputFormat = new TextOutputFormat<>();
    textOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    TextOutputFormat<NullWritable, Text> textOutputFormat = new TextOutputFormat<>();
    return textOutputFormat.getOutputCommitter(context);
  }

  /**
   * Filters records before writing them out as text.
   */
  public static class FilterRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
    private final String filterField;
    private final String passthroughValue;
    private final String delimiter;
    private final Text text;
    private final RecordWriter<NullWritable, Text> delegate;

    FilterRecordWriter(RecordWriter<NullWritable, Text> delegate, String filterField, String passthroughValue,
                              String delimiter) {
      this.filterField = filterField;
      this.passthroughValue = passthroughValue;
      this.delegate = delegate;
      this.delimiter = delimiter;
      this.text = new Text();
    }

    @Override
    public void write(NullWritable key, StructuredRecord record) throws IOException, InterruptedException {
      String val = record.get(filterField);
      if (passthroughValue.equals(val)) {
        StringBuilder line = new StringBuilder();
        for (Schema.Field field : record.getSchema().getFields()) {
          String fieldName = field.getName();
          if (filterField.equals(fieldName)) {
            continue;
          }
          Object fieldVal = record.get(fieldName);
          String fieldValStr = fieldVal == null ? "" : fieldVal.toString();
          line.append(fieldValStr).append(delimiter);
        }
        line.deleteCharAt(line.length() - 1);
        text.set(line.toString());
        delegate.write(key, text);
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.close(context);
    }
  }
}
