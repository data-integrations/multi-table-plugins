/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.format.RecordFilterOutputFormat;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;


/**
 * Writes to multiple partitioned file sets.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("DynamicMultiADLS")
@Description("Writes to multiple partitioned file sets. File sets are partitioned by an ingesttime field " +
  "that will be set to the logical start time of the pipeline run. The sink will write to the correct sink based " +
  "on the value of a split field. For example, if the split field is configured to be 'tablename', any record " +
  "with a 'tablename' field of 'xyz' will be written to file set 'xyz'. This plugin expects that the filesets " +
  "to write to will be present in the pipeline arguments. Each table to write to must have an argument where " +
  "the key is 'multisink.[name]' and the value is the schema for that fileset. Most of the time, " +
  "this plugin will be used with the MultiTableDatabase source, which will set those pipeline arguments.")
public class DynamicMultiADLSSink  extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  public static final String TABLE_PREFIX = "multisink.";

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private Conf config;

  public DynamicMultiADLSSink(Conf config) {
    this.config = config;
  }


  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    for (Map.Entry<String, String> argument : context.getArguments()) {
      String key = argument.getKey();
      if (!key.startsWith(TABLE_PREFIX)) {
        continue;
      }
      String schema = argument.getValue();
      String dbTableName = key.substring(TABLE_PREFIX.length());
      //dbTableName is of the form db:table
      String [] parts = dbTableName.split(":");
      String db = parts[0];
      String name = parts[1];
      Job job = JobUtils.createInstance();
      Configuration conf = job.getConfiguration();

      conf.set(FileOutputFormat.OUTDIR, String.format("%s%s_%s%s",config.adlsBasePath, db, name, config.pathSuffix));
      conf.set(RecordFilterOutputFormat.FORMAT, config.outputFormat);
      conf.set(RecordFilterOutputFormat.FILTER_FIELD, config.getSplitField());
      conf.set(RecordFilterOutputFormat.PASS_VALUE, name);
      conf.set(RecordFilterOutputFormat.ORIGINAL_SCHEMA, schema);
      Map<String, String> properties = config.getFileSystemProperties();
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
      job.setOutputValueClass(NullWritable.class);
      if (RecordFilterOutputFormat.AVRO.equals(config.outputFormat)) {
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema);
        AvroJob.setOutputKeySchema(job, avroSchema);
      } else if (RecordFilterOutputFormat.ORC.equals(config.outputFormat)) {
        StringBuilder builder = new StringBuilder();
        io.cdap.plugin.common.HiveSchemaConverter.appendType(builder, Schema.parseJson(schema));
        conf.set("orc.mapred.output.schema", builder.toString());
      } else {
        // Encode the delimiter to base64 to support control characters. Otherwise serializing it in Cconf would result
        // in an error
        conf.set(RecordFilterOutputFormat.DELIMITER,
                 Base64.encodeBase64String(Bytes.toBytesBinary(config.getFieldDelimiter())));
      }

      context.addOutput(Output.of(name, new SinkOutputFormatProvider(RecordFilterOutputFormat.class.getName(), conf)));
    }
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  /**
   * Plugin configuration properties.
   */
  public static class Conf extends PluginConfig {

    @Macro
    @Description("ADLS base path to store.")
    private String adlsBasePath;

    @Macro
    @Description("The suffix for ADLS base path. Used in conjunction with the ADLS basepath will produce an " +
        "outputdirectory which is ADLSBasePath + TableName + suffix")
    private String pathSuffix;

    @Description("The Microsoft Azure Data Lake client id.")
    @Macro
    private String clientId;

    @Description("The Microsoft Azure Data Lake refresh token URL.")
    @Macro
    private String refreshTokenURL;

    @Description("The Microsoft Azure Data Lake credentials.")
    @Macro
    private String credentials;

    @Nullable
    @Description("A JSON string representing a map of properties needed for the distributed file system.")
    @Macro
    public String fileSystemProperties;

    @Description("The format of output files.")
    @Macro
    private String outputFormat;

    @Nullable
    @Description("Output schema of the JSON document. Required for avro output format. " +
        "If left empty for text output format, the schema of input records will be used." +
        "This must be a subset of the schema of input records. " +
        "Fields of type ARRAY, MAP, and RECORD are not supported with the text format. " +
        "Fields of type UNION are only supported if they represent a nullable type.")
    @Macro
    public String schema;

    @Nullable
    @Macro
    @Description("Field delimiter for text format output files. Defaults to tab.")
    public String fieldDelimiter;

    @Nullable
    @Description("The name of the field that will be used to determine which file to write to. " +
      "Defaults to 'tablename'.")
    private String splitField;

    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = getProps();
      properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
      properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
      properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");
      properties.put("dfs.adls.oauth2.refresh.url", refreshTokenURL);
      properties.put("dfs.adls.oauth2.client.id", clientId);
      properties.put("dfs.adls.oauth2.credential", credentials);
      return properties;
    }

    protected Map<String, String> getProps() {
      if (fileSystemProperties == null) {
        return new HashMap<>();
      }
      try {
        return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse fileSystemProperties: " + e.getMessage());
      }
    }

    @Nullable
    public Schema getSchema() {
      if (schema == null) {
        return null;
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.");
      }
    }

    public String getSplitField() {
      return splitField == null ? "tablename" : splitField;
    }

    public String getFieldDelimiter() {
      return fieldDelimiter == null ? "\t" : fieldDelimiter;
    }
  }
}
