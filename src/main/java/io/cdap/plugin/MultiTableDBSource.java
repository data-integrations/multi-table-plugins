/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.format.DBTableInfo;
import io.cdap.plugin.format.MultiTableConf;
import io.cdap.plugin.format.MultiTableDBInputFormat;
import io.cdap.plugin.format.RecordWrapper;
import io.cdap.plugin.format.error.TableFailureException;
import io.cdap.plugin.format.error.collector.ErrorCollectingMultiTableDBInputFormat;
import io.cdap.plugin.format.error.emitter.ErrorEmittingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Batch source to read from multiple tables in a database using JDBC.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("MultiTableDatabase")
@Description("Reads from multiple tables in a relational database. " +
  "Outputs one record for each row in each table, with the table name as a record field. " +
  "Also sets a pipeline argument for each table read, which contains the table schema. ")
public class MultiTableDBSource extends BatchSource<NullWritable, RecordWrapper, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTableDBSource.class);

  private static final String JDBC_PLUGIN_ID = "jdbc.driver";

  private final MultiTableConf conf;

  public MultiTableDBSource(MultiTableConf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass("jdbc", conf.getJdbcPluginName(),
                                                                                JDBC_PLUGIN_ID,
                                                                                PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      throw new IllegalArgumentException(
        String.format("Unable to load JDBC Driver class for plugin name '%s'. " +
                        "Please make sure that the driver plugin has been installed correctly.",
                      conf.getJdbcPluginName()));
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Configuration hConf = new Configuration();
    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);

    Collection<DBTableInfo> tables;

    try {
      tables = MultiTableDBInputFormat.setInput(hConf, conf, driverClass);

      SettableArguments arguments = context.getArguments();
      for (DBTableInfo tableInfo : tables) {
        arguments.set(DynamicMultiFilesetSink.TABLE_PREFIX + tableInfo.getDbTableName().getTable(),
                      tableInfo.getSchema().toString());
      }

      context.setInput(Input.of(conf.getReferenceName(),
                                new SourceInputFormatProvider(ErrorCollectingMultiTableDBInputFormat.class, hConf)));
    } catch (Exception ex) {
      String errorMessage = "Error getting table schemas from database.";
      LOG.error(errorMessage, ex);

      InputFormatProvider errorEmittingInputFormatProvider = new InputFormatProvider() {
        @Override
        public String getInputFormatClassName() {
          return ErrorEmittingInputFormat.class.getName();
        }

        @Override
        public Map<String, String> getInputFormatConfiguration() {
          Map<String, String> config = new HashMap<>();
          for (Map.Entry<String, String> entry : hConf) {
            config.put(entry.getKey(), entry.getValue());
          }
          //Add ErrorEmittingInputFormat config properties
          config.put(ErrorEmittingInputFormat.ERROR_MESSAGE, errorMessage);
          config.put(ErrorEmittingInputFormat.EXCEPTION_CLASS_NAME, ex.getClass().getCanonicalName());

          return config;
        }
      };

      context.setInput(Input.of(conf.getReferenceName(), errorEmittingInputFormatProvider));
    }
  }

  @Override
  public void transform(KeyValue<NullWritable, RecordWrapper> input, Emitter<StructuredRecord> emitter) {
    RecordWrapper wrapper = input.getValue();

    // Check if the record is an error.
    if (wrapper.isError()) {
      // Fail the pipeline if the table error threshold exceeds the desired number of table failures.
      if (MultiTableConf.ERROR_HANDLING_FAIL_PIPELINE.equals(conf.getErrorHandlingMode())) {
        throw new TableFailureException();
      }

      // Emit error record through error port if configured to do so.
      if (MultiTableConf.ERROR_HANDLING_SEND_TO_ERROR_PORT.equals(conf.getErrorHandlingMode())) {
        emitter.emitError(wrapper.getInvalidEntry());
      }

      return;
    }

    emitter.emit(wrapper.getRecord());
  }
}
