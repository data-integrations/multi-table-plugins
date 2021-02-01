/*
 * Copyright © 2021 Cask Data, Inc.
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

