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

package io.cdap.plugin.format;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;

/**
 * Class used to wrap either a Structured Record (success) or an Invalid Entry (failure).
 */
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
