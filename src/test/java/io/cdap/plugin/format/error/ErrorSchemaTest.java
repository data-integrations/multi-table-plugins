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

package io.cdap.plugin.format.error;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.plugin.format.RecordWrapper;
import org.junit.Assert;
import org.junit.Test;

public class ErrorSchemaTest {

  @Test
  public void testCreateErrorRecord() {
    RecordWrapper wrapper = ErrorSchema.errorRecordWrapper("referenceName",
                                                           "errorMessage",
                                                           "exceptionClass",
                                                           "tableName");

    InvalidEntry<StructuredRecord> invalidEntry = wrapper.getInvalidEntry();
    StructuredRecord invalidRecord = invalidEntry.getInvalidRecord();

    Assert.assertEquals("referenceName", invalidRecord.get(ErrorSchema.REFERENCE_NAME));
    Assert.assertEquals("errorMessage", invalidRecord.get(ErrorSchema.ERROR_MESSAGE));
    Assert.assertEquals("exceptionClass", invalidRecord.get(ErrorSchema.EXCEPTION_CLASS_NAME));
    Assert.assertEquals("tableName", invalidRecord.get(ErrorSchema.TABLE_NAME));
  }

  @Test
  public void testCreateErrorRecordWithNullTable() {
    RecordWrapper wrapper = ErrorSchema.errorRecordWrapper("referenceName", "errorMessage", "exceptionClass", null);

    InvalidEntry<StructuredRecord> invalidEntry = wrapper.getInvalidEntry();
    StructuredRecord invalidRecord = invalidEntry.getInvalidRecord();

    Assert.assertEquals("referenceName", invalidRecord.get(ErrorSchema.REFERENCE_NAME));
    Assert.assertEquals("errorMessage", invalidRecord.get(ErrorSchema.ERROR_MESSAGE));
    Assert.assertEquals("exceptionClass", invalidRecord.get(ErrorSchema.EXCEPTION_CLASS_NAME));
    Assert.assertNull(invalidRecord.get(ErrorSchema.TABLE_NAME));
  }

}
