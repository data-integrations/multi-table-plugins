package co.cask.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.RecordConverter;

import javax.annotation.Nullable;
import java.io.IOException;

public abstract class AbstractStructuredRecordTransformer<OUTPUT> extends RecordConverter<StructuredRecord, OUTPUT> {
	protected final Schema outputCDAPSchema;

	public AbstractStructuredRecordTransformer(@Nullable co.cask.cdap.api.data.schema.Schema outputSchema) {
		outputCDAPSchema = outputSchema;
	}

	public OUTPUT transform(StructuredRecord structuredRecord) throws IOException {
		return transform(structuredRecord,
				outputCDAPSchema == null ? structuredRecord.getSchema() : outputCDAPSchema);
	}
}
