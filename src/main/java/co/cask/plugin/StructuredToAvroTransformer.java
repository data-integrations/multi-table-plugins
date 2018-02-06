package co.cask.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Structured Record to Avro converter
 */
public class StructuredToAvroTransformer extends AbstractStructuredRecordTransformer<GenericRecord> {
	private final Map<Integer, Schema> schemaCache;

	public StructuredToAvroTransformer(@Nullable co.cask.cdap.api.data.schema.Schema outputSchema) {
		super(outputSchema);
		this.schemaCache = Maps.newHashMap();
	}

	@Override
	public GenericRecord transform(StructuredRecord structuredRecord,
																 co.cask.cdap.api.data.schema.Schema schema) throws IOException {
		co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();

		Schema avroSchema = getAvroSchema(schema);

		GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
		for (Schema.Field field : avroSchema.getFields()) {
			String fieldName = field.name();
			co.cask.cdap.api.data.schema.Schema.Field schemaField = structuredRecordSchema.getField(fieldName);
			if (schemaField == null) {
				throw new IllegalArgumentException("Input record does not contain the " + fieldName + " field.");
			}
			recordBuilder.set(fieldName, convertField(structuredRecord.get(fieldName), schemaField.getSchema()));
		}
		return recordBuilder.build();
	}

	private Schema getAvroSchema(co.cask.cdap.api.data.schema.Schema cdapSchema) {
		int hashCode = cdapSchema.hashCode();
		if (schemaCache.containsKey(hashCode)) {
			return schemaCache.get(hashCode);
		} else {
			Schema avroSchema = new Schema.Parser().parse(cdapSchema.toString());
			schemaCache.put(hashCode, avroSchema);
			return avroSchema;
		}
	}

	@Override
	protected Object convertBytes(Object field) {
		if (field instanceof ByteBuffer) {
			return field;
		}
		return ByteBuffer.wrap((byte[]) field);
	}
}