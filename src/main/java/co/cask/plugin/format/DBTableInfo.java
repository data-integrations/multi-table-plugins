package co.cask.plugin.format;

import co.cask.cdap.api.data.schema.Schema;

/**
 * Information about a DB table.
 */
public class DBTableInfo {
  private final String name;
  private final Schema schema;

  public DBTableInfo(String name, Schema schema) {
    this.name = name;
    this.schema = schema;
  }

  public String getName() {
    return name;
  }

  public Schema getSchema() {
    return schema;
  }
}
