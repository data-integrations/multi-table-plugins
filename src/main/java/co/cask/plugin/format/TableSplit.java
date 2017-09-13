package co.cask.plugin.format;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split representing data in a database table.
 */
public class TableSplit extends InputSplit implements Writable {
  private String tableName;
  private long length;

  // used by mapreduce
  public TableSplit() {
  }

  public TableSplit(String tableName, long length) {
    this.tableName = tableName;
    this.length = length;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeUTF(tableName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    length = in.readLong();
    tableName = in.readUTF();
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }
}
