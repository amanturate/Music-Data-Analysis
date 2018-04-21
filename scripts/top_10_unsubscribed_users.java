// ORM class for table 'top_10_unsubscribed_users'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Thu Jun 16 09:15:18 IST 2016
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class top_10_unsubscribed_users extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private String user_id;
  public String get_user_id() {
    return user_id;
  }
  public void set_user_id(String user_id) {
    this.user_id = user_id;
  }
  public top_10_unsubscribed_users with_user_id(String user_id) {
    this.user_id = user_id;
    return this;
  }
  private Long duration;
  public Long get_duration() {
    return duration;
  }
  public void set_duration(Long duration) {
    this.duration = duration;
  }
  public top_10_unsubscribed_users with_duration(Long duration) {
    this.duration = duration;
    return this;
  }
  private Integer batchid;
  public Integer get_batchid() {
    return batchid;
  }
  public void set_batchid(Integer batchid) {
    this.batchid = batchid;
  }
  public top_10_unsubscribed_users with_batchid(Integer batchid) {
    this.batchid = batchid;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof top_10_unsubscribed_users)) {
      return false;
    }
    top_10_unsubscribed_users that = (top_10_unsubscribed_users) o;
    boolean equal = true;
    equal = equal && (this.user_id == null ? that.user_id == null : this.user_id.equals(that.user_id));
    equal = equal && (this.duration == null ? that.duration == null : this.duration.equals(that.duration));
    equal = equal && (this.batchid == null ? that.batchid == null : this.batchid.equals(that.batchid));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof top_10_unsubscribed_users)) {
      return false;
    }
    top_10_unsubscribed_users that = (top_10_unsubscribed_users) o;
    boolean equal = true;
    equal = equal && (this.user_id == null ? that.user_id == null : this.user_id.equals(that.user_id));
    equal = equal && (this.duration == null ? that.duration == null : this.duration.equals(that.duration));
    equal = equal && (this.batchid == null ? that.batchid == null : this.batchid.equals(that.batchid));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.user_id = JdbcWritableBridge.readString(1, __dbResults);
    this.duration = JdbcWritableBridge.readLong(2, __dbResults);
    this.batchid = JdbcWritableBridge.readInteger(3, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.user_id = JdbcWritableBridge.readString(1, __dbResults);
    this.duration = JdbcWritableBridge.readLong(2, __dbResults);
    this.batchid = JdbcWritableBridge.readInteger(3, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(user_id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(duration, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(batchid, 3 + __off, 4, __dbStmt);
    return 3;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(user_id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(duration, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(batchid, 3 + __off, 4, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.user_id = null;
    } else {
    this.user_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.duration = null;
    } else {
    this.duration = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.batchid = null;
    } else {
    this.batchid = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.user_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, user_id);
    }
    if (null == this.duration) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.duration);
    }
    if (null == this.batchid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.batchid);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.user_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, user_id);
    }
    if (null == this.duration) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.duration);
    }
    if (null == this.batchid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.batchid);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(user_id==null?"null":user_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(duration==null?"null":"" + duration, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(batchid==null?"null":"" + batchid, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(user_id==null?"null":user_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(duration==null?"null":"" + duration, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(batchid==null?"null":"" + batchid, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.user_id = null; } else {
      this.user_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.duration = null; } else {
      this.duration = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.batchid = null; } else {
      this.batchid = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.user_id = null; } else {
      this.user_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.duration = null; } else {
      this.duration = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.batchid = null; } else {
      this.batchid = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    top_10_unsubscribed_users o = (top_10_unsubscribed_users) super.clone();
    return o;
  }

  public void clone0(top_10_unsubscribed_users o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("user_id", this.user_id);
    __sqoop$field_map.put("duration", this.duration);
    __sqoop$field_map.put("batchid", this.batchid);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("user_id", this.user_id);
    __sqoop$field_map.put("duration", this.duration);
    __sqoop$field_map.put("batchid", this.batchid);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("user_id".equals(__fieldName)) {
      this.user_id = (String) __fieldVal;
    }
    else    if ("duration".equals(__fieldName)) {
      this.duration = (Long) __fieldVal;
    }
    else    if ("batchid".equals(__fieldName)) {
      this.batchid = (Integer) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("user_id".equals(__fieldName)) {
      this.user_id = (String) __fieldVal;
      return true;
    }
    else    if ("duration".equals(__fieldName)) {
      this.duration = (Long) __fieldVal;
      return true;
    }
    else    if ("batchid".equals(__fieldName)) {
      this.batchid = (Integer) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
