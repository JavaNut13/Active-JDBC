package dbinterfacetest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import dbinterface.Record;

/**
 * Created by will on 5/08/15.
 */
public class TestRecord extends Record {
  public String stringVar;
  public int intVar;
  public boolean boolVar;
  public static final String CREATE = "DROP TABLE IF EXISTS testrecord; CREATE TABLE testrecord (\n"+
      "id integer primary key autoincrement,\n"+
      "stringvar varchar(255),\n"+
      "intvar integer,\n"+
      "boolvar boolean)";

  public TestRecord(ResultSet rs) throws SQLException {
    super(rs);
  }
  public TestRecord() {
    super();
  }

  protected void setValues(ResultSet rs) throws SQLException {
    intVar = rs.getInt("intvar");
    boolVar = rs.getBoolean("boolvar");
    stringVar = rs.getString("stringvar");
  }

  protected void insertValues(HashMap<String, Object> map){
    map.put("intvar", intVar);
    map.put("boolvar", boolVar);
    map.put("stringvar", stringVar);
  }

  public void fastValues(Object[] container) {
    container[0] = boolVar;
    container[1] = intVar;
    container[2] = stringVar;
  }
}
