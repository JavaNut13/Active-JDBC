package dbinterfacetest;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import java.sql.ResultSet;
import java.util.HashMap;

import dbinterface.DBInterface;
import dbinterface.Query;

public class DBInterfaceTest extends TestCase {

  public DBInterface dbi;

  @Before
  public void setUp() throws Exception {
    dbi = new DBInterface().open();
    dbi.runSql(TestRecord.CREATE);
    for(int i = 0; i < 10; i++) {
      TestRecord tr = new TestRecord();
      tr.stringVar = "String value";
      tr.boolVar = (i % 2) == 0;
      tr.intVar = i;
      tr.save(dbi);
    }
  }

  @After
  public void tearDown() throws Exception {
    dbi.close();
  }


  public void testOpen() throws Exception {
    dbi.open();
    ResultSet rs = dbi.rawQuery("SELECT * FROM sqlite_master WHERE type=?", new Object[]{"table"});
    int i = 0;
    while(rs.next()) {
      i++;
    }
    assertEquals(1, i);
    rs.close();
  }


  public void testClose() throws Exception {
    dbi.close();
    assertEquals(true, dbi.isClosed());
  }


  public void testQuery() throws Exception {
    ResultSet rs = dbi.query("testrecord", "*", "boolvar=?", new Object[] {true}, null, null, DBInterface.ALL);
    int i = 0;
    while(rs.next()) {
      i++;
    }
    assertEquals(5, i);
    rs.close();
  }


  public void testQuery1() throws Exception {
    ResultSet rs = dbi.query("testrecord", "*", null, null, null, null, DBInterface.ALL);
    int i = 0;
    while(rs.next()) {
      i++;
    }
    assertEquals(10, i);
    rs.close();
  }


  public void testUpdate() throws Exception {
    HashMap<String, Object> vals = new HashMap<>();
    vals.put("stringvar", "String value");
    vals.put("intvar", 5);
    vals.put("boolvar", false);
    dbi.update("testrecord", vals, null, new Object[] {});
    ResultSet rs = new Query(dbi).from("testrecord").allCursor();
    while(rs.next()) {
      assertEquals(5, rs.getInt("intvar"));
      assertEquals("String value", rs.getString("stringvar"));
      assertEquals(false, rs.getBoolean("boolvar"));
    }
  }


  public void testInsert() throws Exception {
    HashMap<String, Object> vals = new HashMap<>();
    vals.put("stringvar", "This is a unique value");
    vals.put("intvar", 975);
    vals.put("boolvar", true);
    dbi.insert("testrecord", vals);
    ResultSet rs = new Query(dbi).from("testrecord").orderBy("id DESC").firstCursor();
    assertEquals(975, rs.getInt("intvar"));
    assertEquals("This is a unique value", rs.getString("stringvar"));
    assertEquals(true, rs.getBoolean("boolvar"));
  }


  public void testDelete() throws Exception {
    dbi.delete("testrecord", "boolvar=?", new Object[]{true});
    ResultSet rs = new Query(dbi).from("testrecord").allCursor();
    int i = 0;
    while(rs.next()) {
      i++;
      assertEquals(false, rs.getBoolean("boolvar"));
    }
    assertEquals(5, i);
  }


  public void testRawQuery() throws Exception {
    ResultSet rs = dbi.rawQuery("select count(*) as count from testrecord where boolvar=?", new Object[]{true});
    assertEquals(5, rs.getInt("count"));
  }
}