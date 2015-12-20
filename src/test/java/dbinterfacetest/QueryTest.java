package dbinterfacetest;

import junit.framework.Test;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import dbinterface.DBInterface;
import dbinterface.Query;
import dbinterface.Record;

public class QueryTest extends TestCase {
  DBInterface dbi = null;
  private static final String[] vals = new String[]{
      "String 1",
      "Test string",
      "Another test string"
  };

  @Before
  public void setUp() throws Exception {
    dbi = new DBInterface().open();
    dbi.runSql(TestRecord.CREATE);
    for(int i = 0; i < 10; i++) {
      TestRecord tr = new TestRecord();
      tr.stringVar = vals[i % vals.length];
      tr.boolVar = (i % 2) == 0;
      tr.intVar = i;
      tr.save(dbi);
    }
  }

  @After
  public void tearDown() throws Exception {
    dbi.close();
  }


  public void testWhere() throws Exception {
    Query q = new Query(dbi).in(TestRecord.TABLE_NAME).where("stringvar LIKE ?", "Another%");
    ArrayList records = Record.getAll(q.all(), TestRecord.class);

    for(Object r : records) {
      assertEquals("Another test string", ((TestRecord) r).stringVar);
    }
    int len = records.size();
    assertEquals(len, 3);
  }

  public void testWhereObjects() throws Exception {
    Query q = new Query(dbi).in(TestRecord.TABLE_NAME).where("intvar > ?", 5);
    ArrayList records = Record.getAll(q.all(), TestRecord.class);
    TestRecord rec = (TestRecord) records.get(0);
    assertEquals(rec.intVar, 6);
    rec = (TestRecord) records.get(3);
    assertEquals(rec.intVar, 9);
    int len = records.size();
    assertEquals(len, 4);
  }


  public void testWhereID() throws Exception {
    Query q = new Query(dbi).in(TestRecord.class).whereID(1);
    ArrayList records = q.all();
    assertEquals(records.size(), 1);
    TestRecord tr = (TestRecord) records.get(0);
    assertEquals(tr.stringVar, vals[0]);
    assertEquals(0, tr.intVar);
  }


  public void testAll() throws Exception {
    Query q = new Query(dbi).in(TestRecord.class);
    ArrayList records = q.all();
    assertEquals(records.size(), 10);
  }


  public void testFirst() throws Exception {
    Query q = new Query(dbi).in(TestRecord.class);
    TestRecord rec = q.first();
    assertEquals(rec.getID(), 1);
    assertEquals(rec.stringVar, vals[0]);
    assertEquals(rec.intVar, 0);
  }


  public void testGroupBy() throws Exception {
    Query q = new Query(dbi).in(TestRecord.class).select("count(*) as count").groupBy("boolvar");
    ResultSet rs = q.allCursor();
    int c = 0;
    while(rs.next()) {
      assertEquals(5, rs.getInt("count"));
      c++;
    }
    assertEquals(2, c);
  }


  public void testOrderBy() throws Exception {
    Query q = new Query(dbi).in(TestRecord.TABLE_NAME).orderBy("intvar DESC");
    ResultSet rs = q.all();
    int i = 9;
    while(rs.next()) {
      assertEquals(i, rs.getInt("intvar"));
      i--;
    }
  }


  public void testSelect() throws Exception {
    Query q = new Query(dbi).in(TestRecord.TABLE_NAME).select("count(*) as count, intvar").groupBy("boolvar");
    ResultSet rs = q.all();
    int c = 0;
    int i = 9;
    while(rs.next()) {
      assertEquals(5, rs.getInt("count"));
      assertEquals(i, rs.getInt("intvar"));
      i--;
      c++;
    }
    assertEquals(2, c);
  }


  public void testFrom() throws Exception {
    Query q = new Query(dbi).from("sqlite_sequence");
    ResultSet rs = q.all();
    boolean found = false;
    while(rs.next()) {
      if(rs.getString("name").equals("testtable")) {
        found = true;
        break;
      }
    }
    assertEquals(true, found);
  }


  public void testFrom1() throws Exception {
    TestRecord tr = new TestRecord();
    Query q = new Query(dbi).from(tr);
    ResultSet rs = q.all();
    int i = 0;
    while(rs.next()) {
      i++;
    }
    assertEquals(i, 10);
    rs.close();
  }


  public void testInsert() throws Exception {
    TestRecord newRec = new TestRecord();
    newRec.boolVar = false;
    newRec.stringVar = "Unique string value";
    newRec.intVar = 927464;
    newRec.save(dbi);
    Query q = new Query(dbi).from(newRec).whereID(newRec.getID());
    TestRecord otherRecord = new TestRecord(q.first());
    assertEquals(otherRecord.stringVar, "Unique string value");
    assertEquals(otherRecord.intVar, 927464);
    assertEquals(otherRecord.boolVar, false);
  }


  public void testUpdate() throws Exception {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("intvar", 69);
    map.put("id", 1);
    new Query(dbi).from("testtable").update(map);
    Query q = new Query(dbi).from("testtable");
    ResultSet rs = q.all();
    while(rs.next()) {
      assertEquals(69, rs.getInt("intvar"));
    }
  }


  public void testUpdate1() throws Exception {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("intvar", 69);
    map.put("id", 1);
    new Query(dbi).from("testtable").update(1, map);
    Query q = new Query(dbi).from("testtable");
    ResultSet rs = q.find(1);
    int i = 0;
    assertEquals(69, rs.getInt("intvar"));
    while(rs.next()) {
      i++;
    }
    assertEquals(1, i);
  }


  public void testDrop() throws Exception {
    Query q = new Query(dbi).from("testtable").where("boolvar = ?", false);
    q.drop();
    ResultSet rs = q.all();
    int i = 0;
    while(rs.next()) {
      i++;
    }
    assertEquals(0, i);
    rs.close();
  }


  public void testDrop1() throws Exception {
    Query q = new Query(dbi).from("testtable");
    q.drop(1);
    ResultSet rs = q.all();
    int i = 0;
    while(rs.next()) {
      i++;
    }
    assertEquals(9, i);
    rs = q.find(1);
    i = 0;
    while(rs.next()) {
      i++;
    }
    assertEquals(0, i);
    rs.close();
  }


  public void testFind() throws Exception {
    Query q = new Query(dbi).from("testtable");
    TestRecord tr = new TestRecord(q.find(2));
    assertEquals(vals[1], tr.stringVar);
  }


  public void testSql() throws Exception {
    ResultSet rs = new Query(dbi).sql("select count(*) as count from testtable where stringvar LIKE ? AND boolvar = ?", "%test%", true);
    assertEquals(3, rs.getInt("count"));
    rs.close();
  }
}