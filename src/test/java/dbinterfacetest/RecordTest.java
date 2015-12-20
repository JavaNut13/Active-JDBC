package dbinterfacetest;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import java.sql.ResultSet;
import java.util.HashMap;

import dbinterface.DBInterface;
import dbinterface.Query;

public class RecordTest extends TestCase {
  private DBInterface dbi;

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


  public void testGetValues() throws Exception {
    TestRecord tr = new TestRecord();
    HashMap<String, Object> vals = tr.getValues();
    assertEquals(0, vals.get("intvar"));
    assertEquals(false, vals.get("boolvar"));
    assertEquals(null, vals.get("stringvar"));
    tr.boolVar = true;
    tr.intVar = 658;
    tr.stringVar = "A string value";

    vals = tr.getValues();
    assertEquals(658, vals.get("intvar"));
    assertEquals(true, vals.get("boolvar"));
    assertEquals("A string value", vals.get("stringvar"));
  }


  public void testSetValues() throws Exception {
    TestRecord tr = new TestRecord();
    tr.setValues(new Query(dbi).from("testtable").first());
    assertEquals(0, tr.intVar);
    assertEquals(true, tr.boolVar);
    assertEquals("String value", tr.stringVar);
  }


  public void testInsertValues() throws Exception {
    TestRecord tr = new TestRecord();
    HashMap<String, Object> vals = new HashMap<String, Object>();
    tr.boolVar = true;
    tr.intVar = 658;
    tr.stringVar = "Strings strings strings";
    tr.insertValues(vals);
    assertEquals(658, vals.get("intvar"));
    assertEquals("Strings strings strings", vals.get("stringvar"));
    assertEquals(true, vals.get("boolvar"));
  }


  public void testGetID() throws Exception {
    TestRecord tr = new TestRecord();
    tr.boolVar = true;
    tr.intVar = 658;
    tr.stringVar = "Strings strings strings";
    assertEquals(DBInterface.DEFAULT_ID, tr.getID());
    tr.save(dbi);
    assertEquals(11, tr.getID());
  }


  public void testSave() throws Exception {
    TestRecord tr = new TestRecord();
    tr.boolVar = true;
    tr.intVar = 658;
    tr.stringVar = "Strings";
    assertEquals(DBInterface.DEFAULT_ID, tr.getID());
    tr.save(dbi);
    assertEquals(11, tr.getID());
    assertEquals("Strings", tr.stringVar);
    assertEquals(658, tr.intVar);
    assertEquals(true, tr.boolVar);
    tr.save(dbi);
    assertEquals(11, tr.getID());
    assertEquals("Strings", tr.stringVar);
    assertEquals(658, tr.intVar);
    assertEquals(true, tr.boolVar);
  }


  public void testDrop() throws Exception {
    TestRecord tr = new TestRecord();
    tr.save(dbi);
    assertEquals(11, tr.getID());
    tr.drop(dbi);
    ResultSet rs = dbi.rawQuery("select count(*) as count from testtable", new Object[]{});
    assertEquals(10, rs.getInt("count"));
  }


  public void testIsSaved() throws Exception {
    TestRecord tr = new TestRecord();
    assertEquals(false, tr.isSaved());
    tr.save(dbi);
    assertEquals(true, tr.isSaved());
  }


  public void testEquals() throws Exception {
    TestRecord tr = new TestRecord();
    tr.save(dbi);
    TestRecord tr1 = new TestRecord(new Query(dbi).from(TestRecord.class).orderBy("id DESC").first());
    assertEquals(true, tr.equals(tr1));
    TestRecord tr2 = new TestRecord(new Query(dbi).from(TestRecord.class).first());
    assertEquals(false, tr.equals(tr2));
  }
}