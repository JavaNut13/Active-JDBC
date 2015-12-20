package dbinterfacetest;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import dbinterface.DBInterface;
import dbinterface.HashRecord;

public class HashRecordTest extends TestCase {
  private DBInterface dbi;
  private HashRecord hs;

  @Before
  public void setUp() throws Exception {
    dbi = new DBInterface().open();
    dbi.runSql("DROP TABLE IF EXISTS testhash; CREATE TABLE testhash (" +
        "key varchar(255)," +
        "value varchar(255));");
    hs = new HashRecord("testhash");
    hs.put("key1", "Secret value");
    hs.put("key2", "VALUES");
    hs.save(dbi);
  }


  public void testLoad() throws Exception {
    HashRecord hs1 = new HashRecord("testhash");
    hs1.load(dbi);
    assertEquals("Secret value", hs1.get("key1"));
    assertEquals("VALUES", hs1.get("key2"));
  }


  public void testSave() throws Exception {
    hs.put("things", "THINGS ARE THINGS");
    hs.save(dbi);
    HashRecord hr = new HashRecord("testhash");
    hr.load(dbi);
    assertEquals("THINGS ARE THINGS", hr.get("things"));
  }


  public void testRemove() throws Exception {
    hs.remove("key1");
    assertEquals(false, hs.has("key1"));
    hs.save(dbi);
    HashRecord hs1 = new HashRecord("testhash");
    hs1.load(dbi);
    assertEquals(false, hs1.has("key1"));
  }


  public void testPut() throws Exception {
    hs.put("new key", "New value, yo");
    hs.save(dbi);
    HashRecord hs1 = new HashRecord("testhash");
    hs1.load(dbi);
    assertEquals(true, hs1.has("new key"));
  }


  public void testGet() throws Exception {
    hs.put("new key", "New value, yo");
    hs.save(dbi);
    HashRecord hs1 = new HashRecord("testhash");
    hs1.load(dbi);
    assertEquals(true, hs1.has("new key"));
    assertEquals("New value, yo", hs1.get("new key"));
  }


  public void testIsSaved() throws Exception {
    assertEquals(true, hs.isSaved());
    hs.put("New", "not saved");
    assertEquals(false, hs.isSaved());
    hs.save(dbi);
    assertEquals(true, hs.isSaved());
  }
}