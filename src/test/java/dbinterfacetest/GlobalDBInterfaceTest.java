package dbinterfacetest;

import junit.framework.TestCase;

import org.junit.Before;

import java.sql.ResultSet;
import java.sql.SQLException;

import dbinterface.DBInterface;
import dbinterface.Query;

/**
 * Created by will on 20/12/15.
 */
public class GlobalDBInterfaceTest extends TestCase {
  public DBInterface dbi;

  @Before
  public void setUp() throws Exception {
    dbi = new DBInterface().open().globalize();
    dbi.runSql(TestRecord.CREATE);
  }

  public void testIsGlobal() {
    assertEquals(DBInterface.getGlobal(), dbi);
  }

  public void testInsertGlobally() throws SQLException {
    TestRecord tr = new TestRecord();
    tr.boolVar = true;
    tr.stringVar = "String";
    tr.save();
    TestRecord tr1 = new Query(dbi).from(TestRecord.class).first();
    assertEquals(tr.boolVar, tr1.boolVar);
    assertEquals(tr.stringVar, tr1.stringVar);
  }

  public void testUpdateGlobally() throws SQLException {
    TestRecord test = new TestRecord();
    test.stringVar = "String";
    test.save();
    test.stringVar = "Another string";
    test.save();
    TestRecord other = new Query().from(TestRecord.class).first();
    assertEquals(other.stringVar, test.stringVar);
  }

  public void testDropGlobally() throws SQLException {
    TestRecord test = new TestRecord();
    test.stringVar = "String";
    test.save();
    test.drop();
    ResultSet rs = new Query().select("count(*)").in(TestRecord.class).firstCursor();
    assertEquals(0, rs.getInt(1));
  }
}
