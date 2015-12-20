package dbinterface;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Data Access Layer to the Sqlite Database
 */
public class DBInterface {
  private static DBInterface globalDatabase = null;
  public static final String COLUMN_ID = "id";
  public static final String META_TABLE = "meta_table";
  public static final int DATABASE_VERSION = 0;
  public static final int DEFAULT_ID = -1;
  public static final int ALL = -1;
  private File dbLocation;

  private Connection connection;
  private Statement runningStatement;

  /**
   * Create a new in-memory database.
   */
  public DBInterface() {
  }

  /**
   * Create a DB that sits on the HDD, not in memory
   *
   * @param location File to save the DB to.
   */
  public DBInterface(File location) {
    dbLocation = location;
  }

  public DBInterface globalize() {
    globalDatabase = this;
    return this;
  }

  public static DBInterface getGlobal() {
    return globalDatabase;
  }

  /**
   * Open a connection to the current database.
   * Closes database first if already open.
   *
   * @return this (for convenience)
   * @throws SQLException
   */
  public DBInterface open() throws SQLException {
    close();
    String location = "jdbc:sqlite:";
    if(dbLocation == null) {
      location += ":memory:";
    } else {
      location += dbLocation.getAbsolutePath();
    }
    connection = DriverManager.getConnection(location);

    upgrade();
    return this;
  }

  /**
   * Disable auto-commit on the database. Could speed up importing
   *
   * @throws SQLException
   */
  public void disableCommit() throws SQLException {
    connection.setAutoCommit(false);
  }

  /**
   * Re-enable autocommit and commit any uncommitted changes.
   *
   * @throws SQLException
   */
  public void commit() throws SQLException {
    connection.commit();
    connection.setAutoCommit(true);
  }

  /**
   * Closes the database, if it's open.
   */
  public void close() {
    if(connection != null) {
      try {
        connection.close();
      } catch (SQLException sqe) {
        // Meh.
      }
      connection = null;
    }
  }

  /**
   * Checks if there is a connection to the DB.
   *
   * @return
   */
  public boolean isClosed() {
    return connection == null;
  }

  private PreparedStatement prepare(String sql, Object[] whereargs, int start) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sql);
    if(whereargs != null) {
      for(Object arg : whereargs) {
        if(arg instanceof Integer) {
          stmt.setInt(start, (Integer) arg);
        } else if(arg instanceof String) {
          stmt.setString(start, (String) arg);
        } else if(arg instanceof Float) {
          stmt.setFloat(start, (Float) arg);
        } else if(arg instanceof Long) {
          stmt.setLong(start, (Long) arg);
        } else if(arg instanceof Boolean) {
          stmt.setBoolean(start, (Boolean) arg);
        } else if(arg instanceof Double) {
          stmt.setDouble(start, (Double) arg);
        }

        start++;
      }
    }
    return stmt;
  }

  /**
   * Make a query to the DB. args that are null are ignored in the SQL. Blank strings are not.
   *
   * @param table     Content for 'FROM ...'
   * @param select    Content for 'SELECT ...'
   * @param where     Content for 'WHERE ...'
   * @param whereargs Replacements for ?s in WHERE
   * @param groupBy   Content for 'GROUP BY ...'
   * @param orderBy   Content for 'ORDER BY ...'
   * @param limit     Number of rows to limit to (DBInterface.ALL if no limit)
   * @return ResultSet with query result
   * @throws SQLException
   */
  public ResultSet query(String table, String select, String where, Object[] whereargs, String groupBy, String orderBy, int limit) throws SQLException {
    String sql = "SELECT " + (select == null ? "*" : select) + " FROM " + table
        + (where == null ? "" : " WHERE " + where)
        + (groupBy == null ? "" : " GROUP BY " + groupBy)
        + (orderBy == null ? "" : " ORDER BY " + orderBy)
        + (limit != DBInterface.ALL ? " LIMIT " + Integer.toString(limit) : "");

    PreparedStatement stmt;

    if(whereargs != null) {
      stmt = prepare(sql, whereargs, 1);
    } else {
      stmt = connection.prepareStatement(sql);
    }
    runningStatement = stmt;
    ResultSet res = stmt.executeQuery();
    runningStatement = null;
    return res;
  }

  private String createUpdate(String[] keys) {
    String[] statements = new String[keys.length];
    int i = 0;
    for(String key : keys) {
      statements[i] = key + "=?";
      i++;
    }
    return String.join(", ", statements);
  }

  /**
   * Run an update statement. null args are ignored, blank strings are not.
   *
   * @param table     Content for 'UPDATE ...'
   * @param values    Content for 'SET (key=value)+'
   * @param where     Content for 'WHERE ...'
   * @param whereargs Values to insert into ?s
   * @return Number of rows altered.
   * @throws SQLException
   */
  public int update(String table, HashMap<String, Object> values, String where, Object[] whereargs) throws SQLException {
    String[] keys = values.keySet().toArray(new String[values.size()]);
    String sql = "UPDATE " + table + " SET " + createUpdate(keys)
        + (where == null ? "" : " WHERE " + where);
    PreparedStatement stmt = null;
    if(whereargs != null) {
      stmt = prepare(sql, whereargs, keys.length + 1);
    } else {
      stmt = connection.prepareStatement(sql);
    }
    runningStatement = stmt;
    int pos = 1;
    for(String key : keys) {
      Object val = values.get(key);
      if(val instanceof Integer) {
        stmt.setInt(pos, (Integer) val);
      } else if(val instanceof String) {
        stmt.setString(pos, (String) val);
      } else if(val instanceof Float) {
        stmt.setFloat(pos, (Float) val);
      } else if(val instanceof Long) {
        stmt.setLong(pos, (Long) val);
      } else if(val instanceof Boolean) {
        stmt.setBoolean(pos, (Boolean) val);
      } else if(val instanceof Double) {
        stmt.setDouble(pos, (Double) val);
      }

      pos++;
    }
    int res = stmt.executeUpdate();
    runningStatement = null;
    stmt.close();
    return res;
  }

  private String createInsert(int length) {
    String[] qmarks = new String[length];
    for(int i = 0; i < length; i++) {
      qmarks[i] = "?";
    }
    return String.join(", ", qmarks);
  }

  /**
   * Insert a row into the given table
   *
   * @param table  Table to insert into
   * @param values Values to insert. ID is removed because it should autoincrement.
   * @return ID of row inserted.
   * @throws SQLException
   */
  public int insert(String table, HashMap<String, Object> values) throws SQLException {
    values.remove(COLUMN_ID);
    String[] keys = values.keySet().toArray(new String[values.size()]);
    String sql = "INSERT INTO " + table + " ("
        + String.join(", ", keys)
        + ") VALUES (" + createInsert(keys.length) + ");";

    PreparedStatement stmt = connection.prepareStatement(sql);
    runningStatement = stmt;
    int pos = 1;
    for(String key : keys) {
      Object val = values.get(key);
      if(val instanceof Integer) {
        stmt.setInt(pos, (Integer) val);
      } else if(val instanceof String) {
        stmt.setString(pos, (String) val);
      } else if(val instanceof Float) {
        stmt.setFloat(pos, (Float) val);
      } else if(val instanceof Long) {
        stmt.setLong(pos, (Long) val);
      } else if(val instanceof Boolean) {
        stmt.setBoolean(pos, (Boolean) val);
      } else if(val instanceof Double) {
        stmt.setDouble(pos, (Double) val);
      }

      pos++;
    }
    stmt.executeUpdate();
    runningStatement = null;
    ResultSet rs = stmt.getGeneratedKeys();
    int id = rs.getInt("last_insert_rowid()");
    rs.close();
    stmt.close();
    return id;
  }

  /**
   * Insert an array of records fast. IDs are not set on the Record objects.
   *
   * @param items Records to insert
   * @throws SQLException
   */
  public synchronized void batchInsert(Record[] items, int limit) throws SQLException {
    if(limit == 0) return;
    Record template = items[0];
    HashMap<String, Object> values = template.getValues();
    values.remove(COLUMN_ID);
    ArrayList<String> keys = new ArrayList<>(values.keySet());
    Collections.sort(keys);

    StringBuilder sql = new StringBuilder("INSERT INTO " + template.getTableName() + " (" + String.join(", ", keys) + ") VALUES ");

    Object[] fastValues = new Object[keys.size()];
    for(int i = 0; i < limit; i++) {
      sql.append("(");
      Record rec = items[i];
      rec.fastValues(fastValues);
      boolean isFirst = true;
      for(Object val : fastValues) {
        if(!isFirst) {
          sql.append(",");
        }
        if(val instanceof Boolean) {
          sql.append((Boolean) val ? "1" : "0");
        } else if(!(val instanceof String)) {
          sql.append(val);
        } else {
          sql.append("?");
        }
        isFirst = false;
      }
      if(i < limit - 1){
        sql.append("), ");
      } else {
        sql.append(")");
      }
    }
    PreparedStatement stmt = connection.prepareStatement(sql.toString());
    runningStatement = stmt;
    int pos = 1;
    for(int i = 0; i < limit; i++) {
      Record r = items[i];
      r.fastValues(fastValues);
      for(Object val : fastValues) {
        if(val instanceof String) {
          stmt.setString(pos, (String) val);
          pos++;
        }
      }
    }
    stmt.execute();
    runningStatement = null;
    stmt.close();
  }

  /**
   * Drop a set of rows.
   *
   * @param table     Table to drop from.
   * @param where     Content for 'WHERE ...'
   * @param whereargs Values for ?s in where.
   * @return id       Number of rows dropped.
   * @throws SQLException
   */
  public int delete(String table, String where, Object[] whereargs) throws SQLException {
    String sql = "DELETE FROM " + table + (where == null ? "" : " WHERE " + where);
    PreparedStatement stmt = prepare(sql, whereargs, 1);
    runningStatement = stmt;
    int id = stmt.executeUpdate();
    runningStatement = null;
    stmt.close();
    return id;
  }

  /**
   * Perform an SQL query on the DB that returns a result
   *
   * @param sql    SQL query to run
   * @param values Values to insert into ?s
   * @return res    Result of query
   * @throws SQLException
   */
  public ResultSet rawQuery(String sql, Object[] values) throws SQLException {
    PreparedStatement stmt = prepare(sql, values, 1);
    runningStatement = stmt;
    ResultSet res = stmt.executeQuery();
    runningStatement = null;
    return res;
  }

  /**
   * Run an SQL statement that doesn't return any result.
   *
   * @param sql SQL to run, probably should be INSERT, UPDATE or CREATE.
   * @throws SQLException
   */
  public void runSql(String sql) throws SQLException {
    runningStatement = connection.createStatement();
    runningStatement.executeUpdate(sql);
    runningStatement = null;
  }

  /**
   * Called when the DB needs upgrading.
   * Database versions must be sequential integers that are greater than zero.
   */
  protected void upgrade() throws SQLException {
    int version = 0;
    HashRecord hr = new HashRecord(META_TABLE);
    try {
      hr.load(this);
      String strVersion = hr.get("db_version");
      if(strVersion != null && strVersion.matches("\\d+")) {
        version = Integer.parseInt(strVersion);
      }
    } catch (SQLException se) {
      version = -2;
    } catch (NumberFormatException nfe) {
      version = -1;
    }
    if(version == -2) {
      hr.createTable(this);
      version = -1;
    }
    switch(version + 1) {
      case 0: {
        // Do the database
      }
    }
    hr.put("db_version", Integer.toString(DATABASE_VERSION));
    hr.save(this);
  }

  /**
   * Cancel the currently running statement.
   * @return If the statement was cancelled.
   */
  public boolean cancel() {
    if(runningStatement != null) {
      try {
        runningStatement.cancel();
        runningStatement = null;
        return true;
      } catch(SQLException sven) {
        sven.printStackTrace();
        return false;
      }
    } else {
      return false;
    }
  }
}