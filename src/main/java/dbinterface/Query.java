package dbinterface;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
* Builds an SQL statement, executes on the database and returns the result.
*/
public class Query {
  private String where = null;
  private String[] whereargs = null;
  private String select = null;
  private String groupBy = null;
  private String orderBy = null;
  private String table = null;
  private DBInterface database = null;
  private int limit = DBInterface.ALL;
  private int offset = 0;
  private Class classType = null;

  /**
   * Create a query. null args are ignored when executed.
   * @param table Table to query from
   * @param database DBInterface to use
   * @param select Content for 'SELECT ...'
   * @param where Content for 'WHERE ...'
   * @param whereargs Values for ?s in WHERE
   * @param groupBy Content for 'GROUP BY ...'
   * @param orderBy Content for 'ORDER BY ...'
   */
  public Query(String table, DBInterface database, String select, String where, String[] whereargs, String groupBy, String orderBy) {
    this.table = table;
    this.where = where;
    this.whereargs = whereargs;
    this.select = select;
    this.groupBy = groupBy;
    this.orderBy = orderBy;
    this.database = database;
  }

  /**
   * Create a query on given DBInterface
   *
   * @param face DBInterface to use
   */
  public Query(DBInterface face) {
    this.database = face;
  }

  /**
   * Create a query on a dbinterface and table
   *
   * @param database DB to use
   * @param table Table to query on
   */
  public Query(DBInterface database, String table) {
    this.database = database;
    this.table = table;
  }

  /**
   * Create an empty query
   */
  public Query() {

  }

  /**
   * Set the DB
   *
   * @param database DB to use
   */
  public void setDatabase(DBInterface database) {
    this.database = database;
  }

  /**
   * Set the database
   *
   * @param database DB to use
   * @return this (for convenience)
   */
  public Query db(DBInterface database) {
    this.database = database;
    return this;
  }

  /**
   * Set where condition
   *
   * @param where Content for 'WHERE ...'
   * @param whereargs Values for ?s in WHERE
   * @return this (for convenience)
   */
  public Query where(String where, String... whereargs) {
    if(this.where != null) {
      return and(where, whereargs);
    }
    this.where = where;
    this.whereargs = whereargs;
    if(whereargs.length == 0) {
      this.whereargs = null;
    }
    return this;
  }

  private Query and(String where, String... whereargs) {
    this.where = "(" + this.where +  ") AND (" + where + ")";
    if(whereargs.length > 0) {
      if(this.whereargs == null || this.whereargs.length == 0) {
        this.whereargs = whereargs;
      } else {
        String[] newArgs = new String[whereargs.length + this.whereargs.length];
        for(int i = 0; i < this.whereargs.length; i++) {
          newArgs[i] = this.whereargs[i];
        }
        for(int i = 0; i < whereargs.length; i++) {
          newArgs[i + this.whereargs.length] = whereargs[i];
        }
        this.whereargs = newArgs;
      }
    }
    return this;
  }

  /**
   * Where with objects. toString() is called on each object.
   *
   * @param where Content for 'WHERE ...'
   * @param whereargs Values for ?s in WHERE
   * @return this (for convenience)
   */
  public Query where(String where, Object... whereargs) {
    if(this.where != null) {
      String[] newArgs = new String[whereargs.length];
      for(int i = 0; i < whereargs.length; i++) {
        newArgs[i] = whereargs[i].toString();
      }
      return and(where, newArgs);
    }
    this.where = where;
    this.whereargs = new String[whereargs.length];
    for(int i = 0; i < whereargs.length; i++) {
      this.whereargs[i] = whereargs[i].toString();
    }
    if(whereargs.length == 0) {
      this.whereargs = null;
    }
    return this;
  }

  /**
   * Set WHERE to be DBInterface.COLUMN_ID = ?
   * Set whereargs to be just the ID.
   *
   * @param id ID for where
   * @return this (for convenience)
   */
  public Query whereID(int id) {
    return where(DBInterface.COLUMN_ID + "=?", Integer.toString(id));
  }

  /**
   * Execute query and return ResultSet
   *
   * @return ResultSet of query
   * @throws SQLException
   */
  public ResultSet allCursor() throws SQLException {
    if(database == null) database = DBInterface.getGlobal();
    return database.query(table, select, where, whereargs, groupBy, orderBy, limit);
  }

  /**
   * Execute a query and limit it to the first result.
   *
   * @return ResultSet with one row of the query.
   * @throws SQLException
   */
  public ResultSet firstCursor() throws SQLException {
    if(database == null) database = DBInterface.getGlobal();
    return database.query(table, select, where, whereargs, groupBy, orderBy, 1);
  }

  /**
   * Set group by parameter of Query
   *
   * @param groupBy Content for 'GROUP BY ...'
   * @return this (for convenience)
   */
  public Query groupBy(String groupBy) {
    this.groupBy = groupBy;
    return this;
  }

  /**
   * Set order by parameter of Query
   *
   * @param orderBy Content for 'GROUP BY ...'
   * @return this (for convenience)
   */
  public Query orderBy(String orderBy) {
    this.orderBy = orderBy;
    return this;
  }

  /**
   * Set select parameter of Query
   *
   * @param select Content for 'SELECT ...'
   * @return this (for convenience)
   */
  public Query select(String select) {
    this.select = select;
    return this;
  }


  public Query from(String table) {
    this.table = table;
    return this;
  }

  public Query in(Class<? extends Record> cl) {
    classType = cl;
    return from(Record.getTableName(cl));
  }

  public Query in(String cl) {
    return from(cl);
  }

  public Query from(Class<? extends Record> cl) {
    classType = cl;
    return from(Record.getTableName(cl));
  }

  /**
   * Set the limit of rows to return
   * @param limit Max number of rows to return
   * @return this (for convenience)
   */
  public Query limit(int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * Insert hashmap values into the set table
   *
   * @param values Values to insert
   * @return ID of row inserted
   * @throws SQLException
   */
  public int insert(HashMap<String, Object> values) throws SQLException {
    return database.insert(table, values);
  }

  /**
   * Update using given WHERE clause, and other parameters
   *
   * @param values Values to set
   * @return Number of rows altered
   * @throws SQLException
   */
  public int update(HashMap<String, Object> values) throws SQLException {
    values.remove(DBInterface.COLUMN_ID);
    return database.update(table, values, where, whereargs);
  }

  /**
   * Update where COLUMN_ID = id
   *
   * @param id ID to update
   * @param values Values to set
   * @return Number of rows updated (Should be 1)
   * @throws SQLException
   */
  public int update(int id, HashMap<String, Object> values) throws SQLException {
    values.remove(DBInterface.COLUMN_ID);
    String where = DBInterface.COLUMN_ID + " = " + Integer.toString(id);
    return database.update(table, values, where, whereargs);
  }

  /**
   * Drop rows using given WHERE
   *
   * @return Number of rows dropped
   * @throws SQLException
   */
  public int drop() throws SQLException {
    return database.delete(table, where, whereargs);
  }

  /**
   * Drop where COLUMN_ID = id
   *
   * @param id ID to limit to
   * @return Number of rows dropped (Should be 1)
   * @throws SQLException
   */
  public int drop(int id) throws SQLException {
    String wh = DBInterface.COLUMN_ID + "=?";
    return database.delete(table, wh, new Object[]{id});
  }

  /**
   * Find by id in given table
   *
   * @param id ID to find
   * @return ResultSet with one row for selected record
   * @throws SQLException
   */
  public ResultSet findCursor(int id) throws SQLException {
    return where(DBInterface.COLUMN_ID + " = " + Integer.toString(id)).firstCursor();
  }

  /**
   * Run an SQL statement with escaped args that returns a result.
   *
   * @param sql SQL to execute
   * @param selectionArgs args to insert into ?s
   * @return ResultSet of query results
   * @throws SQLException
   */
  public ResultSet sql(String sql, Object... selectionArgs) throws SQLException {
    return database.rawQuery(sql, selectionArgs);
  }

  public <T extends Record> ArrayList<T> all(Class<T> cl) throws SQLException {
    ResultSet c = allCursor();
    ArrayList<T> ar = new ArrayList<>();
    while((c.next())) {
      try {
        T l = cl.newInstance();
        l.setFromCursor(c, false);
        ar.add(l);
      } catch (InstantiationException ie) {
        ie.printStackTrace();
      } catch (IllegalAccessException iae) {
        iae.printStackTrace();
      }
    }
    c.close();
    return ar;
  }

  public <T extends Record> ArrayList<T> all() throws SQLException {
    if(classType != null){
      return all(classType);
    }
    return null;
  }

  public <T extends Record> T find(int id) throws SQLException {
    if(classType != null) {
      ResultSet c = findCursor(id);
      try {
        T l = (T) classType.newInstance();
        l.setFromCursor(c, true);
        c.close();
        return l;
      } catch (InstantiationException ie) {
        ie.printStackTrace();
      } catch (IllegalAccessException iae) {
        iae.printStackTrace();
      }
    }
    return null;
  }

  public <T extends Record> T first() throws SQLException {
    if(classType != null) {
      ResultSet c = firstCursor();
      try {
        T l = (T) classType.newInstance();
        l.setFromCursor(c, true);
        c.close();
        return l;
      } catch (InstantiationException ie) {
        ie.printStackTrace();
      } catch (IllegalAccessException iae) {
        iae.printStackTrace();
      }
    }
    return null;
  }

  public int count() throws SQLException {
    return count("*");
  }

  public int count(String column) throws SQLException {
    return (Integer) scalar("count(" + column + ")");
  }

  public Object max(String column) throws SQLException {
    return scalar("max(" + column + ")");
  }

  public Object min(String column) throws SQLException {
    return scalar("min(" + column + ")");
  }

  public Object sum(String column) throws SQLException {
    return scalar("sum(" + column + ")");
  }

  public Object scalar(String function) throws SQLException {
    select = function;
    ResultSet rs = firstCursor();
    Object res = rs.getObject(1);
    rs.close();
    return res;
  }
}
