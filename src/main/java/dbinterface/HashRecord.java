package dbinterface;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Class for storing String key/ value pairs as a DB table.
*/

public class HashRecord {
  private boolean saved = false;
  private HashMap<String, StoredValue> values;
  private String tableName;

  /**
   * Create HashRecord from ResultSet (Which should be from a .all() query)
   *
   * @param tableName name of the table to use
   * @param c ResultSet to load values from
   * @throws SQLException
   */
  public HashRecord(String tableName, ResultSet c) throws SQLException {
    this.tableName = tableName;
    values = new HashMap<String, StoredValue>();
  }

  /**
   * Create an empty HashRecord
   *
   * @param tableName Name of table to save to
   */
  public HashRecord(String tableName) {
    this.tableName = tableName;
    values = new HashMap<String, StoredValue>();
  }

  /**
   * Load from the set table into the HashMap
   *
   * @param dbi DBInterface to query with
   * @throws SQLException
   */
  public void load(DBInterface dbi) throws SQLException {
    load(new Query(dbi).from(tableName).allCursor());
  }

  /**
   * Load values from ResultSet
   *
   * @param rs ResultSet with values
   * @throws SQLException
   */
  public void load(ResultSet rs) throws SQLException {
    while(rs.next()) {
      values.put(rs.getString("key"), new StoredValue(rs.getString("value"), StoredValue.LOADED_MODE));
    }
    saved = true;
  }

  /**
   * Output the HashMap to the database, in the table given in the constructor
   *
   * @param dbi DBInterface to use.
   * @throws SQLException
   */
  public void save(DBInterface dbi) throws SQLException {
    for(String key : values.keySet()) {
      StoredValue stv = values.get(key);
      if(stv.hasMode(StoredValue.REMOVED_MODE)) {
        new Query(dbi).from(tableName).where("key=?", key).drop();
      } else if(stv.hasMode(StoredValue.EDITED_MODE)) {
        HashMap<String, Object> updater = new HashMap<String, Object>();
        updater.put("value", stv.value);
        new Query(dbi).from(tableName).where("key=?", key).update(updater);
      } else if(stv.hasMode(StoredValue.NEW_MODE)) {
        HashMap<String, Object> inserter = new HashMap<String, Object>();
        inserter.put("key", key);
        inserter.put("value", stv.value);
        new Query(dbi).from(tableName).insert(inserter);
      }
    }
    saved = true;
  }

  /**
   * Removes the key/value pair from the HashMap.
   * Does not remove from the DB until the HashRecord is saved.
   *
   * @param key Key to remove
   * @return If a value was removed.
   * @throws SQLException
   */
  public boolean remove(String key) throws SQLException {
    StoredValue stv = values.get(key);
    if(stv != null) {
      stv.toggleMode(StoredValue.REMOVED_MODE);
      saved = false;
    }
    return stv != null;
  }

  /**
   * Insert a value into the HashMap
   * Is not put into the DB until save() is called.
   *
   * @param key Key to store with
   * @param value Value to store
   */
  public void put(String key, String value) {
    int mode = StoredValue.NEW_MODE;
    if(values.containsKey(key)) {
      mode = StoredValue.EDITED_MODE;
    }
    StoredValue stv = new StoredValue(value, mode);
    values.put(key, stv);
    saved = false;
  }

  /**
   * Get a value from the HashMap
   * null if the value is non-existent or removed
   *
   * @param key Key of value to get
   * @return The corresponding value of the key, otherwise null.
   */
  public String get(String key) {
    StoredValue stv = values.get(key);
    if(stv != null && !stv.hasMode(StoredValue.REMOVED_MODE)) {
      return stv.value;
    } else {
      return null;
    }
  }

  /**
   * Check if there is a value in the HashMap for a given key
   *
   * @param key Key to check
   * @return true if there is a value for the key, false otherwise
   */
  public boolean has(String key) {
    return values.containsKey(key) && !values.get(key).hasMode(StoredValue.REMOVED_MODE);
  }

  /**
   * Check if there are unsaved changes in the HashMap
   *
   * @return true if changes have been saved, false otherwise.
   */
  public boolean isSaved() {
    return saved;
  }

  /**
   * Create a table for this hashrecord to save into. Drops table if it already exists.
   * @param dbi DBI to create table in
   * @throws SQLException
   */
  public void createTable(DBInterface dbi) throws SQLException {
    dbi.runSql("DROP TABLE IF EXISTS " + tableName + "; CREATE TABLE " + tableName + " (key varchar(50), value text);");
  }

  private class StoredValue {
    public static final int LOADED_MODE = 0;
    public static final int NEW_MODE = 1;
    public static final int REMOVED_MODE = 2;
    public static final int EDITED_MODE = 4;

    public String value;
    private int mode = 0;

    public StoredValue(String val, int mode) {
      value = val;
      this.mode = mode;
    }

    public boolean hasMode(int mode) {
      return (mode & this.mode) > 0;
    }

    public void toggleMode(int mode) {
      this.mode ^= mode;
    }

    public String toString() {
      return this.value + " : " + Integer.toString(this.mode);
    }
  }
}