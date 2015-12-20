package dbinterface;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public abstract class Record {
  private int id;

  /**
   * Creates a new Record from a ResultSet, closes the RS after completion.
   * @param c ResultSet to load from
   * @throws SQLException
   */
  public Record(ResultSet c) throws SQLException {
    setFromCursor(c, true);
  }

  /**
   * Creates a blank record with an id of DEFAULT_ID
   */
  public Record() {
    id = DBInterface.DEFAULT_ID;
  }

  /**
   * Gets a HashMap of the values of this object that should be stored in the DB.
   * Used when saving into the database, calls abstract method insertValues() to get fields from
   * subclasses.
   * @return map a HashMap of of key/values.
   */
  public HashMap<String, Object> getValues() {
    HashMap<String, Object> map = new HashMap<>();
    map.put(DBInterface.COLUMN_ID, id);
    insertValues(map);
    return map;
  }

  /**
   * Loads fields from ResultSet and sets the correct params.
   * Calls setValues() to set values in subclass.
   * @param c ResultSet to use
   * @param closeAfter whether to close the RS after using it.
   * @throws SQLException
   */
  public void setFromCursor(ResultSet c, boolean closeAfter) throws SQLException {
    setID(c.getInt(DBInterface.COLUMN_ID));
    setValues(c);
    if(closeAfter) c.close();
  }

  /**
   * Called to set properties of a subclass when a Record is loaded.
   * @param rs ResultSet to get values from
   * @throws SQLException
   */
  abstract protected void setValues(ResultSet rs) throws SQLException;

  /**
   * Called to get properties from subclass that should be saved into the DB.
   * @param map HashMap to store the values in.
   */
  abstract protected void insertValues(HashMap<String, Object> map);

  /**
   * Overridden to provide a faster way to insert items in a batch.
   * Attributes should be sorted alphabetically when inserted.
   * @param container To put values in. Will have correct size, hopefully.
   */
  abstract public void fastValues(Object[] container);

  /**
   * Gets the record ID, -1 (DBInterface.DEFAULT_ID) if the record is not saved.
   * @return ID the record id
   */
  public int getID() {
    return id;
  }

  private void setID(int id) {
    this.id = id;
  }

  /**
   * Saves the record into its table using the given DBInterface
   * @param database DBInterface to use
   * @throws SQLException
   */
  public void save(DBInterface database) throws SQLException {
    if(getID() == -1) {
      setID(new Query(database).in(getClass()).insert(getValues()));
    } else {
      new Query(database).in(getClass()).update(getID(), getValues());
    }
  }

  public void save() throws SQLException {
    save(DBInterface.getGlobal());
  }

  /**
   * Deletes the record from the database.
   * @param database database
   * @return 0 if record doesn't exist, 1 otherwise.
   * @throws SQLException
   */
  public int drop(DBInterface database) throws SQLException {
    if(getID() == -1) {
      return 0;
    }
    return new Query(database).in(getClass()).drop(getID());
  }

  public int drop() throws SQLException {
    return drop(DBInterface.getGlobal());
  }
  /**
   * Checks whether this is a new record that is yet to be inserted.
   * @return true if saved, false otherwise.
   */
  public boolean isSaved() {
    return getID() > -1;
  }

  /**
   * Check whether this object is the same row as another object by comparing IDs
   * @param o object to compare with.
   * @return true if the two objects are equal, false otherwise.
   */
  @Override
  public boolean equals(Object o) {
    if(o instanceof Record) {
      return getID() == ((Record) o).getID();
    }
    return super.equals(o);
  }

  /**
   * Represent this record as a string, default is 'Record: ID'
   * @return String representation
   */
  @Override
  public String toString() {
    return getClass().getSimpleName() + " id: " + Integer.toString(getID());
  }
  public static String getTableName(Class<? extends Record> cl) {
    return cl.getSimpleName().toLowerCase();
  }

  public String getTableName() {
    return getTableName(getClass());
  }
}
