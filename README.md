# Active-JDBC

Wrap up Java SQLite databases like it's ActiveRecord.

Kind of like [my other one](https://github.com/JavaNut13/Android-DB-Interface), but a bit smarter and for JDBC, rather than the built-in Android one.

## How to:

Clone the repo, chuck the _dbinterface_ folder into your project (`Record`, `DBInterface`, `HashRecord` and `Query` are required). Create a class for each table in your database, that extends `Record`. The name of the table should eb the same as the class name.

In the DBInterface `upgrade()` method, run SQL to create your tables and other initialisation - depending on the existing version of the database.

## Open the Database

Open the database - either in memory or in a `.db` file:

    DBInterface dbi = DBInterface().open(); // open in memory
    // OR
    DBInterface dbi = DBInterface(new File("/Users/me/data.db"));

If you always want to use this database and never have to reference it, make it global:

    dbi.globalize();

This means that updates and lookups do not have to be passed a database reference. When you're done with the database, close it:

    dbi.close();

## Creating records

Create a Location (Assuming `Location` is a subclass of `Record`):

    // Create the object
    Location place = new Location();
    // Set it's attributes
    place.setName("My place");
    place.setLocation("123.456, -12.5");
    // DBInterface manages the connection to the database
    // This will create a record in the DB
    place.save(dbi);
    // If dbi is set as global, we don't have to reference dbi when saving
    place.save();
    // Don't forget to close the database when you're finished with it.
    dbi.close();

Get a single location by ID:

    Location ml = new Query(dbi).from(Location.class).find(locationId);
    // Do stuff to ml and then save it if required
    ml.save();
    dbi.close();

Get a list of people:

    ArrayList<Person> people = new Query(dbi).in(Person.class).where("name LIKE ?", search).all();
    // use people for something handy
    
> `in()` and `from()` can either be passed a string specifying the FROM clause, or given a `Record` that it will get the table name from.

## Scalar functions

Queries that just need to get a single value (count, min, max, sum) can use the built in scalar operators, or the `scalar()` function. These will return an Object with the value that would be selected by the query. This may be a Float or an Integer, depending on the data types of the table.

This can also be used to get a single column of a single row:

    String name = (String) new Query(dbi).from(Person.class).where("id=?", personId).scalar("name");

