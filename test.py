import sys
from table_queries import TableQueries
from file_queries import FileQueries

queries = TableQueries("http://localhost:8080")

fqueries = FileQueries("http://localhost:8080")

def test_create_db_and_table():
    """
    Creates a database and 1 table in that database.
    Then, tries to get that table.
    """
    queries.create_database("test_db")
    queries.create_table("test_db", "test_table", {"col_1": "string", "col_2": "integer"})
    print(queries.get_database("test_db"))
    print("Tables: " + str(queries.get_all_tables("test_db")))
    print(queries.get_table("test_table"))

def test_update_table():
    """
    Creates a table within a database. Then updates the table.
    The get_table after the update should reflect the update.
    """
    queries.create_database("test_db")
    queries.create_table("test_db", "test_table", {"col_1": "string", "col_2": "integer"})
    print(queries.get_database("test_db"))
    print("Tables: " + str(queries.get_all_tables("test_db")))
    print(queries.get_table("test_table"))
    queries.update_table("test_table", {"new_col1": "boolean", "new_col2": "string"})
    print(queries.get_table("test_table"))

def test_create_multiple_tables():
    """
    Creates 2 tables within one database. Get all tables call should
    return both tables in the database.
    """
    queries.create_database("test_db")
    queries.create_table("test_db", "test_table1", {"col_1": "string", "col_2": "integer"})
    queries.create_table("test_db", "test_table2", {"column1": "boolean", "column2": "string", "column3": "integer"})
    print(queries.get_database("test_db"))
    print("Tables: " + str(queries.get_all_tables("test_db")))

def test_multiple_tables_drop_one():
    """
    Creates 2 tables within a database, then drops one of them.
    Get all tables call should return the 1 not dropped table.
    """
    queries.create_database("test_db")
    queries.create_table("test_db", "test_table1", {"col_1": "string", "col_2": "integer"})
    queries.create_table("test_db", "test_table2", {"column1": "boolean", "column2": "string", "column3": "integer"})
    print(queries.get_database("test_db"))
    queries.drop_table("test_table2")
    print(queries.get_database("test_db"))
    print("Tables: " + str(queries.get_all_tables("test_db")))
    print(queries.get_table("test_table2"))

def test_drop_db_try_to_create_table():
    """
    Creates a database and table, then drops the database.
    Attempts to create a table in the dropped database; should fail.
    """
    queries.create_database("test_db")
    queries.create_table("test_db", "test_table", {"col_1": "string", "col_2": "integer"})
    queries.drop_database("test_db")
    queries.create_table("test_db", "test", {"column": "boolean"})

def test_create_directory_nested():
    """
    Creates a directory. Then creates a directory within that directory
    """
    fqueries.create_directory("/")
    fqueries.create_directory("/dir1")

def test_create_file():
    """
    Creates a file within the root directory
    """
    fqueries.create_directory("/")
    fqueries.create_file("/test_file", {"size": "20MB", "date_modified": "10-25-2016", "kind": "PDF"})
    print(fqueries.get_file("/test_file"))

def test_create_file_within_2_dirs():
    """
    Creates the root directory, then another directory within the root.
    Then, creates a file in the nested directory
    """
    fqueries.create_directory("/")
    fqueries.create_directory("/dir")
    fqueries.create_file("/dir/file", {"size": "20MB", "date_modified": "10-25-2016", "kind": "PDF"})
    print(fqueries.get_file("/dir/file"))

def test_ls():
    """
    Creates a directory with a file and directory inside it,
    then calls 'ls'
    """
    fqueries.create_directory("/")
    fqueries.create_directory("/test_dir")
    fqueries.create_file("/test_file", {"size": "20MB", "date_modified": "10-25-2016", "kind": "PDF"})
    print(fqueries.ls("/"))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "To run a test, check test.py for possible tests"
    else:
        test = sys.argv[1]
        if test == "create_db_and_table":
            test_create_db_and_table()
        elif test == "update_table":
            test_update_table()
        elif test == "create_multiple_tables":
            test_create_multiple_tables()
        elif test == "multiple_tables_drop_one":
            test_multiple_tables_drop_one()
        elif test == "drop_db_try_to_create_table":
            test_drop_db_try_to_create_table()
        elif test == "create_file":
            test_create_file()
        elif test == "create_directory_nested":
            test_create_directory_nested()
        elif test == "create_file_within_2_dirs":
            test_create_file_within_2_dirs()
        elif test == "ls":
            test_ls()
