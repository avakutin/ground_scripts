from table_queries import TableQueries

queries = TableQueries("http://localhost:8080")
queries.create_database("test_db")
queries.create_table("test_db", "test_table", {"col_1": "string", "col_2": "integer"})
print(queries.get_database("test_db"))
print("Tables: " + str(queries.get_all_tables("test_db")))
print(queries.get_table("test_table"))
queries.update_table("test_table", {"new_col1": "boolean", "new_col2": "string"})
print(queries.get_table("test_table"))
# queries.drop_table("test_table")
# print(queries.get_table("test_table"))
