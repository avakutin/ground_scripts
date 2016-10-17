from table_queries import TableQueries

queries = TableQueries("http://localhost:8080")
queries.create_database("test_db")
queries.create_table("test_db", "test_table", {"col_1": "string", "col_2": "integer"})
print("Tables: " + queries.get_all_tables("test_db"))
print(queries.get_table("test_table"))
