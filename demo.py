schema_name = "new_schema"
database_name = "new_database"
role_name = "new_role"

with open('Template.sql', 'r') as file:
    sql_script = file.read()

sql_script = sql_script.replace("{{schema_name}}", schema_name)
sql_script = sql_script.replace("{{database_name}}", database_name)
sql_script = sql_script.replace("{{role_name}}", role_name)

print(sql_script)
