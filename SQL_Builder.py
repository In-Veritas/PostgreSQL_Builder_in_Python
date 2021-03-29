import psycopg2
from psycopg2 import sql
import os


def replace_invalid_characters(name):
    return ''.join(e for e in name if e.isalnum())

print("If you wish to make it faster, change the entries directly on the code.")
your_database = input("Insert Database")
your_user = input("Insert User")
your_password = input("Insert Password")
your_host = input("Insert Host")
your_port = input("Insert Port")

# This script will iterate with all CSV files in a folder. DO NOT leave any non CSV files in your folder.
path = "C:/YourPath"   #Insert Path of your folder
for file in os.listdir(path):
    if not file.endswith('.csv'):
        continue
    con = psycopg2.connect(database=your_database, user=your_user, password=your_password, host=your_host, port=your_port)   #To change connection settings manually, change them here
    cur = con.cursor()
    SQL_data = []
    print(file)
    filepath = 'C:/Users/Public/ONTOLOGIES/' + file
    f = open(filepath)
    line = f.readline()
    listing = line.split(',')
    
    for column in listing:
        valid_number = False
        while not valid_number:
            print(column)
            print("DataType? \n 1. INT \n 2. BIGING \n 3. VARCHAR \n 4. BOOL \n 5. SERIAL \n 6. DECIMAL")
            data_type = int(input())
            if data_type == 1:
                SQL_data.append(replace_invalid_characters(column) + " INT")
                valid_number = True
            elif data_type == 2:
                SQL_data.append(replace_invalid_characters(column) + " BIGINT")
                valid_number = True
            elif data_type == 3:
                valid_text = False
                while not valid_text:
                    max_size = input("Max size of string? \n")
                    if max_size.isnumeric():
                        SQL_data.append(replace_invalid_characters(column) + " VARCHAR(" + str(max_size) + ")")
                        valid_text = True
                        valid_number = True
                    else:
                        print("INVALID ENTRY")
            elif data_type == 4:
                SQL_data.append(replace_invalid_characters(column) + " BOOL")
                valid_number = True
            elif data_type == 5:
                SQL_data.append(replace_invalid_characters(column) + " SERIAL")
                valid_number = True
            elif data_type == 6:
                SQL_data.append(replace_invalid_characters(column) + " DECIMAL")
                valid_number = True
            else:
                print('INVALID ENTRY')    

    #After the iteration, the script will build a PostgreSQL query based on the information given
    string_SQL_data = ','.join(SQL_data)
    query = sql.SQL("CREATE TABLE {some_table} ({table_data});").format(
        some_table=sql.SQL(os.path.splitext(file)[0]),
        table_data=sql.SQL(string_SQL_data))
    cur.execute(query) 
    string_SQL_data = []
    con.commit()
    query = sql.SQL("COPY {some_table} FROM '{original_file}' DELIMITER ',' CSV HEADER ENCODING 'UTF8';").format(
        some_table=sql.SQL(os.path.splitext(file)[0]),
        original_file=sql.SQL(filepath))
    cur.execute(query) 
    con.commit()
    con.close()

