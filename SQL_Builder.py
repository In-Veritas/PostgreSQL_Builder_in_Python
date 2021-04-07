import psycopg2
from psycopg2 import sql
import os


def replace_invalid_characters(name):
    return ''.join(e for e in name if e.isalnum())

def res_do_it_for_all():
    res = input("\n\x1b[32mDo it for all ? [y/N] \n\n\x1b[39m")
    if res.upper() == "Y":
        print("\n\x1b[41mTHIS CHOICE WILL AFFECT ALL OTHER COLUMNS ON THIS LIST\x1b[49m\n\n")
        return True
    return False


print("If you wish to make it faster, change the entries directly on the code.")
your_database = input("Insert Database: ")
your_user = input("Insert User: ")
your_password = input("Insert Password: ")
your_host = input("Insert Host: ")
your_port = input("Insert Port: ")

# This script will iterate with all CSV files in a folder. DO NOT leave any non CSV files in your folder.
path = "C:/YourPath"   #Insert Path of your folder

for file in os.listdir(path):
    if not file.endswith('.csv'):
        continue
    con = psycopg2.connect(database=your_database, user=your_user, password=your_password, host=your_host, port=your_port)   #To change connection settings manually, change them here
    cur = con.cursor()
    SQL_data = []
    print(file)
    filepath = path + file
    f = open(filepath)
    line = f.readline()
    listing = line.split(',')

    types = {'1':"INT", '2':"BIGINT", '3':"VARCHAR", '4':"BOOL", '5':"SERIAL", '6':"DECIMAL"}
    diff = ["VARCHAR"]
    do_it_for_all = False
    data_type = 0

    for column in listing:
        while True:
            if not do_it_for_all:
                print(column)
                data_type = int(input("\x1b[32mDataType? \n 1. INT \n 2. BIGINT \n 3. VARCHAR \n 4. BOOL \n 5. SERIAL \n 6. DECIMAL\x1b[39m\n\n"))
            if data_type in types:
                if types[data_type] not in diff:
                    SQL_data.append(replace_invalid_characters(column) + " " + types[data_type] )
                    if not do_it_for_all:
                        do_it_for_all = res_do_it_for_all();
                    break;
                elif types[data_type] == "VARCHAR":
                    while True:
                        if not do_it_for_all :
                            max_size = input("\n\x1b[32mMax size of string?\x1b[39m\n\n")
                        if max_size.isnumeric():
                            SQL_data.append(replace_invalid_characters(column) + " VARCHAR(" + str(max_size) + ")")
                            if not do_it_for_all:
                                do_it_for_all = res_do_it_for_all();
                            break;
                        else:
                            print("\x1b[41mINVALID ENTRY\x1b[49m")
                    break;
            else:
                print('\x1b[41mINVALID ENTRY\x1b[49m')    

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

