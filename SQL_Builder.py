import psycopg2
from psycopg2 import sql
import os


def replace_invalid_characters(name):
    return ''.join(e for e in name if e.isalnum())

def res_do_it_for_all():
    res = input("\nDo it for all ? [y/N] \n\n")
    if res.upper() == "Y":
        print("\nTHIS CHOICE WILL AFFECT ALL OTHER COLUMNS ON THIS LIST\n\n")
        return True
    return False

def print_listing(listing : dict):
    for key in listing:
        print(key + " - " + str(listing[key]))


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

    dict_data = {replace_invalid_characters(column) : [] for column in line.split(',')}

    if len(dict_data) == 0:
        print("header not found => next file")
        continue

    data_names = list(dict_data.keys())
    list_action = [[]]
    types = {'1':"INT", '2':"BIGINT", '3':"VARCHAR", '4':"BOOL", '5':"SERIAL", '6':"DECIMAL"}
    diff = ["VARCHAR"]
    do_it_for_all = False
    idx_column = 0




    while True:
            print("\nList column :\n")
            print_listing(dict_data)
            print("\nCurrent column :\n"+data_names[idx_column]+"\n")
            res = input("\nMenu: (1) \n 1. next \n 2. do it for all \n 3. undo\n\n")
            if res == '1' or res == '':
                do_it_for_all = False
            elif res == '2':
                do_it_for_all = True
            elif res == '3':
                if(len(list_action) == 0):
                    print("cannot execute undo")
                    continue
                for key in list_action[-1]:
                    dict_data[key] = []
                    idx_column -= 1
                list_action.pop(-1)
                continue
            else:
                continue

            list_action.append([])
            data_type = input("DataType? \n 1. INT \n 2. BIGINT \n 3. VARCHAR \n 4. BOOL \n 5. SERIAL \n 6. DECIMAL\n\n")
            if data_type.isnumeric() and data_type in types:
                dict_data[data_names[idx_column]].append(types[data_type])
                if types[data_type] == "VARCHAR":
                    while True:
                        max_size = input("\nMax size of string?\n\n")
                        if max_size.isnumeric():
                            dict_data[data_names[idx_column]].append(max_size)
                            break;
                        else:
                            print("\nINVALID ENTRY\n")
                list_action[-1].append(data_names[idx_column])
                idx_column += 1
            else:
                print('\nINVALID ENTRY\n')
                continue    
            
            if do_it_for_all:
                idx_tmp = idx_column-1
                while idx_column != len(data_names):
                    dict_data[data_names[idx_column]] = dict_data[data_names[idx_tmp]]
                    list_action[-1].append(data_names[idx_column])
                    idx_column += 1
                    
            
            if idx_column == len(data_names):
                res = input("\nAll defined (1):\n1. Finish\n2. Undo\n\n")
                if res == '2':
                    for key in list_action[-1]:
                        dict_data[key] = []
                        idx_column -= 1
                    list_action.pop(-1)
                else:
                    break;
            
            


    SQL_data = [f"{col} {dict_data[col][0]}" if len(dict_data[col]) == 1 else f"{col} {dict_data[col][0]}({dict_data[col][1]})" for col in dict_data]

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
