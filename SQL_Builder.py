import psycopg2
from psycopg2 import sql
import os
import csv

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

def is_float(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def get_suggest_type(path_f, data_names):
    dict_suggest = {name : [] for name in data_names}
    dict_data = {name : [] for name in data_names}
    with open(path_f, newline='', encoding="utf-8") as csv_f:
        lines = csv.reader(csv_f, delimiter=',', quotechar='"')
        lines = list(lines)[1:]
        for line in lines:
            for idx, elem in enumerate(line):
                dict_data[data_names[idx]].append(elem)

    list_pg_bool = ["TRUE", "t", "true", "y", "yes", "on", "1", "FALSE", "f", "false", "n", "no", "off", "0"]
    for name in data_names:
        types = {"INT": True, "BIGINT":True, "VARCHAR":True, "BOOL":True, "SERIAL":True, "DECIMAL":True}
        size_varchar = 0
        size_deci = 0
        nb_deci = 0
        for elem in dict_data[name]:
            if types["INT"] and (not elem.isnumeric() or abs(int(elem)) > 2**31-1):
                types["INT"] = False
            if types["BIGINT"] and (not elem.isnumeric() or abs(int(elem) > 2**63-1)):
                types["BIGINT"] = False
            if types["VARCHAR"] and int(size_varchar) < len(elem) :
                size_varchar = len(elem)
                if size_varchar > 10485760:
                    types["VARCHAR"] = False
            if types["BOOL"] and elem not in list_pg_bool:
                types["BOOL"] = False
            if types["SERIAL"] and elem != "DEFAULT" and (not elem.isnumeric() or int(elem) >= 2*31-1 or int(elem) < 1):
                types["SERIAL"] = False
            if types["DECIMAL"]:
                if not is_float(elem) or len(elem.split(".")) != 2 :
                    types["DECIMAL"] = False
                else:
                    if len(elem) > size_deci:
                        size_deci = len(elem)-1
                    elem = elem.split(".")
                    print(elem)
                    if len(elem[1]) > nb_deci:
                        nb_deci = len(elem[1])
                    if (size_deci - nb_deci) > 131072 or nb_deci > 16383:
                        types["DECIMAL"] = False
        for type in types:
            if types[type]:
                dict_suggest[name].append(type)
                if type == "VARCHAR":
                    dict_suggest[name][-1] += f"({size_varchar})"
                if type == "DECIMAL":
                    dict_suggest[name][-1] += f"({size_deci}, {nb_deci})"
    return dict_suggest
                
def waiting_input(msg : str, dict_data : dict, data_names : list, idx_column : int):
    while True:
        value = input(f"\n{msg}\n\n")
        if value.isnumeric():
            dict_data[data_names[idx_column]].append(value)
            break;
        else:
            print("\nINVALID ENTRY\n")


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
    print(f"file : {file}")
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
            print("\nList column :")
            print_listing(dict_data)
            print("\nCurrent column :\n"+data_names[idx_column])
            res = input("\nMenu: (1) \n 1. next \n 2. do it for all \n 3. undo\n 4. suggest\n\n")
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
            elif res == '4':
                dict_suggest = get_suggest_type(filepath, data_names)
                print("\nSuggest:")
                for key in dict_suggest:
                    msg = f"{key} : "
                    for type in dict_suggest[key]:
                        msg += f"{type} "
                    print(f"{msg}")
                continue
            else:
                continue

            list_action.append([])
            data_type = input("\nDataType? \n 1. INT \n 2. BIGINT \n 3. VARCHAR \n 4. BOOL \n 5. SERIAL \n 6. DECIMAL\n\n")
            if data_type.isnumeric() and data_type in types:
                dict_data[data_names[idx_column]].append(types[data_type])
                if types[data_type] == "VARCHAR":
                    waiting_input("Max size of string", dict_data, data_names, idx_column)
                if types[data_type] == "DECIMAL":
                    waiting_input("Total number size", dict_data, data_names, idx_column)
                    waiting_input("Precision", dict_data, data_names, idx_column)
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

    SQL_data = []
    for col in dict_data:
        if dict_data[col][0] == "VARCHAR":
            SQL_data.append(f"{col} {dict_data[col][0]}({dict_data[col][1]})")
        elif dict_data[col][0] == "DECIMAL":
            SQL_data.append(f"{col} {dict_data[col][0]}({dict_data[col][1]}, {dict_data[col][2]})")
        else:
            SQL_data.append(f"{col} {dict_data[col][0]}")

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
