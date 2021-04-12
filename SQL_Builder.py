import psycopg2
from psycopg2 import sql, errors
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

def create_from_csv(path_sql, path_csv, con, cur):
    files = os.listdir(path_csv)
    print(f"\nChoose the file ({len(files)+2}):")
    for i in range(len(files)):
        print(f"{i+1}. {files[i]}")
    if len(files) >= 2:
        print(f"{len(files)+1}. All of them")
    print(f"{len(files)+2}. Cancel\n")
    res = input()
    selected_files = []
    if not res.isnumeric() or int(res)<1 or int(res)>len(files)+1:
        return
    if len(files) >= 2 and int(res) == len(files)+1:
        selected_files = files
    else:
        selected_files.append(files[int(res)-1])

    for file in selected_files:
        if not file.endswith('.csv'):
            continue
        print(f"file : {file}")
        filepath = path_csv + file
        f = open(filepath)
        line = f.readline()

        dict_data = {replace_invalid_characters(column) : [] for column in line.split(',')}
        if len(dict_data) == 0:
            print("header not found => next file")
            continue

        data_names = list(dict_data.keys())

        list_action = [[]]
        do_it_for_all = False
        idx_column = 0
        dict_suggest, dict_wnis = get_suggest_type(filepath, data_names)
        print(dict_suggest)
        print(dict_wnis)
        while True:
                print("\nList column :")
                print_listing(dict_data)
                print("\nCurrent column :\n"+data_names[idx_column])
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
                types = {}
                for idx,dtype in enumerate(dict_suggest[data_names[idx_column]]):
                    dtype = dtype.split("-")
                    types[str(idx+1)] = [elem for elem in dtype]
                
                while True:
                    print("\nDataType?")
                    for idx in types:
                        print(f"\n{idx}. {types[idx][0]}")
                    data_type = input("\nx. Why not the others ?\n\n")
                    if data_type.isnumeric() and data_type in types:
                        dict_data[data_names[idx_column]].append(types[data_type][0])
                        if types[data_type][0] == "VARCHAR":
                            waiting_input(f"Max size of string, min:{types[data_type][1]}", dict_data, data_names, idx_column, types[data_type][1] )
                        if types[data_type][0] == "DECIMAL":
                            waiting_input(f"Total number size, min:{types[data_type][2]}", dict_data, data_names, idx_column, types[data_type][2])
                            waiting_input(f"Precision, min:{types[data_type][3]}", dict_data, data_names, idx_column, types[data_type][3])
                        list_action[-1].append(data_names[idx_column])
                        idx_column += 1
                        break;
                    elif data_type.upper() == "X":
                        for elem in dict_wnis[data_names[idx_column]].values():
                            print(elem) 
                    else:
                        print('\nINVALID ENTRY\n')
                
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
        table_name = file[:-4]

        res = input("\nFinal step (1):\n1. Create table and import data\n2. Create table without data\n3. Export table without data\n4. Export table with data\n5. Cancel\n\n")
        if res == "2":
            execute_sql(sql.SQL(f"CREATE TABLE {table_name} ({string_SQL_data});"), con, cur)
        elif res == "3":
            export_sql(f"CREATE TABLE {table_name} ({string_SQL_data});\n", f"{path_sql}{table_name}.sql", "w")
        elif res == "4":
            export_sql(f"CREATE TABLE {table_name} ({string_SQL_data});\n", f"{path_sql}{table_name}.sql", "w")
            export_sql(f"COPY {table_name} FROM '{path_csv}{table_name}.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';\n", f"{path_sql}{table_name}.sql", "a")
        elif res == "5":
            continue
        else:
            execute_sql(f"CREATE TABLE {table_name} ({string_SQL_data});COPY {table_name} FROM '{path_csv}{table_name}.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';\n", con, cur)


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
    dict_why_not_in_suggest = {name : {} for name in data_names}
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
        for idx,elem in enumerate(dict_data[name]):
            if types["INT"] and (not elem.isnumeric() or abs(int(elem)) > 2**31-1):
                types["INT"] = False
                dict_why_not_in_suggest[name]["INT"] = f"INT : line {idx} : {elem}"
            if types["BIGINT"] and (not elem.isnumeric() or abs(int(elem) > 2**63-1)):
                types["BIGINT"] = False
                dict_why_not_in_suggest[name]["BIGINT"] = f"BIGINT : line {idx} : {elem}"
            if types["VARCHAR"] and int(size_varchar) < len(elem):
                size_varchar = len(elem)
                if size_varchar > 10485760:
                    types["VARCHAR"] = False
                    dict_why_not_in_suggest[name]["VARCHAR"] = f"VARCHAR : line {idx} : {elem}"
            if types["BOOL"] and elem not in list_pg_bool:
                types["BOOL"] = False
                dict_why_not_in_suggest[name]["BOOL"] = f"BOOL : line {idx} : {elem}"
            if types["SERIAL"] and elem != "DEFAULT" and (not elem.isnumeric() or int(elem) >= 2**31-1 or int(elem) < 1):
                types["SERIAL"] = False
                dict_why_not_in_suggest[name]["SERIAL"] = f"SERIAL : line {idx} : {elem}"
            if types["DECIMAL"]:
                if not is_float(elem) or len(elem.split(".")) != 2 :
                    types["DECIMAL"] = False
                    dict_why_not_in_suggest[name]["DECIMAL"] = f"DECIMAL : line {idx} : {elem}"
                else:
                    if len(elem) > size_deci:
                        size_deci = len(elem)-1
                    elem = elem.split(".")
                    if len(elem[1]) > nb_deci:
                        nb_deci = len(elem[1])
                    if (size_deci - nb_deci) > 131072 or nb_deci > 16383:
                        types["DECIMAL"] = False
                        dict_why_not_in_suggest[name]["DECIMAL"] = f"DECIMAL : line {idx} : {elem}"
        for type in types:
            if types[type]:
                dict_suggest[name].append(type)
                if type == "VARCHAR":
                    dict_suggest[name][-1] += f"-{size_varchar}"
                if type == "DECIMAL":
                    dict_suggest[name][-1] += f"-{size_deci}-{nb_deci}"
    return dict_suggest,dict_why_not_in_suggest
                
def waiting_input(msg : str, dict_data : dict, data_names : list, idx_column : int, mini = 0):
    while True:
        value = input(f"\n{msg}\n\n")
        if value.isnumeric() and value >= mini:
            dict_data[data_names[idx_column]].append(value)
            break;
        else:
            print("\nINVALID ENTRY\n")

def execute_sql(query, con, cur):
    try:
        cur.execute(query)
        con.commit()
    except (psycopg2.errors.DuplicateTable, psycopg2.errors.SyntaxError) as e:
        e = str(e)
        res = input("\nTable already exists\nDrop the table ?\nY. Drop\nn. Cancel\n\n")
        if res.upper() == "Y" or res == "":
            con.rollback()
            table_name = e.split("«")[1].split("»")[0] if "«" in e else e.split('"')[0]
            cur.execute(f"DROP TABLE{table_name};")
            con.commit()
            cur.execute(query)
            con.commit()

def export_sql(query, sql_file, option):
    with open(sql_file, option, encoding="utf-8") as f:
        f.write(query)

def import_sql(path, con, cur):
    files = os.listdir(path)
    print(f"\nChoice the file to import ({len(files)+1}):")
    for i in range(len(files)):
        print(f"{i+1}. {files[i]}")
    print(f"{len(files)+1}. Cancel\n")
    res = input()
    if not res.isnumeric() or int(res)<1 or int(res)>len(files):
        return
    else:
        with open(f"{path}{files[int(res)-1]}", "r", encoding="utf-8") as f:
            execute_sql(f.read(), con, cur)  
    
def drop_tables(con, cur):
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    list_tables = []
    for table in cur.fetchall():
        list_tables.append(str(table).split("'")[1])
    print(f"\nSelect the table ({len(list_tables)+2}):")
    for i in range(len(list_tables)):
        print(f"{i+1}. {list_tables[i]}")
    if len(list_tables) >= 2:
        print(f"{len(list_tables)+1}. All of them")
    print(f"{len(list_tables)+2}. Cancel\n")
    res = input()
    selected_tables = []
    if not res.isnumeric() or int(res)<1 or int(res)>len(list_tables)+1:
        return
    if len(list_tables) >= 2 and int(res) == len(list_tables)+1:
        selected_tables = list_tables
    else:
        selected_tables.append(list_tables[int(res)-1])
    for table in selected_tables:
        cur.execute(f"DROP TABLE {table};")
        con.commit()
    


def init_db():
    print("If you wish to make it faster, change the entries directly on the code.")
    your_database = input("Insert Database: ")
    your_user = input("Insert User: ")
    your_password = input("Insert Password: ")
    your_host = input("Insert Host: ")
    your_port = input("Insert Port: ")

    return psycopg2.connect(database=your_database, user=your_user, password=your_password, host=your_host, port=your_port)   #To change connection settings manually, change them here

def main(con):
    # This script will iterate with all CSV files in a folder. DO NOT leave any non CSV files in your folder.
    path_csv = "C:/YourPath"   #Insert Path of your csv folder
    path_sql = "C:/YourPath"   #Insert Path of your sql folder

    cur = con.cursor()

    while True:
        res = input("\nMenu (1):\n1. Create table from csv files\n2. Import table from sql files\n3. Drop tables\n4. Exit\n\n")
        if res == "2":
            import_sql(path_sql, con, cur)
        elif res == "3":
            drop_tables(con,cur)
        elif res == "4":
            print("\nGoodbye !")
            break
        else:
            create_from_csv(path_sql, path_csv, con, cur)

    con.close()

if __name__ == "__main__":
    con = init_db()
    main(con)
