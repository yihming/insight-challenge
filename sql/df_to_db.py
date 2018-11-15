import mysql.connector as M
import json, sys

def get_values_from_line(line):
    """Get values of an observation as a list from string,
    splitted by semicolons.
    Note 1: Ignore ';' inside \"...\". E.g. consider \"xxx;yyy\" as one value.
    Note 2: Null string '' for NA values.
    """
    raw_list = line.strip().split(';')
    i = 0
    values = []
    while i < len(raw_list):
        if raw_list[i] == "" or raw_list[i][0] != '\"':
            values.append(raw_list[i])
            i = i + 1
        else:
            if raw_list[i][-1] == '\"':
                values.append(raw_list[i][1:-1])
                i = i + 1
            else:
                j = i + 1
                entry = raw_list[i][1:]
                while j < len(raw_list) and raw_list[j][-1] != '\"':
                    entry = entry + raw_list[j]
                    j = j + 1
                if j == len(raw_list):
                    i = j
                else:
                    entry = entry + raw_list[j][:-1]
                    values.append(entry)
                    i = j + 1
                    
    return values

def process_head(head):
    fields = ['INDEX'] + head[:-1].split(';')[1:]
    return fields

def write_to_db(h1b_list, db):
    if len(h1b_list) == 0:
        return

    cur = db.cursor()
    try:
        for obs in h1b_list:
            cur.execute(
                """INSERT INTO h1b_2015 (index_id, case_id, status, state, employer, soc_code, soc_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (obs['INDEX'], obs['CASE_NUMBER'], obs['CASE_STATUS'], obs['WORKSITE_STATE'],
                 obs['EMPLOYER_NAME'], obs['SOC_CODE'], obs['SOC_NAME']))
        db.commit()
    except M.Error as e:
        print(e)
        db.rollback()

    print("Finish writing ", len(h1b_list), " entries to database.")

def process_data(filename, db):
    fp = open(filename, "r")
    print("Start to write entries in ", filename, " to database:")

    fields = process_head(fp.readline())

    batchSize = 100000
    cnt = 0

    h1b_list = []
    for line in fp:
        if cnt == batchSize or "" == line:
            write_to_db(h1b_list, db)
            cnt = 0
            h1b_list = []
            
        cnt = cnt + 1
        values = get_values_from_line(line)
        obs = dict(zip(fields, values))
        h1b_list.append(obs)

    print("Process finished.")
    fp.close()

def main():
    config = json.load(open('dbconn.json'))["mysql"]
    db = M.connect(host = config["host"],
                   user = config["user"],
                   passwd = config["password"],
                   db = config["database"])

    df_name = sys.argv[1]
    
    process_data(df_name, db)

    db.close()

if __name__ == "__main__":
    main()
