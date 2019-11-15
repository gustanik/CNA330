# This script pulls from a job website and stores positions into a database. If there is a new posting it notifies the user.
# CNA 330, Fall 2019
# Gabriel Ustanik, gjustanik@student.rtc.edu
import mysql.connector
import sys
import json
import urllib.request
import os
import time
import datetime
from datetime import datetime
import re

# Connect to database
# You may need to edit the connect function based on your local settings.
def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='password',
                                  host='127.0.0.1',
                                  database='cna330')
    return conn

# Create the table structure
def create_tables(cursor, table):
    ## Add your code here. Starter code below
    cursor.execute('CREATE TABLE IF NOT EXISTS van(id int PRIMARY KEY AUTO_INCREMENT, type varchar(10), title varchar(100), description text, job_id varchar(99) UNIQUE, created_at DATE, company varchar(100), location varchar(50), how_to_apply varchar(500));')
    return

###ref:https://stackoverflow.com/questions/2564568/python-mysql-check-for-duplicate-before-insert

# Query the database.
# You should not need to edit anything in this function
def query_sql(cursor, query):
    cursor.execute(query)
    return cursor
# Add a new job
def add_new_job(cursor, jobdetails):
    ## Add your code here
    query = "INSERT INTO"
    return query_sql(cursor, query)
# Check if new job
def check_if_job_exists(cursor, jobdetails):
    ## Add your code here
    query = "SELECT"
    return query_sql(cursor, query)

def delete_job(cursor, jobdetails):
    ## Add your code here
    query = "UPDATE"
    return query_sql(cursor, query)

# Grab new jobs from a website
def fetch_new_jobs(arg_dict):
    # Code from https://github.com/RTCedu/CNA336/blob/master/Spring2018/Sql.py
    query = "https://jobs.github.com/positions.json?" + "location=seattle" ## Add arguments here
    jsonpage = 0
    try:
        contents = urllib.request.urlopen(query)
        response = contents.read()
        jsonpage = json.loads(response)
    except:
        pass
    return jsonpage

# Load a text-based configuration file
def load_config_file(filename):
    argument_dictionary = 0
    # Code from https://github.com/RTCedu/CNA336/blob/master/Spring2018/FileIO.py
    rel_path = os.path.abspath(os.path.dirname(__file__))
    file = 0
    file_contents = 0
    try:
        file = open(filename, "r")
        file_contents = file.read()
    except FileNotFoundError:
        print("File not found, it will be created.")
        file = open(filename, "w")
        file.write("")
        file.close()

    ## Add in information for argument dictionary
    return argument_dictionary

# Main area of the code.
def jobhunt(arg_dict):
    # Fetch jobs from website
    jobpage = fetch_new_jobs(arg_dict)
    # print (jobpage)

    ## Add your code here to parse the job page
def parse(arg_dict, jobdetails, cursor, conn):
    jobpage = fetch_new_jobs(arg_dict)
    jobdetails = (json.dumps(jobpage))
    # print(jobdetails)
    with urllib.request.urlopen("https://jobs.github.com/positions.json?location=seattle") as url:
        json_data = json.loads(url.read().decode())
    # print(json_data)
    sing = (json.dumps(json_data, indent=4, sort_keys=True))
    with open('data.txt', "w") as text_file:
        text_file.write(sing)

    with open('data.txt', 'r') as searchfile:
        for line in searchfile:
            if re.search(r'"id"', line, re.M | re.I):
                print(line)

    with open('data.txt', 'r') as searchfile:
        for line in searchfile:
            if re.search(r'"type"', line, re.M | re.I):
                print(line)

    with open('data.txt', 'r') as searchfile:
        for line in searchfile:
            if re.search(r'"description"', line, re.M | re.I):
                print(line)
    for record in jobpage:
        record['created_at'] = str(datetime.strptime(record['created_at'], '%a %b %d %H:%M:%S %Z %Y').date())
        print((record['created_at']))
        # p = record['created_at']
        sql = """
                    INSERT INTO
                        van(type, title, description, job_id, created_at, company, location, how_to_apply)
                    VALUES
                        (%(type)s, %(title)s, %(description)s, %(id)s, %(created_at)s, %(company)s, %(location)s, %(how_to_apply)s)""" .format(record)


        cursor.execute(sql, record)
        cursor.execute(conn.commit())
        ######https://stackoverflow.com/questions/29380979/importing-json-into-mysql-using-python
        # ref: http: // strftime.org /
        ######refhttps://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior

    ###reference: ###ref:https://stackoverflow.com/questions/30326562/regular-expression-match-everything-after-a-particular-word?rq=1
    ###https://stackoverflow.com/questions/5228448/how-do-i-match-a-word-in-a-text-file-using-python

    ## Add in your code here to check if the job already exists in the DB
def is_exists(cursor, arg_dict, conn):
    cursor = conn.cursor(buffered=True)  ###ref:https://stackoverflow.com/questions/38350816/python-mysql-connector-internalerror-unread-result-found-when-close-cursor
    jobpage = fetch_new_jobs(arg_dict)
    jsonpage = json.dumps(jobpage)
    for job in jobpage:
        job['id'] = str((job['id']))
        print((job['id']))
        cursor.execute("SELECT job_id, COUNT(*) FROM van WHERE job_id = job_id GROUP BY job_id", job)
        row_count = cursor.rowcount
        # print("number of affected rows: {}".format(row_count))
        if row_count == 0:
            print("new job/s have been posted")

        # msg = cursor.fetchone()
        # # check if it is empty and print error
        # if not msg:
        #     print('It does not exist')

    ###ref:https://stackoverflow.com/questions/31692339/mysqldb-check-if-row-exists-python/31695856
    ## Add in your code here to notify the user of a new posting

    ## EXTRA CREDIT: Add your code to delete old entries
def delete_old(cursor, conn):
    cursor.execute("delete from van where datediff(now(), van.created_at) > 1;")
    # cursor.executeDELETE FROM CALLRECORD WHERE DATE < DATE_SUB(NOW(), INTERVAL 7 DAY)
    cursor.execute(conn.commit())
    ###https://www.tutorialspoint.com/deleting-all-rows-older-than-5-days-in-mysql
# Setup portion of the program. Take arguments and set up the script
# You should not need to edit anything here.
def main():
    # Connect to SQL and get cursor
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor, 'table')
    # Load text file and store arguments into dictionary
    arg_dict = load_config_file(sys.argv[0])
    # check_if_job_exists(cursor, )
    while(1):
        jobhunt(arg_dict)
        is_exists(cursor, arg_dict, conn)
        parse(arg_dict, 'pj', cursor, conn)
        delete_old(cursor, conn)
        # check_if_job_exists(cursor, 'endpoint')
        time.sleep(3600) # Sleep for 1h

if __name__ == '__main__':
    main()