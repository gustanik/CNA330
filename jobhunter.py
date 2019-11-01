# This script pulls from a job website and stores positions into a database. If there is a new posting it notifies the user.
# CNA 330, Fall 2019
# Gabriel Ustanik, gjustanik@student.rtc.edu
import mysql.connector
import sys
import json
import urllib.request
import urllib3
import os
import time
import re
import urllib.error


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
    cursor.execute("CREATE TABLE IF NOT EXISTS scrooge(id VARCHAR(256), type VARCHAR(256), title VARCHAR(256), description TEXT, job_id VARCHAR(256), created_at TIMESTAMP, company VARCHAR(256), location VARCHAR(256), how_to_apply TEXT(256));")
    return

# Query the database.
# You should not need to edit anything in this function
def query_sql(cursor, query):
    cursor.execute(query)
    return cursor
#
# # Add a new job
def add_new_job(cursor, jobdetails):
    ## Add your code here
    mydict = {
        "company": "Rational Consulting",
        # "company_logo": "https://jobs.github.com/rails/active_storage/blobs/eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaHBBbmhZIiwiZXhwIjpudWxsLCJwdXIiOiJibG9iX2lkIn19--90d24a725c441581640d041a43afca856a1197e9/RATIONAL%20CONSULTING_FULL_LOGO_BLK.jpg",
        # "company_url": null,
        # "created_at": "Tue Nov 13 19:35:35 UTC 2018",
        "description": "<p>Rational Consulting is growing, and we need amazing talent to join our team. If you have an entrepreneurial attitude with a deep spirit of service and killer subject expertise, we want to talk to you. We are always looking for the next great consultant to raise the bar and push Rational Consulting to be better than we were yesterday. Staying hungry, curious, and looking forward to what\u2019s next is part of our DNA.</p>\n<p>Be part of the Mobile First, Cloud First excitement! One of our Microsoft teams is building software and services to enable development of rich enterprise class applications on the Microsoft cloud platform. As part of the unified engineering team, you will have the opportunity to apply quality focused engineering practices to your daily work.</p>\n<p>This team is an exciting and fast evolving engineering team within Microsoft focused on building scalable distributed cloud services for Engineering Systems and for the Product Telemetry.\nWe have an exciting opportunity for you as a Full Stack Engineer to innovate, influence, transform, inspire and grow within our organization and we encourage you to apply to learn more!</p>\n<p>What you will do:</p>\n<p>-You will be using sound engineering practices, tools, and standards.\n-You should be willing to work in a highly collaborative environment.\n-Additionally, you will be part of a fun team with diverse interests and backgrounds in a supportive environment.\n-You will become a productive team member through your knowledge and ability to learn technologies and practices such as C#, X++, HTML5, SQL Azure, Azure Service Fabric, JavaScript, Windows and Azure Frameworks, analytics, PowerApps, Power BI, Common Data Service, design patterns, unit and functional testing, OO design, and more.\n-You will be exposed to a broad range of problems and technologies as we are rapidly evolving our service architecture to leverage Microsoft\u2019s cloud services assets like Azure.\n-Our team works in an agile start-up like environment where we expect each team member to think out of the box to contribute and collaborate towards the mission of the team in a feature-crew setting\n-As part of this team you will get exciting opportunities to work on cutting edge technologies across a variety of platforms to build the next generation application experiences for Microsoft and revolutionize the way businesses operate.</p>\n<p>What you will bring:</p>\n<p>-Experience and ability to work with Microsoft development technologies like .NET and C#\n-5 years or more of HTML, CSS, and JavaScript/TypeScript\n-3 years or more of SQL\n-Experience using GitHub as version control\n-Azure or Cloud experience highly preferred\n-3 years of object-oriented design, database design, algorithms, data structures and parallel programming\n-A minimum of 5 years of industry experience working as a software engineer/developer\n-BA/BS in Computer Science or equivalent.\n-Ability to meet Microsoft, customer and/or government security screening requirements are required for this role. These requirements include, but are not limited to the following specialized security screenings: Microsoft Cloud Background Check</p>\n<p>Who you are:</p>\n<p>-Proactive. You are always thinking ahead about what is best for the Client\n-Adaptable. Change happens at lightning speed (especially at Rational), you are flexible, enjoy challenge, and get behind new ideas\n-Visionary.\u202fAbility to see \"the whole picture\" and simultaneously remain slightly obsessed with the details\u202f\n-Collaborative to the Core. Demonstrated ability to work in a team environment, as a leader and member</p>\n<p>Rational Consulting is a results-oriented consultancy designing premium customer experiences. Each of our business practices are deeply rooted in delivering client success. We see ourselves as partners to our clients, and we invest in each of their business goals, ensuring that our work helps deliver on these goals. Client success is our ultimate metric, and what drives our mindset, skillset, and company culture.</p>\n<p>Incredible people are our non-negotiable. Our experienced team of consultants spans the globe. We love what we do, and who we do it with \u2013 let\u2019s begin our next chapter together.\u202f</p>\n",
        # "how_to_apply": "<p>Please send your resume to <a href=\"mailto:katyann@rationalconsulting.com\">katyann@rationalconsulting.com</a></p>\n<p>OR</p>\n<p>apply at: <a href=\"https://boards.greenhouse.io/rationalconsulting/jobs/4104879002\">https://boards.greenhouse.io/rationalconsulting/jobs/4104879002</a></p>\n",
        "id": "2c9f30d4-e77b-11e8-8aab-ce7423d373e0",
        "location": "Redmond, WA",
        "title": "Software Engineer",
        "type": "Full Time",
        # "url": "https://jobs.github.com/positions/2c9f30d4-e77b-11e8-8aab-ce7423d373e0"
    }

    columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in ())
    values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in ())

    query = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('scrooge', columns, values)

    cursor.execute(query, mydict.values())
    ###reference: https: // blog.softhints.com / python - 3 - convert - dictionary - to - sql - insert /

    # query = "INSERT INTO scrooge(id, type, title, description, job_id, created_at, company, location, how_to_apply"")VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);"
    # return query_sql(cursor, query)

#
# Check if new job
def check_if_job_exists(cursor, jobdetails):
    ## Add your code here
    query = "SELECT * from scrooge;"
    # print(query)
    return query_sql(cursor, query)
#
def delete_job(cursor, jobdetails):
    ## Add your code here
    query = "UPDATE zane SET id = 1, type = ?, title = ?, description = ?, job_id = ?, created_at = ?, company = ?,location = ?, how_to_apply = ? WHERE id = 1"
    return query_sql(cursor, query)    ###reference:http://www.mysqltutorial.org/python-mysql-update/, https://stackoverflow.com/questions/10575776/update-an-entire-row-in-mysql
#
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
#
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

    jobdetails = {'host': 'localhost',
             'user': 'root',
             'passwd': 'my secret password',
             'db': 'write-math'}

    return argument_dictionary

# Main area of the code.
def jobhunt(arg_dict):
    # Fetch jobs from website
    jobpage = fetch_new_jobs(arg_dict)
    # print (jobpage)
    ## Add your code here to parse the job page
    with urllib.request.urlopen("https://jobs.github.com/positions.json?location=seattle") as url:
        json_data = json.loads(url.read().decode())
    # print(json_data)
    sing = (json.dumps(json_data, indent=4, sort_keys=True))
    with open('data.txt', "w") as text_file:
        text_file.write(sing)
        # print(sing)

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
    ###ref:https://stackoverflow.com/questions/30326562/regular-expression-match-everything-after-a-particular-word?rq=1
    ###https://stackoverflow.com/questions/5228448/how-do-i-match-a-word-in-a-text-file-using-python
    ## Add in your code here to check if the job already exists in the DB

def already_exist(cursor):
    text='data.txt'
    cursor.execute("SELECT job_id, COUNT(*) FROM jobinfo WHERE job_id = %s GROUP BY job_id", (text,))

    msg = cursor.fetchone()
    if msg:
        print('already exists')

    if not msg:
        print('It does not exist')
    ###ref:https://stackoverflow.com/questions/31692339/mysqldb-check-if-row-exists-python/31695856
    ## Add in your code here to notify the user of a new posting
def new_posting():
    saved_time_file = 'last time check.txt'
    url = 'https://jobs.github.com/positions.json?location=seattle'

    request = urllib.request
    if os.path.exists(saved_time_file):
        """ If we've previously stored a time, get it and add it to the request"""
        last_time = open(saved_time_file, 'r').read()
        urllib3.make_headers("If-Modified-Since", last_time)

    try:
        response = urllib.request.urlopen(url)  # Make the reques
    except urllib.request.HTTPError as err:
        if err.code == 304:
            print("Nothing new.")
            sys.exit(0)
        raise  # some other http error (like 404 not found etc); re-raise it.

    last_modified = response.info().get('Last-Modified', False)
    if last_modified:
        open(saved_time_file, 'w').write('last_modified')
    else:
        print("Server did not provide a last-modified property. Continuing...")
        ###reference: https://stackoverflow.com/questions/11252576/how-to-check-if-the-value-on-a-website-has-changed
        ###https://www.codementor.io/gergelykovcs/how-and-why-i-built-a-simple-web-scrapig-script-to-notify-us-about-our-favourite-food-fcrhuhn45
        ## EXTRA CREDIT: Add your code to delete old entries

def delete_old(cursor, conn):
    try:
        sql="DELETE FROM jobinfo WHERE created_at < DATE_SUB(NOW(), INTERVAL 1 DAY)"

        try:
            cursor.execute(sql)
            conn.commit()
            print("Deleted Older Data from database")

        except:
            conn.rollback()
            print("Cann't delete older data")
            cursor.close()

    except:
                print("localserver not connected")
        ###reference:https://stackoverflow.com/questions/35151952/delete-older-data-from-mysql-database-using-python, https://github.com/meub/craigslist-for-sale-alerts/blob/master/remove_listing.py
        ###https://github.com/meub/craigslist-for-sale-alerts

# Setup portion of the program. Take arguments and set up the script
# You should not need to edit anything here.
def main():
    # Connect to SQL and get cursor
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor, "table")
    check_if_job_exists(cursor, 'jobdetails')
    new_posting()
    # Load text file and store arguments into dictionary
    arg_dict = load_config_file(sys.argv[0])
    while(1):
        jobhunt(arg_dict)
        time.sleep(3600) # Sleep for 1h

if __name__ == '__main__':
    main()

