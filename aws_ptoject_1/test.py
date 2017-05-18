import boto
from boto.s3.key import Key
import csv
import pymysql
import time


import uuid
import boto.s3.connection
import urllib


from flask import Flask, request, render_template, session

app = Flask(__name__)
app.secret_key = 'super secret key'

@app.route("/")
def mainpage():
    return render_template('index.html')


#upload local to s3 (bucket)
@app.route('/loads3', methods=['GET', 'POST'])
def index():
    
    session['total_time']={}
    # If user has submitted the form...
    if request.method == 'POST':

        # Connect to Amazon S3
        s3 = boto.connect_s3()

        # Get a handle to the S3 bucket
        bucket_name = 'vxs3bucket'
        bucket = s3.get_bucket(bucket_name)
        k = Key(bucket)

        # Loop over the list of files from the HTML input control
        data_files = request.files.getlist('file[]')
        start_time = time.time() # start time
        for data_file in data_files:

            # Read the contents of the file
            file_contents = data_file.read()

            # Use Boto to upload the file to the S3 bucket
            k.key = data_file.filename
            print "Uploading some data to " + bucket_name + " with key: " + k.key
            k.set_contents_from_string(file_contents)
        end_time = time.time() #end time
        total_time = end_time-start_time #total time
        session['total_time']=total_time
        print session['total_time']
        print "total_time"
    return render_template('index.html')

#uploading local csv to mysql
@app.route('/upload', methods=['GET','POST'])
def upload():
    
    #mydb = pymysql.connect(host='vxdbinstance.cwo47igf50ip.us-west-2.rds.amazonaws.com',
    #    user='',
    #    passwd='',
    #    db='')
    #cursor = mydb.cursor()
    #print "before insert"

    
    mydb = pymysql.connect(host='',user='',passwd='',db='')
    cursor = mydb.cursor()
    print "before insert"

    #
    filedata = request.files['file']
    csv_data = csv.reader(filedata)
    next(csv_data, None)
    start_time = time.time() # start time

    print "timer started"
    cursor.execute("TRUNCATE dbinstancename.csvtable")
    mydb.commit()
    count=0
    for row in csv_data:
        if(count<=10000):
            cursor.execute("INSERT INTO csvtable(year,zipcode,cause,count,location) VALUES (%s, %s, %s,%s,%s)",row)
            count=count+1
            print count
    #close the connection to the database.
    mydb.commit()
    end_time = time.time() #end time
    total_time = end_time-start_time #total time
    session['total_time']=total_time
    print session['total_time']
    cursor.close()
    print "Done"
    return render_template('index.html')


@app.route('/display', methods=['GET','POST'])
def diaplay():
    if request.method=='POST':
        s3 = boto.connect_s3()
        bucket_name = 'vxs3bucket'
        bucket = s3.get_bucket(bucket_name)
        k = Key(bucket)
        bucket.objects.all()
        s3.ObjectSummary
        object.get
    return render_template('index.html')

#
#uploading s3 to mysql
@app.route('/uploadsetords', methods=['GET','POST'])
def uploadsetords():
    if request.method=='POST':
        AWS_ACCESS_KEY_ID=''
        AWS_SECRET_ACCESS_KEY = ''
        s3 = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,is_secure = False)

        bucket = s3.get_bucket('vxs3bucket')

        all_users = 'http://acs.amazonaws.com/groups/global/AllUsers'
        
        for key in bucket:
            print str(key).split(",")
            readable = False
            acl = key.get_acl()
            for grant in acl.acl.grants:
                if grant.permission == 'READ':
                    if grant.uri == all_users:
                        readable = True
            if not readable:
                key.make_public()
        opens=urllib.URLopener()
        mydb = pymysql.connect(host='',
            user='vineethxavier',
            passwd='',
            db='')
        cursor = mydb.cursor()  
        link="https://s3-us-west-2.amazonaws.com/vxs3bucket/3lines.csv"  #uploaded file in s3 link
        f=opens.open(link)
        csvfile= csv.reader(f)
        print(csvfile)
        print("executing")
        startTime = int(time.time())
        for row in csvfile:
                cursor.execute("INSERT INTO csvtable(year,zipcode,cause,count,location) VALUES (%s, %s, %s,%s,%s)",row)
                mydb.commit()

        print("done")
        endTime = int(time.time())
        totalTime = endTime - startTime
        print 'total time is '+str(totalTime)+' seconds'
        cursor.close()  
    return render_template('index.html')


#
if __name__ == '__main__':
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'
    app.debug=True
    app.run(
        host='127.0.0.1',
        port=int('5000'),
        debug=True
    )