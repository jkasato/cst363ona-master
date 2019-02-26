from cst363ona.src import mpclient
import random

# create student table on all nodes
mpclient.readConfig()
f = open("student.data", "w")

# generate 100 rows of test data into a file and load the database
for i in range(1, 101):
    name = 'Student' + str(i)
    # Randomly generate student id
    studentid=i
    '''
    for a in range(1,400):
        studentid=a+4
    '''
    #Major should be random selection of ‘Biology’, ‘Business’, ‘CS’, ‘Statistics’
    if i % 7:
        major='Biology'
    elif i % 3:
        major='Statistics'
    elif i % 5:
        major='CS'
    else:
        major='Business'
    #gpa is a random value from 2.5 to 4.0
    gpa=round(random.uniform(2.5, 4.0), 2)
    #Table: student (id int primary key,  name char(20),  major char(10), double gpa)
    #line = str(studentid)+', '+name+', '+str(major)+', '+str(gpa)+'\n'
    line = str(studentid)+', '+name+', '+major+', '+str(gpa)+'\n'
    f.write(line)
f.close()

c = mpclient.Coordinator()
c.sendToAll("drop table if exists student")
c.sendToAll("create table student (studentid int primary key, name char(20), major char(10),gpa double)")
c.loadTable("student","student.data")

#Verify that the cluster is working by doing the queries in mpclient1.py
'''
#select * from student where id=2  -- 2 is an id that exists
for studentid in range(1,101):
    c.getRowByKey("select * from student where studentid="+str(studentid),2)
    print("two is present")

#select * from student where id=yyyy  -- yyyy is an id that does not exist
for studentid in range(1,101):
    c.getRowByKey("select * from student where studentid="+str(studentid),1000)
    print("1000 is not present")

#select * from student where major = ‘Biology’
for studentid in range(1,101):
    c.getRowByKey("select * from student where major="+str(major),'Biology')
    print("we have bio majors")

#select * from student where gpa >= 3.2
for studentid in range(1,101):
    c.getRowByKey("select * from student where gpa>="+float(gpa),3.2)
    print("we have honor rolls")

'''
c.close()