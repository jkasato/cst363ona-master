from cst363ona.src import mpclient
import random

# create student table on all nodes
mpclient.readConfig()
f = open("student.data", "w")

# generate 100 rows of test data into a file and load the database
# Randomly generate student id
# studentid=i
# '''
for i in range(1, 101):
    studentid = i + 4

for studentid in range(1, 101):
    name = 'Student' + str(studentid)
    #'''
    # Major should be random selection of ‘Biology’, ‘Business’, ‘CS’, ‘Statistics’
    if studentid % 2:
        major = 'Biology'
    elif studentid % 3:
        major = 'Statistics'
    elif studentid % 5:
        major = 'CS'
    else:
        major = 'Business'
    # gpa is a random value from 2.5 to 4.0
    gpa = round(random.uniform(2.5, 4.0), 2)
    # Table: student (id int primary key,  name char(20),  major char(10), double gpa)
    line = str(studentid) + ', "' + name + '", ' + '"' + str(major) + '", ' + str(gpa) + '\n'
    f.write(line)
f.close()

c = mpclient.Coordinator()
#drop table
c.sendToAll("drop table if exists student")

#create table
c.sendToAll("create table student (studentid int primary key, name char(20), major char(10), gpa double)")

#load table
c.loadTable("student", "student.data")

# Verify that the cluster is working by doing the queries in mpclient1.py
# '''
'''#find by key
# select * from student where id=2  -- 2 is an id that exists
for studentid in range(1, 110):
    print("test1")
    c.getRowByKey("select * from student where studentid=" + str('5'), 5)
    if str(studentid) == 5:
        print("five is present")

    print("test2")
    # select * from student where id=yyyy  -- yyyy is an id that does not exist
    c.getRowByKey("select * from student where studentid=" + str('150'), 150)
    if str(studentid) == 150:
        print("150 is not present")
        print("test3")

    # select * from student where major = ‘Biology’
    c.getRowByKey("select * from student where major=" + str('Biology'), 'Biology')
    if str(major) == 'Biology':
        print("we have bio majors")

    print("test4")
    # select * from student where gpa >= 3.2
    c.getRowByKey("select * from student where gpa>=" + str(gpa), 3.20)
    if gpa >= 3.20:
        print("we have honor roll")
'''
#find by non key
print("five is present")
c.sendToAll("reduce select * from student where studentid=5")

print("150 is not present")
c.sendToAll("reduce select * from student where studentid=150")

print("we have bio majors")
c.sendToAll("reduce select * from student where major='Biology'")

print("we have honor roll")
c.sendToAll("reduce select * from student where gpa>=3.20")

'''
# example of map-shuffle-reduce
    # create temp table
c.sendToAll("drop table if exists tempdept ")
c.sendToAll("create table tempdept (dept int, salary double)")

# map phase
# map phase executes the select but holds the result data at the worker
# map should be followed by shuffle
c.sendToAll("map select dept, salary from student ")

# shuffle phase
# the result from prior map phase is distributed to servers
# and inserted into temp table.
# use {} as place holder for data values as shown below.
c.sendToAll("shuffle insert into tempdept  values {}")

# reduce phase
# execute the select and return result to client.
print("Reduce result set")
c.sendToAll("reduce select dept, avg(salary), count(*) from tempdept  group by dept order by dept")

# clean up - delete temp table
c.sendToAll("drop table if exists tempdept ")
'''
c.close()
