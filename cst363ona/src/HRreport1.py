from cst363ona.src import HRclient

HRclient.readConfig()
c = HRclient.Coordinator()
'''
#make tempdepartment
c.sendToAll("drop table if exists tempdepartment ")#drop table if exists
c.sendToAll("create table tempdepartment (dept int,dept_name char(20),mgr_id int)")#create table
c.sendToAll("map select dept, dept_name,mgr_id from department")#map
c.sendToAll("shuffle insert into tempdepartment values {}")#shuffle
c.sendToAll("reduce select dept, dept_name,mgr_id, count(*) from tempdepartment group by dept order by dept")#reduce
'''

'''
#make tempemployee
c.sendToAll("drop table if exists tempemployee ")#drop table if exists
c.sendToAll("create table tempemployee (empid int,name char(20),dept int, salary double)")#create table
c.sendToAll("map select empid,name,dept,salary from employee")#map
c.sendToAll("shuffle insert into tempemployee values {}")#shuffle
c.sendToAll("reduce select empid,name,dept,salary, count(*) from tempemployee group by dept order by dept")
'''
'''
#make tempemployee-this one has dept as first
c.sendToAll("drop table if exists tempemployee ")#drop table if exists
c.sendToAll("create table tempemployee (dept int,name char(20),empid int, salary double)")#create table
c.sendToAll("map select dept,name,empid,salary from employee")#map
c.sendToAll("shuffle insert into tempemployee values {}")#shuffle
c.sendToAll("reduce select dept,name,empid,salary, count(*) from tempemployee group by dept order by dept")
'''
'''
#make megalist
c.sendToAll("drop table if exists megalist ")#drop table if exists
c.sendToAll("create table megalist (dept int,name char(20),empid int, salary double,dept_name char(20),mgr_id int)")#create table
c.sendToAll("map select d.dept,name,empid,salary,d.dept_name,d.mgr_id "
    "from employee e join department d on e.dept = d.dept")#map
c.sendToAll("shuffle insert into megalist values {}")#shuffle
c.sendToAll("reduce select d.dept,name,empid,salary, dept_name,mgr_id,count(*) from megalist group by dept order by dept")
'''
#'''
# map-shuffle-reduce
# returns department id,manager name of department
c.sendToAll("drop table if exists megalist ")#drop table if exists
c.sendToAll("create table megalist (dept int,name char(20))")#create table
c.sendToAll("map select d.dept,name "
    "from employee e join department d on e.dept = d.mgr_id where name like 'Manager")#map
c.sendToAll("shuffle insert into megalist values {}")#shuffle
c.sendToAll("reduce select d.dept,name, count(*) from megalist group by dept order by dept")
#'''
# map-shuffle-reduce
# returns department id,manager name of department
#This works
'''
c.sendToAll("drop table if exists tempdept ")#drop table if exists
c.sendToAll("create table tempdept (dept int,name char(20))")#create table
c.sendToAll("map select dept, name from employee")#map
c.sendToAll("shuffle insert into tempdept values {}")#shuffle
c.sendToAll("reduce select dept, name, count(*) from tempdept group by dept order by dept")#reduce
'''
'''
#number of employees in the department
c.sendToAll("drop table if exists tempnum")#drop table if exists
#c.sendToAll("create table tempnum (dept int,name char(20))")#create table
c.sendToAll("create table tempnum (dept int,name char(20),salary double)")#create table
#c.sendToAll("map select dept,name from employee")#map
c.sendToAll("map select dept,name,salary from employee")#map
c.sendToAll("shuffle insert into tempnum values {}")#shuffle
c.sendToAll("reduce select dept,name char(20), count(*) from tempdept group by dept order by dept")#reduce
'''
'''
# returns average salary of employees in department,
# min salary of employees in department,
# max salary of employees in department
c.sendToAll("drop table if exists tempsal")  # drop table if exists
c.sendToAll(
    "create table tempsal (dept int primary key,salary double,avgsal double, minsal double,maxsal double)")
c.sendToAll("map select dept,salary,salary,salary,salary from employee")  # map
c.sendToAll("shuffle insert into tempsal values {}")  # shuffle
# c.sendToAll("reduce select dept, name char(20), count(*) from tempdept group by dept order by dept")#reduce
#c.sendToAll("reduce select dept,salary,round(avg(salary),2),min(salary),max(salary),count(*)from tempsal group by dept order by dept")
c.sendToAll("reduce select dept,salary,round(avg(salary),2),min(salary),max(salary),count(*)from tempsal group by dept order by dept")
'''

# close table
c.close()
