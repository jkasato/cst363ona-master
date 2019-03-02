from cst363ona.src import HRclient

HRclient.readConfig()
c = HRclient.Coordinator()
'''
#drop tables
#c.sendToAll("drop table if exists employee")
#c.sendToAll("drop table if exists department")

#create tables
#c.sendToAll("create table employee (empid int primary key, name char(20), dept int, salary double)")
#c.sendToAll("create table department (dept int primary key, dept_name char(20), mgr_id int)")

#load tables
#print("Loading employee table.  This will take a few minutes.")
#c.loadTable("employee", "employee.data")
#print("Loading department table")
#c.loadTable("department", "department.data")
'''
'''
#map-shuffle-reduce
#   drop table if it exists
c.sendToAll("drop table if exists tempdept ")
#c.sendToAll("drop table if exists tempemp ")

#   create temp table
#c.sendToAll("create table tempdept (dept int,dept_name char(20),mgr_id int)")
#c.sendToAll("create table tempemp (empid int, name char(20), dept int, salary double)")
c.sendToAll("create table tempdept (dept int, salary double)")

#   map phase
#c.sendToAll("map select d.department,name, mgr_id from employee e join department d on e.empid = d.mgr_id")
#c.sendToAll("map select empid,name,dept,salary,d.dept_name,d.mgr_id from employee e join department d on e.dept = d.dept")

#c.sendToAll("map select employee, salary from employee ")
c.sendToAll("map select dept, salary from emp ")

#   shuffle
#c.sendToAll("shuffle insert into tempdept  values {}")
#c.sendToAll("shuffle insert into tempemp  values {}")
c.sendToAll("shuffle insert into tempdept  values {}")

#   reduce
print("Reduce result set")
#c.sendToAll("reduce select department, avg(salary), count(*) from tempdept  group by dept order by dept")
c.sendToAll("reduce select dept, avg(salary), count(*) from tempdept  group by dept order by dept")
'''
#returns department id,manager name of department,number of employees in the department
#map-shuffle-reduce
c.sendToAll("drop table if exists tempdept ")#drop table if exists
c.sendToAll("create table tempdept (dept int,name char(20))")#create table
c.sendToAll("map select dept, name from emp ")#map
c.sendToAll("shuffle insert into tempdept  values {}")#shuffle
print("Reduce result set")
c.sendToAll("reduce select dept, name char(20), count(*) from tempdept  group by dept order by dept")#reduce

#returns average salary of employees in department,
#min salary of employees in department,
#max salary of employees in department

#close table
c.close()