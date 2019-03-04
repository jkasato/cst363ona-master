from cst363ona.src import HRclient

HRclient.readConfig()
c = HRclient.Coordinator()

#'''
# join employee with department on dept to find mgr_id for each employee
c.sendToAll("drop table if exists temp1 ")  # drop table if exists
c.sendToAll("create table temp1 (dept int, salary double,empid int)")  # create table
c.sendToAll("map select dept,salary,empid from employee")  # repartition employee table by dept
c.sendToAll("shuffle insert into temp1 values {}")

# join mgr_id to employee empid to get the manager name
c.sendToAll("drop table if exists temp2 ")  # drop table if exists
c.sendToAll("create table temp2 (mgr_id int,dept int,salary double,empid int)")  # create table
c.sendToAll("map select d.mgr_id, d.dept, e.salary,e.empid from department d join temp1 e "
            "on e.dept=d.dept")
c.sendToAll("shuffle insert into temp2 values {}")
#'''
'''
c.sendToAll("drop table if exists temp3 ")  # drop table if exists
c.sendToAll("create table temp3 (dept int,name char(20),salary double,empid int)")  # create table
c.sendToAll("map select e.dept, m.name, e.salary,e.empid from employee m join temp2 e "
            "on m.empid=e.empid")
c.sendToAll("shuffle insert into temp3 values {}")
c.sendToAll("reduce select dept, name, round(avg(salary),2), min(salary), max(salary), "
            "count(*) from temp3 group by dept order by dept")
'''
'''
lists employees (id, name, salary, dept) that earn more than their manager 
show employee id, employee name, employee salary, dept, manager name 
and manager salary in the report
'''
'''
# join employee with department on dept to find mgr_id for each employee
c.sendToAll("drop table if exists temp1 ")  # drop table if exists
c.sendToAll("create table temp1 (dept int, salary double,empid int)")  # create table
c.sendToAll("map select dept,salary,empid from employee")  # repartition employee table by dept
c.sendToAll("shuffle insert into temp1 values {}")

c.sendToAll("drop table if exists temp2 ")  # drop table if exists
c.sendToAll("create table temp2 (mgr_id int,empid int,salary double,dept int)")  # create table
c.sendToAll("map select d.mgr_id, e.empid, e.salary,d.dept from department d join temp1 e "
            "on e.dept=d.dept")
c.sendToAll("shuffle insert into temp2 values {}")
'''

'''
# join mgr_id to employee empid to get the manager name
c.sendToAll("drop table if exists temp3 ")  # drop table if exists
# added a column for manager salary
c.sendToAll("create table temp3 (dept int,name char(20),salary double,empid int,salary double)")  # create table
# e.salary is employee salary joined to m.salary which is manager salary
c.sendToAll("map select e.dept, m.name, e.salary,e.empid,m.salary from employee m join temp2 e "
            "on m.empid=e.empid")
c.sendToAll("shuffle insert into temp3 values {}")
#c.sendToAll("reduce select dept, name, round(avg(salary),2), min(salary), max(salary), "
            "count(*) from temp3 group by dept order by dept")
'''

#'''
#join empid to employee mgr_id to get the manager salary and employee name
c.sendToAll("drop table if exists temp3 ")  # drop table if exists
c.sendToAll("create table temp3 (dept int,name char(20),salary double,empid int)")  # create table

c.sendToAll("map select e.dept, m.name, e.salary,e.empid,m.salary from employee m join temp2 e "
            "on m.empid=e.empid")
c.sendToAll("shuffle insert into temp3 values {}")


c.sendToAll("reduce select dept, name as manager_name, salary as employee_salary,empid,"
            "salary as manager_salary,count(*) from temp3 group by dept order by dept")
#'''
c.close()
