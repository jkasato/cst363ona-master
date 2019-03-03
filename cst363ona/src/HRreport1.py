from cst363ona.src import HRclient

HRclient.readConfig()
c = HRclient.Coordinator()

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

c.sendToAll("drop table if exists temp3 ")  # drop table if exists
c.sendToAll("create table temp3 (dept int,name char(20),salary double,empid int)")  # create table
c.sendToAll("map select e.dept, m.name, e.salary,e.empid from employee m join temp2 e "
            "on m.empid=e.empid")
c.sendToAll("shuffle insert into temp3 values {}")
c.sendToAll("reduce select dept, name, round(avg(salary),2), min(salary), max(salary), "
            "count(*) from temp3 group by dept order by dept")

c.close()
