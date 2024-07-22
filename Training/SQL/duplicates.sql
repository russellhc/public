/*
delete from test
where rowid in (
select rowid from (
select 
    Name,
    Age,
    City,
    Salary,
    Bonus,
    rowid as employee_id,
    rowid,
    rank () over (partition by Name, Age, City order by rowid asc) as employee_rank
from test
) s
where s.employee_rank > 1
)
;
*/

with duplicate_check as (
SELECT id,
       name,
       age,
       department,
       count(*) over (partition by name, age, department) as dup_count,
       rank() over (partition by name, age, department order by rowid desc) as dup_rank
FROM employeesTest
order by id
)
select * from duplicate_check
where dup_rank = 1
;


