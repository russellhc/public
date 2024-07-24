with duplicate_check as (
SELECT submission_key,
       submission_date,
       first_name,
       last_name,
       email,
       ip_address,
       count(*) over (partition by submission_key,submission_date,first_name,last_name,email,ip_address) as dup_count,
       rank() over (partition by submission_key,submission_date,first_name,last_name,email,ip_address order by rowid desc) as dup_rank
FROM sample_data
order by submission_key
)
select * from duplicate_check
where dup_count > 1
;

with duplicate_check as (
SELECT submission_key,
       submission_date,
       first_name,
       last_name,
       email,
       ip_address,
       count(*) over (partition by submission_key,submission_date,first_name,last_name) as dup_count,
       rank() over (partition by submission_key,submission_date,first_name,last_name order by rowid desc) as dup_rank
FROM sample_data
order by submission_key
)
select * from duplicate_check
where dup_count > 1
;


with duplicate_check as (
SELECT submission_key,
       submission_date,
       first_name,
       last_name,
       email,
       ip_address,
       count(*) over (partition by submission_key,submission_date,first_name,last_name) as dup_count,
       rank() over (partition by submission_key,submission_date,first_name,last_name order by rowid desc) as dup_rank
FROM sample_data
order by submission_key
)
select * from duplicate_check
where dup_count > 1
;
