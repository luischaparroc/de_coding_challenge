EMPLOYEES_BY_QUARTER_QUERY = """
with combination_keys as (
    select department_id, job_id
    from hired_employees he
    where extract(year from hire_datetime) = %(year)s
    group by department_id, job_id
), quarter_1 as (
    select department_id, job_id, count(1) as number_employees
    from hired_employees he
    where extract(year from hire_datetime) = %(year)s and extract(quarter from hire_datetime) = 1
    group by department_id, job_id
), quarter_2 as (
    select department_id, job_id, count(1) as number_employees
    from hired_employees he
    where extract(year from hire_datetime) = %(year)s and extract(quarter from hire_datetime) = 2
    group by department_id, job_id
),  quarter_3 as (
    select department_id, job_id, count(1) as number_employees
    from hired_employees he
    where extract(year from hire_datetime) = %(year)s and extract(quarter from hire_datetime) = 3
    group by department_id, job_id
), quarter_4 as (
    select department_id, job_id, count(1) as number_employees
    from hired_employees he
    where extract(year from hire_datetime) = %(year)s and extract(quarter from hire_datetime) = 4
    group by department_id, job_id
)
select d.department, j.job,
       case when q_1.number_employees is null then 0 else q_1.number_employees end as q1,
       case when q_2.number_employees is null then 0 else q_2.number_employees end as q2,
       case when q_3.number_employees is null then 0 else q_3.number_employees end as q3,
       case when q_4.number_employees is null then 0 else q_4.number_employees end as q4
from combination_keys c_k
join departments d on d.id = c_k.department_id
join jobs j on j.id = c_k.job_id
left join quarter_1 q_1 on (q_1.department_id = c_k.department_id) and (q_1.job_id = c_k.job_id)
left join quarter_2 q_2 on (q_2.department_id = c_k.department_id) and (q_2.job_id = c_k.job_id)
left join quarter_3 q_3 on (q_3.department_id = c_k.department_id) and (q_3.job_id = c_k.job_id)
left join quarter_4 q_4 on (q_4.department_id = c_k.department_id) and (q_4.job_id = c_k.job_id)
order by d.department, j.job;
"""
