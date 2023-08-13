GREATER_HIRED_EMPLOYEES_THAN_MEAN_QUERY = """
with employees_by_department as (
    select department_id, count(1) as hired
    from hired_employees he
    where extract(year from hire_datetime) = %(year)s
    group by department_id
)
select d.id, d.department, e_d.hired
from employees_by_department e_d
join departments d on d.id = e_d.department_id
where hired > (
    select avg(hired) from employees_by_department
)
order by hired desc
"""
