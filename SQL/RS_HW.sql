-----------------------------------
-----------------------------------
-- Задача:
В БД есть таблица Salaries, в которой у каждого сотрудника есть идентификатор, а также столбец для идентификатора отдела.
Напишите SQL-запрос, чтобы найти сотрудников, которые получают три самые высокие зарплаты в каждом отделе.
 Salary:
+----+-------+--------+-----------+
Id Name Salary DepartmentId
+----+-------+--------+-----------+
| 1 | Petr | 85000 | 1 |
| 2 | Ivan | 80000 | 2 |
| 3 | Alex | 60000 | 2 |
| 4 | Den  | 90000 | 1 |
| 5 | Bob  | 69000 | 1 |
| 6 | Kir  | 85000 | 1 |
| 7 | Mike | 76000 | 1 |
 
Department: (DepartmentId, DepartmentName)
1             IT
2             Sales


with data1 as ( select
    
    DepartmentId
    , Name
    , SUM(Salary) AS Salary

from  Salary

group by     DepartmentId
    , Name
)

select
    DepartmentId
    , Name
    , Salary

from( select

    DepartmentId
    , Name
    , Salary
    , row_number() over(partition BY DepartmentId ORDER BY Salary DESC) as rn
    
from data1 ) as data2

where rn<= 3


Правильный ответ: Запрос SQL должен возвращать следующие строки (порядок строк не имеет значения):
+-----------+----------+-----------+
Department  Employee  Salary
+-----------+----------+-----------+
| IT | Den  | 90000 |
| IT | Petr | 85000 |
| IT | Kir  | 85000 |
| IT | Mike | 76000 |
| Sales | Ivan | 80000 |
| Sales | Alex | 60000 |



-----------------------------------
-----------------------------------
-- Задача:
Есть таблица с двумя полями Id и Timestamp, где 
Id- возрастающая последовательность, каждая вставка новой записи в таблицу приводит к генерации ID(n)=ID(n-1) + 1 

Timestamp – временная метка,  при вставке задним числом может принимать любые значения меньше максимума времени всех предыдущих записей 
Вставка задним числом – операция вставки записи в таблицу при которой 
ID(n) > ID(n-1) 
Timestamp(n) < max(timestamp(1):timestamp(n-1)) 
Пример таблицы 
| 1 | 2016.09.11 | 
| 2 | 2016.09.12 | 1 | 2016.09.11
| 3 | 2016.09.13 | 2 | 2016.09.13
| 4 | 2016.09.14 | 
| 5 | 2016.09.09 | Вставка задним числом
| 6 | 2016.09.12 | Вставка задним числом
| 7 | 2016.09.15 | 

Написать код, который будет возвращать список всех id, подходящих под определение вставки задним числом.

select
    case when max1<max2 then id1 else Null end as row

from( select

    t1.id as id1
    , t1.dt as max1

    , max(t2.dt) as max2

from table1 as t1
left join table1 as t2
    on t1.id >= t2.id
     and
    
group by t1.id
    , t1.dt
   
) as xx


-----------------------------------
-----------------------------------
-- Задача:
Необходимо написать запрос который выводит последовательность цифр от 1 до 1000, 
притом если число кратно трем, то вместо числа выводится Fizz, если кратно пяти, то Buzz, а если и трем, и пяти, то FizzBuzz

select

id
, case when (( id % 3 = 0) and (id % 5 = 0)) then 
            cast( id as varchar(100) ) || 'FizzBuzz'
    when id % 3 = 0 then 
            cast( id as varchar(100) ) ||  'Fizz'
    when id % 5 = 0 then 
            cast( id as varchar(100) ) || 'Buzz' 
    else cast( id as varchar(100) )
end as flag1

from table