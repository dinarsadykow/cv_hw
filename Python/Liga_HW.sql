---------------------------------------------------------
---------------------------------------------------------
--[SQL]
---------------------------------------------------------
---------------------------------------------------------

--1. Какой запрос вернет самое большое число? Почему?
--Для справки: (A right join B) эквивалентно (B left join A).

---------------------------------------------------------
-- (A) -- Точно нет, where поверх ограничиывает выборку по таблице second
select 
	count(distinct first.counter_column)

from first

left join second 
on first.join_key = second.join_key

where second.filter_column >= 42
;
---------------------------------------------------------
-- (B) -- ПРАВИЛЬНЫЙ ответ, т.к. (A lef join B) не ограничивает выборку таблицы first 
select 
	count(distinct first.counter_column)

from first

left join second 
on first.join_key = second.join_key 
and second.filter_column >= 42
;
---------------------------------------------------------
-- (C) -- Точно нет, where поверх ограничиывает выборку по таблице second
select 
	count(distinct first.counter_column)

from first

right join second 
on first.join_key = second.join_key

where second.filter_column >= 42
;
---------------------------------------------------------
-- (D) -- Точно нет, т.к. (B left join A), ограничивает выборку таблицы first
select 
	count(distinct first.counter_column)

from first

right join second 
on first.join_key = second.join_key 
and second.filter_column >= 42
;

---------------------------------------------------------
---------------------------------------------------------
--2. Для этого задания нам потребуются несколько дополнительных таблиц:

select
	
	-- Таблица, информация о заказах
	A.id --ID заказа
	, A.Date --Время оформления заказа
	, A.ClientID --ID пользователя
	, A.ClientOrderStateID --ID статуса заказа 
	--(1 – оформлен, 2 – получен, 3 – отменен)

	-- Таблица, информация о товарах в заказе
	, B.ClientOrderID --ID заказа
	, B.ItemID --ID товара
	, B.categoryLvl1 --Коммерческая категория 1-го уровня

	-- Таблица, дополнительные атрибуты заказа
	, C.ClientOrderID --ID заказа
	, C.code --Название атрибута 
	--(OrderType – тип заказа, 
	--Platform – платформа, с которой совершен заказ)
	, C.value --Значение атрибута (OrderType

from ClientOrder as A 

join ClientOrderItems as B 
on A.id=B.ClientOrderID --ID заказа

join ClientOrderAdditionalInfo as C 
on A.id=C.ClientOrderID --ID заказа
;

---------------------------------------------------------
--[Задача:]
-- 1. Найти среднее время между первым и вторым заказом у пользователей. 
-- В запросе запрещается использование JOIN’ов. 
-- Тестовые заказы фильтровать не нужно.

---------------------------------------------------------
--[Решение:]

with D as (
	select
		
		-- Таблица, информация о заказах
		A.id --ID заказа
		, A.Date --Время оформления заказа
		, A.ClientID --ID пользователя
		, A.ClientOrderStateID --ID статуса заказа 
		--(1 – оформлен, 2 – получен, 3 – отменен)
		, row_number() over( partition BY A.ClientID ORDER BY A.Date ) as rn

	from ClientOrder as A

	where 1=1
		and A.ClientOrderStateID = 1 --Выбор записи, когда Заказ оформлен
)

select

	avg(F.date_delta) AS date_avg -- ОТВЕТ на вопрос задачи, среднее время по всем Клиентам

from( select

			E.ClientID
			, (date_max-date_min) as date_delta -- ОТВЕТ на вопрос задачи, среднее время на Клиента

		from(  select

					D.ClientID
					, MIN( D.Date ) as date_min
					, MAX( D.Date ) as date_max

				from D

				where D.rn between 1 and 2

				group by D.ClientID
		) as E
) as F
;


---------------------------------------------------------
--[Задача:]
-- 2. Для каждой пары платформы и категории товара найти топ-3 пользователей, 
-- у которых наименьшее количество дней между первым 
-- и последним не тестовым заказом товаров из этой категории.

---------------------------------------------------------
--[Решение:]
-- Считаем, что одна уник запись в табл ClientOrder на [A.Date]+[A.ClientID]+[A.ClientOrderStateID]

with D as (
	select

		B.categoryLvl1 --Коммерческая категория 1-го уровня
		, C.value --Значение атрибута (OrderType=Platform)
		, A.ClientID --ID пользователя
		, min(A.Date) as date_min
		, max(A.Date) as date_max

	from ClientOrder as A 

	join ClientOrderItems as B 
	on A.id=B.ClientOrderID --ID заказа

	join ClientOrderAdditionalInfo as C 
	on A.id=C.ClientOrderID --ID заказа
	and C.code='Platform'

	where 1=1
		and A.ClientOrderStateID = 1 --Выбор записи, когда Заказ оформлен
		and A.id not in --Заказ НЕ отменен, т.е. не тестовый, если правильно понял
				( select 
					distinct id 
				from ClientOrder 
				where ClientOrderStateID=3) 
)

-- ОТВЕТ, топ-3 пользователей для каждой пары платформы и категории
select

			F.categoryLvl1
			, F.value
			, F.ClientID
			, F.rn

from (	select
			
			E.categoryLvl1
			, E.value
			, E.ClientID
			, row_number() over( partition BY 
						E.categoryLvl1
						, E.value ORDER BY E.date_delta ) as rn

		from (
			select
				D.categoryLvl1
				, D.value
				, D.ClientID
				, (date_max-date_min) as date_delta
			from D
			group by
				D.categoryLvl1
				, D.value
				, D.ClientID
		) as E
) AS F	

-- у которых наименьшее количество дней между первым и последним 
-- не тестовым заказом товаров из этой категории
where F.rn <=3
;
