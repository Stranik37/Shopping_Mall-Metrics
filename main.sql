-- В данном проекте мы будем рассчитывать продуктовые метрики для топ 5-ти торговых центров
-- А именно такие метрики, как GMV (оборот), AOV (Average Order Value, средний чек),
-- ARPPU (Average Revenue Per Paying User) и
-- Retention Rate с применением когортного анализа


-- Для начала создадим таблицу для наших данных
CREATE TABLE Customer_Shopping (
invoice_no character(30), 
customer_id character(30),
gender character(20),
age int,
category character(30),
quantity int,
price float8,
payment_method character(30),
invoice_date date,
shopping_mall character(30),
PRIMARY KEY (invoice_no, customer_id)
);

-- Далее перенесем данные из локальной папки на пк в нашу таблицу
COPY Customer_Shopping(invoice_no, customer_id, gender, age, category, 
quantity, price, payment_method, invoice_date, shopping_mall)
FROM 'C:/Portfolio/datasets/customer_shopping_data.csv'
DELIMITER ','
CSV HEADER;

-- Далее рассчитаем все указанные выше метрики для 5-ти успешных торговых центров, по GMV, в разрере месячных когорт
-- И сразу же создадим таблицу на основе этих данных
CREATE TABLE shoppingMalls_metrics AS
with t1 as (
	select customer_id, date(date_trunc('month', min(invoice_date))) as cohort_month
	from Customer_Shopping
	group by customer_id
)
SELECT shopping_mall,
	t1.cohort_month,
	sum(quantity * price) as GMV, 
	sum(quantity * price) / count(distinct invoice_no) as AOV,
	date(date_trunc('month',invoice_date)) as  purchase_month,
	row_number()over(partition by t1.cohort_month order by date(date_trunc('month',invoice_date))) purchase_month_number,
	count(distinct cs.customer_id) as customer_created,
	count(distinct cs.customer_id)*1.0/min(cohort_size) as retention,
	sum(quantity * price)/count(distinct cs.customer_id) as arppu_created
from Customer_Shopping cs 
	join t1 on cs.customer_id = t1.customer_id
	join (
		select cohort_month, count(*) as cohort_size
		from t1
		group by cohort_month
	) as t2 on t1.cohort_month = t2.cohort_month
where shopping_mall in (
    select shopping_mall
    from Customer_Shopping
    group by shopping_mall
    order by sum(price * quantity) DESC
    limit 5
)
group by shopping_mall, t1.cohort_month, date(date_trunc('month', invoice_date))
order by shopping_mall, t1.cohort_month, date(date_trunc('month', invoice_date));

-- Проверим нашу таблицу
select * from shoppingMalls_metrics;

--Теперь загрузим данные в csv файл, для его дальнейшей передачи в BI Yandex DataLens
COPY shoppingMalls_metrics TO 'C:/Portfolio/Project-Customer_Shopping/top_5_shopping_malls.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');