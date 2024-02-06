-- 0. Подготовка среды
create database core_layer;
create database datamart_layer;

drop table if exists core_layer.customers;
drop table if exists core_layer.product_category;
drop table if exists core_layer.products;
drop table if exists core_layer.order_product;
drop table if exists core_layer.orders;
drop table if exists datamart_layer.customer_dim;
drop table if exists datamart_layer.month_dim;
drop table if exists datamart_layer.product_category_dim;
drop dictionary if exists datamart_layer.dict_product;
drop table if exists datamart_layer.product_dim;
drop table if exists datamart_layer.sales_fact;

-- 1. Документация ClickHouse https://clickhouse.com/docs/en/quick-start

-- 2. Тестовый вызов схем - убедимся еще раз, что мы подключились к базе данных

show databases;
select version();


-- 3. Посмотрим таблицы raw_layer (заранее предподготовлены и загружены)

select *
from raw_layer.auth_customers;

select *
from raw_layer.crm_orders;

select *
from raw_layer.crm_products;


-- 4. Создание тестовой таблицы в схеме core_layer
CREATE TABLE core_layer.test (
    test_id Int32,
    test_name String
)
ENGINE = MergeTree()
ORDER BY (test_id);

select *
from core_layer.test;

drop table core_layer.test;


-- 5. Создание прототипов таблиц в схеме core_layer на базе физической модели данных Core Data Layer

-- 5.1 таблица покупателей Customers
create table core_layer.customers (
    customer_id Int32,
    customer_name String,
    phone String
)
ENGINE = MergeTree()
ORDER BY (customer_id)
as
select
    customer_id,
    customer_name,
    phone
from
    raw_layer.auth_customers;


-- 5.2 таблица категории продуктов Product_Category
create table core_layer.product_category (
    category_id Int32,
    category_name String,
    category_description String
)
ENGINE = MergeTree()
ORDER BY (category_id)
as
select
    ROW_NUMBER() over(ORDER BY category_name) as category_id, -- т.к. нет на источнике кода категории, определяем синтетический первичный ключ
    category_name,
    category_description
from (
    select distinct
        category as category_name,
        category_description
    from
        raw_layer.crm_products
) as t;


-- 5.3 таблица продуктов Products
create table core_layer.products (
    product_id Int32,
    product_code String,
    product_name String,
    category_id Int32,
    net_cost Float32
)
ENGINE = MergeTree()
ORDER BY (product_id)
as
select
    ROW_NUMBER() over (ORDER BY product_id), -- т.к. нет на источнике кода типа int, определяем синтетический первичный ключ
    product_id as product_code,
    product_name,
    pc.category_id, -- связь с категорией продукта определяем через left join
    net_cost
from
    raw_layer.crm_products as p
    left join core_layer.product_category as pc
        on p.category = pc.category_name;


-- 5.4 таблица связи продукта с заказом Order_Product
create table core_layer.order_product (
    order_id Int32,
    product_id Int32,
    quantity Int32
)
ENGINE = MergeTree()
ORDER BY (order_id, product_id)
as
select
    order_id,
    p.product_id,
    quantity
from
    raw_layer.crm_orders as co
    left join core_layer.products as p
        on co.product_id = p.product_code;

    
-- 5.5 таблица заказов Orders
create table core_layer.orders (
    order_id Int32,
    order_date Date,
    customer_id Int32,
    sales Float32
)
ENGINE = MergeTree()
ORDER BY (order_id)
as
SELECT distinct
    order_id, -- т.к. на источнике продукты и заказы находятся в одной таблице, уникальность кода заказа не соблюдена
    order_date,
    customer_id,
    sales
from
    raw_layer.crm_orders as co;


-- 6. Посмотрим таблицы core_layer
select *
from core_layer.customers;

select *
from core_layer.product_category;

select *
from core_layer.products;

select *
from core_layer.order_product;

select *
from core_layer.orders;


-- 7. Создание прототипов таблиц в схеме datamart_layer на базе физической модели данных Data Mart Layer
create table datamart_layer.customer_dim (
    customer_id Int32,
    customer_name String,
    phone String
)
ENGINE = MergeTree()
ORDER BY (customer_id)
as
select
    customer_id,
    customer_name,
    phone
from
    raw_layer.auth_customers;

-- 7.1 таблица месяцев Month_dim (измерение)
-- обратимся к функционалу ClickHouse https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions/#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters
create table datamart_layer.month_dim (
    month_dt Date,
    month_id Int32,
    month_number Int32,
    month_name String,
    year_number Int32
)
ENGINE = MergeTree()
ORDER BY (month_id)
as
select distinct
    date_trunc('month', day_dt) as month_dt,
    toYYYYMM(day_dt) AS month_id,
    toMonth(day_dt) as month_number,
    dateName('month', day_dt) as month_name,
    dateName('year', day_dt) as year_number
from (
    select
        DateAdd(day, n.number, toDate('2003-01-01')) as day_dt
    from
        numbers(0, 365 * 21) as n
);


-- 7.2 таблица категорий продуктов Product_Category_dim (измерение)
create table datamart_layer.product_category_dim (
    category_id Int32,
    category_name String,
    category_description String
)
ENGINE = MergeTree()
ORDER BY (category_id)
as 
select
    category_id,
    category_name,
    category_description 
from
    core_layer.product_category as pc;


-- 7.3 таблица продуктов Product_dim (измерение)
create table datamart_layer.product_dim (
    product_id Int32,
    product_code String,
    product_name String,
    net_cost Float32,
    category_id Int32,
)
ENGINE = MergeTree()
ORDER BY (product_id)
as
select
    product_id,
    product_code,
    product_name,
    net_cost,
    category_id
from
    core_layer.products as p;


-- 7.4 таблица продаж с расчетом выручки по продукту в месяц Sales_fact (факт)
create table datamart_layer.sales_fact (
    month_id Int32,
    product_id Int32,
    sales Float32,
    net_cost_total Float32
)
ENGINE = MergeTree()
ORDER BY (month_id, product_id)
as 
select
    toYYYYMM(o.order_date) AS month_id,
    op.product_id,
    sum(o.sales) as sales, -- посчитаем выручку как сумму от всех заказов
    sum(op.quantity * p.net_cost) as net_cost_total -- посчитаем расходы на продукты как сумму от кол-ва продукта, умноженного на себестоимость
from
    core_layer.order_product as op
    left join core_layer.products as p
        on op.product_id = p.product_id
    left join core_layer.orders as o
        on op.order_id = o.order_id
group by 1, 2;


-- 8. Посмотрим таблицы datamart_layer
select * from datamart_layer.month_dim;

select * from datamart_layer.product_category_dim;

select * from datamart_layer.product_dim;

select * from datamart_layer.sales_fact;


-- 9. Необходимо посчитать, какой месяц и по какому продукту была самая высокая выручка
select
    pd.product_code,
    pd.product_name,
    md.month_dt,
    sf.sales
from
    datamart_layer.sales_fact as sf
    left join datamart_layer.product_dim as pd
        on sf.product_id = pd.product_id
    left join datamart_layer.month_dim as md
        on sf.month_id = md.month_id
order by sf.sales desc
limit 5;


-- 10. Необходимо посчитать, какой месяц и по какому продукту была самая высокая прибыль
select
    pd.product_code,
    pd.product_name,
    md.month_dt,
    sf.sales - sf.net_cost_total as profit
from
    datamart_layer.sales_fact sf
    left join datamart_layer.product_dim pd
        on sf.product_id = pd.product_id
    left join datamart_layer.month_dim md
        on sf.month_id = md.month_id
order by profit desc
limit 5;


-- 11. Использование словарей
create dictionary datamart_layer.dict_product (
  product_id Int32,
  product_code String,
  product_name String
)
PRIMARY KEY product_id
source (CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'admin' DB 'datamart_layer' TABLE 'product_dim' PASSWORD 'admin'))
LAYOUT (HASHED())
LIFETIME (10);


select
    dictGet('datamart_layer.dict_product', 'product_code', sf.product_id) as product_code,
    dictGet('datamart_layer.dict_product', 'product_name', sf.product_id) as product_name,
    md.month_dt,
    sf.sales - sf.net_cost_total as profit
from
    datamart_layer.sales_fact sf
    left join datamart_layer.month_dim md
        on sf.month_id = md.month_id
order by profit desc
limit 5;
