create database if not exists Sales_performance_Analysis;
use Sales_performance_Analysis;

-- Table creation and data uploading--
create table if not exists sales_data(
Row_ID	INT,
Order_ID	VARCHAR(100),
Order_Date	DATE,
Ship_Date	DATE,
Ship_Mode	VARCHAR(100),
Customer_ID	VARCHAR(100),
Customer_Name	VARCHAR(100),
Segment	VARCHAR(100),
Country	VARCHAR(100),
City	VARCHAR(100),
State	VARCHAR(100),
Postal_Code	INT,
Region	VARCHAR(100),
Product_ID	VARCHAR(100),
Category	VARCHAR(100),
Sub_Category	VARCHAR(100),
Product_Name	VARCHAR(1000),
Sales	Decimal(10,2),
Quantity	Decimal(10,2),
Discount	Decimal(10,2),
Profit	Decimal(10,2) 
);
load data infile "Sample - Superstore.csv" into table Sales_data CHARACTER SET utf8mb4
fields terminated by ',' enclosed by '"' lines terminated by '\r\n' ignore 1 rows;

# Total sales and profit
select sum(sales) as Total_sales, sum(profit) as profit from sales_data;

# Monthly Sales Trend
select date_format(order_Date,'%y-%m') as Month,sum(sales) as Total_sales, sum(profit) as profit from sales_data
group by month
order by month;

# Top 5 products by sales
SELECT Sub_Category AS Product_name, SUM(Sales) AS Total_sales
FROM sales_data
GROUP BY Sub_Category
ORDER BY Total_sales DESC
LIMIT 5;

# Sales by region
SELECT Region, SUM(Sales) AS Total_sales
from sales_data
GROUP BY Region
ORDER BY Total_sales DESC
LIMIT 5;

# Agerege sale per order
SELECT AVG(Sales) as Avg_Sale_Per_Order from Sales_data;

# Orders with discount >20% and profit <0
select * from sales_data
where Profit<0 and Discount>0.2;

# Total Quantity sold per Sub-Catogery
Select Sub_Category, SUM(Quantity) as Total_Quantity
FROM sales_data
GROUP BY Sub_Category
ORDER BY Total_Quantity desc;

# Customers Segments with Highest Profits
SELECT Segment, sum(Profit) as Total_Profit
FROM Sales_Data
GROUP BY Segment
ORDER BY Total_Profit desc;


# First 10 Order Cronology

SELECT * FROM Sales_data
ORDER BY Order_date asc
LIMIT 10;

# Sales and profit by product and category
SELECT Category,Product_Name,SUM(sales) AS Total_Sales,SUM(Profit) as Total_Profit
FROM Sales_Data
GROUP BY Category,Product_Name
ORDER BY Product_Name Asc;

# TOP 3 Profitable Customers per region
SELECT Region,Customer_Name,profit From(
select Region,Customer_Name,Profit,
row_number() OVER (partition by
region order by profit desc)
as renk from sales_data) ranked_data where renk<=3;

# Order with boh high quantity >10 and high discount >15%

SELECT * FROM Sales_data
WHERE quantity>10 AND discount>0.15;

# Ship Mode Distribution;
Select Ship_mode,count(Ship_mode) 
FROM Sales_data
GROUP BY ship_mode;

# Duplicate Customer Orders

SELECT Customer_ID,Count(ORDER_ID) as Order_Count from sales_data
GROUP BY Customer_ID
HAVING Order_count >1;

# Region with most orders
SELECT Region, count(*) as Order_count from sales_data
GROUP BY Region
ORDER BY Order_count desc
LIMIT 1;

# Total Sales per Year

SELECT YEAR(Order_date) AS Year, SUM(Sales) as Total_sales from Sales_data
GROUP BY Year
ORDER BY Total_sales;


# Profit margin(Profit/Sales) by category

SELECT Category,round(sum(profit)/sum(sales)*100,2) as Profit_Margin
FROM Sales_data
GROUP BY Category;

# Customer with Highest Order value

SELECT Customer_name,SUM(Sales) as total_sales from sales_data
GROUP BY Customer_name
ORDER BY total_sales desc
limit 1;


# Use CTE TO Calculate total sales per customer

WITH CustomerSales AS(
SELECT Customer_ID,Customer_Name,Sum(sales) as Total_sales from sales_data
GROUP BY Customer_ID,Customer_Name)
SELECT * FROM CustomerSales
ORDER BY Total_sales desc;

# Top 3 customers using rank function

WITH top_3 as(
SELECT region,Customer_ID,SUM(sales) as Total_Sales , 
RANK() OVER(PARTITION BY region ORDER BY sum(sales) DESC) as sales_rank
FROM sales_data
GROUP BY region,Customer_ID)
SELECT * FROM top_3
WHERE sales_rank<3;

# Assign a row number to each order within its category, sorted by descending sales (ROW_NUMBER()).

WITH ROW_N AS(
SELECT Order_ID,Category,sales as total_sales,
ROW_NUMBER() OVER (PARTITION BY Category ORDER BY sales desc) as Row_num
FROM Sales_data
)
SELECT * FROM ROW_N
ORDER BY Category,ROW_NUM;

# For each customer, rank their orders by Profit in descending order (DENSE_RANK()).

SELECT Customer_Name,Sub_category,SUM(profit) as profit,
DENSE_RANK() OVER(PARTITION BY Sub_category ORDER BY sum(profit) desc) as O_rank
FROM Sales_data
GROUP BY Customer_Name,Sub_Category;

# Find customers who placed more than one order on the same day using ROW_NUMBER().

SELECT Order_Date,Customer_name,Sub_category,count(Order_date) as CT,
ROW_NUMBER() OVER( PARTITION BY Order_date ORDER BY count(order_date) desc) as R_no
FROM sales_data
GROUP BY Order_Date,Customer_name,Sub_category
ORDER BY CT DESC;

# Calculate the total sales per region, and show each order’s contribution (%) using SUM() OVER(PARTITION BY Region).
SELECT
	region,
    SUM(SUM(Sales)) OVER(PARTITION BY region) as Total_sales
    From sales_data
    Group by region
    ORDER BY Total_sales;

# For every product sub-category, find the highest sale using MAX() OVER(PARTITION BY Sub_Category).

SELECT 
	Sub_category,
    MAX(MAX(Sales)) OVER(PARTITION BY Sub_category) as Max_Sale
FROM sales_performance_analysis.sales_data
GROUP BY Sub_Category
ORDER BY Max_sale desc;


# For each customer, calculate their running total sales over time.

SELECT 
	Customer_name,
    SUM(SUM(Sales)) OVER(PARTITION BY Customer_name) AS total_sale
FROM Sales_data
GROUP BY Customer_name
ORDER BY Total_sale desc;

#For every order, find the previous order’s sales by the same customer (LAG()).

SELECT 
	Customer_name,
    Order_ID,
    Order_date,
    sales,
    LAG(Sales) OVER (PARTITION BY customer_name ORDER BY order_date) as previous_date
FROM Sales_data
ORDER BY order_date;


# For each order, compute the difference in sales with the next order of the same customer (LEAD()).
WITH Difference AS (
SELECT
	Customer_name,
    Order_ID,
    Order_date,
    Sales,
	LEAD(sales) OVER(PARTITION BY Customer_name ORDER BY order_date) as next_day_sale
FROM Sales_data)
SELECT *,(Sales-next_day_sale) as difference FROM Difference;

# Identify customers whose sales dropped compared to their previous order using LAG().
WITH Drop_sale AS(
SELECT 
	Customer_name,
    Order_ID,
    Order_date,
    sales,
    LAG(Sales) OVER (PARTITION BY customer_name ORDER BY order_date) as previous_date
FROM Sales_data
ORDER BY order_date)
SELECT * FROM Drop_sale
where (Sales-Previous_date)<0;


# Calculate the cumulative profit for each region sorted by Order Date.
SELECT 
	Region,
    Order_date,
	SUM(SUM(Profit)) OVER (PARTITION BY Region ORDER BY order_date) as cumulative_profit
From sales_data
GROUP BY Region,Order_date;

# Find the moving average of sales for each sub-category (over the last 3 orders).

SELECT
    Sub_Category,
    Order_Date,
    Sales,
    ROUND(AVG(Sales) OVER (
        PARTITION BY Sub_Category
        ORDER BY Order_Date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS Moving_Avg_Sales
FROM
    sales_data
ORDER BY
    Sub_Category,
    Order_Date;

#Divide all customers into 4 sales quartiles using NTILE(4) and show their quartile number.

SELECT 
	Customer_ID,
    Customer_Name,
    Sales,
    NTILE(4) OVER(PARTITION BY Customer_ID) AS percentile
FROM Sales_Data
ORDER BY Percentile DESC;

#For each category, divide its products into 3 buckets based on total sales.
SELECT 
	Category,
    Sub_Category,
    Sales,
    NTILE(3) OVER (PARTITION BY Category ORDER BY Sales desc) as Bucket
From Sales_Data;

# Calculate the % difference in sales between consecutive orders for each customer.

with diff as (
SELECT
	Order_id,
    Customer_Name,
    sales,
    LAG(Sales) OVER(PARTITION BY Customer_name ORDER BY Sales desc) as last_Sale
FROM Sales_Data)
select *,
	round((Sales-last_sale)/Last_sale*100,2) as per_diff 
	From diff;

# For each customer, find their first and last order date using FIRST_VALUE() and LAST_VALUE().
SELECT DISTINCT
	Customer_ID,
	Customer_Name,
    FIRST_VALUE(Order_date) OVER (PARTITION BY Customer_name ORDER BY Order_date) as First_date,
    LAST_VALUE(Order_date) OVER (PARTITION BY Customer_name ORDER BY Order_date) as Last_date
FROM Sales_data;


# Find all customers where their latest order’s profit was less than their previous one.

with profit as (
SELECT
	Customer_id,
    Customer_name,
    Profit,
    LAG(Profit) OVER (PARTITION BY Customer_ID ORDER BY Profit DESC) AS prev_profit
FROM Sales_data)
SELECT * FROM Profit
WHERE Profit<prev_profit;

#For each region, find the 2nd most profitable sub-category using RANK().

WITH Profit as(
SELECT
	Region,
    Sub_category,
    Sum(profit) as total_Profit,
    RANK() OVER(PARTITION BY Region ORDER BY sum(profit) desc) as Ranked
FROM Sales_data
GROUP BY Region,Sub_category
ORDER BY Region)
SELECT * FROM PROFIT
WHERE Ranked=2;

#For every product, show: Total sales, Average sales, Rank of the product within its category based on total sales
SELECT
	Category,
	Sub_category,
    SUM(SUM(Sales)) OVER (PARTITION BY Sub_category) as Total_Sale,
    AVG(AVG(Sales)) OVER (PARTITION BY Sub_category) as AVG_Sale,
    RANK() OVER (PARTITION BY Category ORDER BY sum(Sales) DESC) as Sales_Rank
FROM Sales_data
GROUP BY Category,Sub_category;

# Find the customer who contributed the highest % of sales in each region.

WITH Regional_Sales AS (
    SELECT
        Region,
        Customer_Name,
        SUM(Sales) AS Customer_Sales,
        SUM(SUM(Sales)) OVER (PARTITION BY Region) AS Total_Regional_Sales
    FROM
        sales_data
    GROUP BY
        Region, Customer_Name
),
Customer_Percent AS (
    SELECT
        Region,
        Customer_Name,
        Customer_Sales,
        Total_Regional_Sales,
        ROUND((Customer_Sales / Total_Regional_Sales) * 100, 2) AS Sales_Percentage,
        RANK() OVER (
            PARTITION BY Region
            ORDER BY Customer_Sales DESC
        ) AS Sales_Rank
    FROM
        Regional_Sales
)
SELECT
    Region,
    Customer_Name,
    Customer_Sales,
    Total_Regional_Sales,
    Sales_Percentage
FROM
    Customer_Percent
WHERE
    Sales_Rank = 1
ORDER BY
    Region;