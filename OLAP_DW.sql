SELECT * FROM CUSTOMERS;
SELECT * FROM DATES;
SELECT * FROM FACT_TABLE;
SELECT * FROM PRODUCTS;
SELECT * FROM STORES;
SELECT * FROM SUPPLIERS;
SELECT * FROM TRANSACTIONS;

#Question no 1 

SELECT SUPPLIERS_ID,
	Sum(CASE
		WHEN DATES.QUATER = 1 THEN QUANTITY
		ELSE 0
	end) AS "Q1",
	Sum(CASE
		WHEN DATES.QUATER = 2 THEN QUANTITY
		ELSE 0
	end) AS "Q2",
	Sum(CASE
		WHEN DATES.QUATER = 3 THEN QUANTITY
		ELSE 0
	end) AS "Q3",
	Sum(CASE
		WHEN DATES.QUATER = 4 THEN QUANTITY
		ELSE 0
	end) AS "Q4",
	Sum(CASE
		WHEN DATES.MONTH = 1 THEN QUANTITY
		ELSE 0
	end) AS 'Jan',
	Sum(CASE
		WHEN DATES.MONTH = 2 THEN QUANTITY
		ELSE 0
	end) AS 'Feb',
	Sum(CASE
		WHEN DATES.MONTH = 3 THEN QUANTITY
		ELSE 0
	end) AS 'Mar',
	Sum(CASE
		WHEN DATES.MONTH = 4 THEN QUANTITY
		ELSE 0
	end) AS 'Apr',
	Sum(CASE
		WHEN DATES.MONTH = 5 THEN QUANTITY
		ELSE 0
	end) AS 'May',
	Sum(CASE
		WHEN DATES.MONTH = 6 THEN QUANTITY
		ELSE 0
	end) AS 'June',
	Sum(CASE
		WHEN DATES.MONTH = 7 THEN QUANTITY
		ELSE 0
	end) AS 'July',
	Sum(CASE
		WHEN DATES.MONTH = 8 THEN QUANTITY
		ELSE 0
	end) AS 'Aug',
	Sum(CASE
		WHEN DATES.MONTH = 9 THEN QUANTITY
		ELSE 0
	end) AS 'Sep',
	Sum(CASE
		WHEN DATES.MONTH = 10 THEN QUANTITY
		ELSE 0
	end) AS 'Oct',
    Sum(CASE
		WHEN DATES.MONTH = 11 THEN QUANTITY
		ELSE 0
	end) AS 'Nov',
	Sum(CASE
		WHEN DATES.MONTH = 12 THEN QUANTITY
		ELSE 0
	end) AS 'Dec'
FROM FACT_TABLE
	NATURAL JOIN SUPPLIERS
	NATURAL JOIN DATES
GROUP BY SUPPLIERS_ID; 

#Questiion 2

SELECT STORE_NAME AS "Store_Name",
	PRODUCT_NAME  AS "Product_Name",
	Sum(QUANTITY) AS "Total_Sales"
FROM FACT_TABLE
	NATURAL JOIN STORES
	NATURAL JOIN PRODUCTS
GROUP BY STORE_NAME,PRODUCT_NAME
ORDER BY STORE_NAME,PRODUCT_NAME;

#Question 3 

SELECT PRODUCT_NAME,
       Sum(QUANTITY) AS 'QUANTITY SOLD'
FROM FACT_TABLE
	NATURAL JOIN PRODUCTS
	NATURAL JOIN DATES
WHERE ( WEEKDAY(DATES.DATE_ID) >= 5 )
GROUP BY PRODUCT_NAME
ORDER BY Sum(QUANTITY) DESC
LIMIT 5;

#QUESTION 4

SELECT PRODUCT_NAME,
	Sum(CASE
		WHEN DATES.QUATER = 1 THEN QUANTITY
		ELSE 0
	end) AS "Q1",
	Sum(CASE
		WHEN DATES.QUATER = 2 THEN QUANTITY
		ELSE 0
	end) AS "Q2",
	Sum(CASE
		WHEN DATES.QUATER = 3 THEN QUANTITY
		ELSE 0
	end) AS "Q3",
	Sum(CASE
		WHEN DATES.QUATER = 4 THEN QUANTITY
		ELSE 0
	end) AS "Q4"
FROM   FACT_TABLE
       NATURAL JOIN PRODUCTS
       NATURAL JOIN DATES
WHERE  DATES.YEAR = 2016
GROUP  BY PRODUCT_NAME; 

#QUESTION 5

SELECT PRODUCT_NAME,
	Sum(CASE
		WHEN DATES.MONTH < 6 THEN QUANTITY
		ELSE 0
	end) AS "1st Half",
	Sum(CASE
		WHEN DATES.MONTH >= 6 THEN QUANTITY
		ELSE 0
	end) AS "2nd Half"
FROM FACT_TABLE
	NATURAL JOIN PRODUCTS
	NATURAL JOIN DATES
WHERE DATES.YEAR = 2016
GROUP BY PRODUCT_NAME;


#QUESTION 6

SELECT * FROM   MASTERDATA WHERE  PRODUCT_NAME = 'Tomatoes'; 
# THE PRICE DIFFERENCE BETWEEN THE TWO TOMATOES ARE QUITE HUGE SO THIS DATA DOESN"T HAVE PROPER CHECKS ON PRICE.


#QUESTION 7
DROP VIEW IF EXISTS STOREANALYSIS_MV;

CREATE VIEW STOREANALYSIS_MV AS
SELECT STORE_ID,
       PRODUCT_ID,
       ROUND(Sum(SUM),2) AS SALES_TOTAL
FROM   FACT_TABLE
       NATURAL JOIN STORES
       NATURAL JOIN PRODUCTS
GROUP  BY STORE_ID,
          PRODUCT_ID;
          
SELECT * FROM STOREANALYSIS_MV;


          