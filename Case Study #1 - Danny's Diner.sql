 -- 0.0. Create Table
   
CREATE TABLE sales (
  "customer_id" VARCHAR(1),
  "order_date" DATE,
  "product_id" INTEGER
);

INSERT INTO sales
  ("customer_id", "order_date", "product_id")
VALUES
  ('A', '2021-01-01', '1'),
  ('A', '2021-01-01', '2'),
  ('A', '2021-01-07', '2'),
  ('A', '2021-01-10', '3'),
  ('A', '2021-01-11', '3'),
  ('A', '2021-01-11', '3'),
  ('B', '2021-01-01', '2'),
  ('B', '2021-01-02', '2'),
  ('B', '2021-01-04', '1'),
  ('B', '2021-01-11', '1'),
  ('B', '2021-01-16', '3'),
  ('B', '2021-02-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-07', '3');
 

CREATE TABLE menu (
  "product_id" INTEGER,
  "product_name" VARCHAR(5),
  "price" INTEGER
);

INSERT INTO menu
  ("product_id", "product_name", "price")
VALUES
  ('1', 'sushi', '10'),
  ('2', 'curry', '15'),
  ('3', 'ramen', '12');
  

CREATE TABLE members (
  "customer_id" VARCHAR(1),
  "join_date" DATE
);

INSERT INTO members
  ("customer_id", "join_date")
VALUES
  ('A', '2021-01-07'),
  ('B', '2021-01-09');

-- 0.00. Join All The Things
DROP TABLE IF EXISTS
SELECT s.customer_id, s.order_date, mn.product_name, mn.price, 
    (CASE WHEN m.join_date IS NOT NULL AND s.order_date >= m.join_date THEN 'Y'
    WHEN m.join_date IS NULL THEN 'N'
    ELSE 'N'
    END) AS member
INTO dannys_diner
FROM sales AS s
LEFT JOIN members AS m 
ON s.customer_id = m.customer_id
LEFT JOIN menu AS mn
ON s.product_id = mn.product_id

/* --------------------
   Case Study Questions
   --------------------*/
-- 1. What is the total amount each customer spent at the restaurant?
SELECT dannys_diner.customer_id, SUM(dannys_diner.price) AS total_spent
FROM dannys_diner
GROUP BY dannys_diner.customer_id;

-- 2. How many days has each customer visited the restaurant?
SELECT dannys_diner.customer_id, COUNT (DISTINCT dannys_diner.order_date) AS countdays
FROM dannys_diner
GROUP BY dannys_diner.customer_id;

-- 3. What was the first item from the menu purchased by each customer?
WITH PURCHASED_RANK AS (
    			        SELECT customer_id, 
			                    order_date,
			                    product_name, 
    			                  DENSE_RANK() OVER(ORDER BY order_date ) AS RANK
                        FROM dannys_diner
)
SELECT customer_id,product_name
FROM PURCHASED_RANK 
WHERE RANK =1 
GROUP BY customer_id,product_name;

-- 4. What is the most purchased item on the menu and how many times was it purchased by all customers?
SELECT TOP 1 product_name AS most_purchased_item, COUNT(order_date) OVER (PARTITION BY product_name) AS purchased_time
FROM dannys_diner
ORDER BY purchased_time DESC;

-- 5. Which item was the most popular for each customer?
WITH rank_favorite_item AS (
                            SELECT customer_id,
                                    product_name, 
                                    COUNT(product_name) AS NUMBER_ORDER,
                                    rank() OVER(PARTITION BY customer_id 
                            ORDER BY COUNT(product_name) DESC) AS RANK
                            FROM dannys_diner
                            GROUP BY customer_id,product_name
)
SELECT customer_id,product_name,NUMBER_ORDER
FROM rank_favorite_item
WHERE RANK =1;

-- 6. Which item was purchased first by the customer after they became a member?
WITH rank_purchase_item AS(
                            SELECT sales.customer_id, 
                                    product_name, 
                                    order_date, 
                                    join_date,
                                    DENSE_RANK() OVER(Partition by sales.customer_id ORDER BY order_date) AS RANK 
                            FROM sales 
                            INNER JOIN menu
                            ON sales.product_id = menu.product_id
                            INNER JOIN members
                            ON sales.customer_id = members.customer_id
                            WHERE sales.order_date >= members.join_date
)
SELECT customer_id, product_name
FROM rank_purchase_item
WHERE RANK =1;

-- 7. Which item was purchased just before the customer became a member?
WITH rank_purchase_item AS(
                            SELECT sales.customer_id, 
                                    product_name, 
                                    order_date, 
                                    join_date,
                                    DENSE_RANK() OVER(Partition by sales.customer_id ORDER BY order_date) AS RANK 
                            FROM sales 
                            INNER JOIN menu
                            ON sales.product_id = menu.product_id
                            INNER JOIN members
                            ON sales.customer_id = members.customer_id
                            WHERE sales.order_date < members.join_date
)
SELECT customer_id, product_name
FROM rank_purchase_item
WHERE RANK =1;

--8/What is the total items and amount spent for each member before they became a member?
SELECT  sales.customer_id,
        count(product_name) as total_item, 
        SUM(price) as Total_amount_spent
FROM sales 
INNER JOIN menu
ON sales.product_id = menu.product_id
INNER JOIN members
ON sales.customer_id = members.customer_id
WHERE sales.order_date < members.join_date
GROUP BY sales.customer_id;

--9/If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
WITH point_table AS(
                    SELECT product_id,product_name, price,
                    CASE WHEN product_name = 'sushi' THEN price *20 
                        ELSE price * 10 END AS point 
                    FROM menu 
)

SELECT customer_id, sum(point ) as total_point
FROM sales s
INNER JOIN point_table p
ON s.product_id = p.product_id
GROUP BY  customer_id;

-- 10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?
WITH dates_cte AS(
		SELECT *, 
			DATEADD(DAY, 6, join_date) AS valid_date, 
			EOMONTH('2021-01-1') AS last_date
		FROM members
)

SELECT s.customer_id,
	sum(CASE WHEN s.product_id = 1 THEN price*20
		WHEN s.order_date between d.join_date and d.valid_date THEN price*20
		ELSE price*10 
	END) as total_points
FROM dates_cte d,
     sales s,
     menu m
WHERE d.customer_id = s.customer_id 
	AND m.product_id = s.product_id
	AND s.order_date <= d.last_date
GROUP BY s.customer_id;

--Extra 
WITH member_table as (  SELECT  s.customer_id,
                                s.order_date,m.product_name,m.price,
                                CASE WHEN s.order_date >= me.join_date THEN 'Y'
                                ELSE 'N'  END AS member
                        FROM sales s
                        INNER JOIN menu m ON s.product_id = m.product_id
                        LEFT JOIN members me ON s.customer_id = me.customer_id
		)
SELECT  customer_id,
	order_date,
	product_name,
	price,
	member,
    CASE WHEN member ='N' THEN NULL 
    ELSE RANK() OVER(PARTITION BY customer_id, member ORDER BY order_date) END AS ranking
FROM member_table
