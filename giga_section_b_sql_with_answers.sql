-- Section B:

-- 1. Top 5 selling products in 2024

SELECT
  product_id,
  SUM(quantity) AS total_quantity_sold,
  SUM(sale_amount) AS total_sale_amount
FROM `project.giga.transactions`
WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
  AND is_return != TRUE
GROUP BY product_id
ORDER BY total_sale_amount DESC
LIMIT 5;


-- 2. Month-over-Month (MoM) percentage growth of total sale_amount in 2024

WITH monthly_sales AS (
  SELECT
    DATE_TRUNC(sale_date, MONTH) AS month_start,
    SUM(sale_amount) AS total_sale_amount
  FROM `project.giga.transactions`
  WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
    AND is_return != TRUE
  GROUP BY month_start
),
with_prev AS (
  SELECT
    month_start,
    total_sale_amount,
    LAG(total_sale_amount) OVER (ORDER BY month_start) AS prev_month_amount
  FROM monthly_sales
)
SELECT
  month_start AS month,
  total_sale_amount,
  CASE
    WHEN prev_month_amount IS NULL THEN 0.0
    WHEN prev_month_amount = 0 THEN NULL
    ELSE 100.0 * (total_sale_amount - prev_month_amount)
               / prev_month_amount
  END AS mom_pct_growth
FROM with_prev
ORDER BY month;


-- 3. Distinct active 'Gold' customers with non-return purchases > $100

SELECT
  COUNT(DISTINCT t.customer_id) AS active_gold_customers_over_100
FROM `project.giga.transactions` AS t
JOIN `project.giga.customer_profiles` AS c
  ON t.customer_id = c.customer_id
WHERE c.loyalty_tier = 'Gold'
  AND c.is_active = TRUE
  AND t.is_return != TRUE
  AND t.sale_amount > 100;


-- 4. Latest version of each transaction based on ingestion_timestamp

CREATE OR REPLACE VIEW `project.giga.latest_transactions` AS
SELECT
  *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY ingestion_timestamp DESC
    ) AS rn
  FROM `project.giga.transactions`
) t
WHERE t.rn = 1;


-- 5. Customer Lifetime Value (CLV)

SELECT
  c.customer_id,
  c.country,
  COALESCE(
    SUM(
      CASE
        WHEN t.is_return THEN -t.sale_amount
        ELSE t.sale_amount
      END
    ),
    0
  ) AS clv_amount
FROM `giga.customer_profiles` AS c
LEFT JOIN `project.giga.latest_transactions` AS t
  ON t.customer_id = c.customer_id
  AND t.sale_date >= c.registration_date
GROUP BY
  c.customer_id,
  c.country
ORDER BY
  clv_amount DESC;
