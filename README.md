# project2-edulyt

-- First, let's create a table and import the CSV data
CREATE TABLE credit_card_transactions (
    [index] INT,
    City VARCHAR(255),
    Date DATE,
    [Card Type] VARCHAR(50),
    [Exp Type] VARCHAR(50),
    Gender CHAR(1),
    Amount DECIMAL(10, 2)
);

-- Import data from CSV
BULK INSERT credit_card_transactions
FROM 'C:\Users\hp\Downloads\Data-&-Problem-Statement---Project----2\Credit card transactions - Project - 2.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

-- 1. Top 5 cities with highest spends and their percentage contribution
WITH total_spend AS (
    SELECT SUM(Amount) AS total_amount
    FROM credit_card_transactions
)
SELECT TOP 5
    City,
    SUM(Amount) AS city_spend,
    (SUM(Amount) * 100.0 / (SELECT total_amount FROM total_spend)) AS percentage_contribution
FROM credit_card_transactions
GROUP BY City
ORDER BY city_spend DESC;

-- 2. Highest spend month and amount for each card type
WITH monthly_spend AS (
    SELECT 
        [Card Type],
        DATEPART(YEAR, Date) AS Year,
        DATEPART(MONTH, Date) AS Month,
        SUM(Amount) AS monthly_amount,
        DENSE_RANK() OVER (PARTITION BY [Card Type] ORDER BY SUM(Amount) DESC) AS rank
    FROM credit_card_transactions
    GROUP BY [Card Type], DATEPART(YEAR, Date), DATEPART(MONTH, Date)
)
SELECT 
    [Card Type],
    CONCAT(Year, '-', Month) AS highest_spend_month,
    monthly_amount
FROM monthly_spend
WHERE rank = 1;

-- 3. Transaction details when cumulative spend reaches 1000000 for each card type
WITH cumulative_spend AS (
    SELECT *,
        SUM(Amount) OVER (PARTITION BY [Card Type] ORDER BY Date, Amount) AS cumulative_amount,
        ROW_NUMBER() OVER (PARTITION BY [Card Type] ORDER BY SUM(Amount) OVER (PARTITION BY [Card Type] ORDER BY Date, Amount)) AS rn
    FROM credit_card_transactions
)
SELECT *
FROM cumulative_spend
WHERE rn = (SELECT MIN(rn) FROM cumulative_spend cs2 WHERE cs2.[Card Type] = cumulative_spend.[Card Type] AND cs2.cumulative_amount >= 1000000);

-- 4. City with lowest percentage spend for gold card type
WITH gold_spend AS (
    SELECT 
        City,
        SUM(CASE WHEN [Card Type] = 'Gold' THEN Amount ELSE 0 END) AS gold_amount,
        SUM(Amount) AS total_amount
    FROM credit_card_transactions
    GROUP BY City
)
SELECT TOP 1
    City,
    (gold_amount * 100.0 / total_amount) AS gold_percentage
FROM gold_spend
WHERE gold_amount > 0
ORDER BY gold_percentage ASC;

-- 5. City with highest and lowest expense type
WITH expense_rank AS (
    SELECT 
        City,
        [Exp Type],
        SUM(Amount) AS total_amount,
        RANK() OVER (PARTITION BY City ORDER BY SUM(Amount) DESC) AS highest_rank,
        RANK() OVER (PARTITION BY City ORDER BY SUM(Amount) ASC) AS lowest_rank
    FROM credit_card_transactions
    GROUP BY City, [Exp Type]
)
SELECT 
    City,
    MAX(CASE WHEN highest_rank = 1 THEN [Exp Type] END) AS highest_expense_type,
    MAX(CASE WHEN lowest_rank = 1 THEN [Exp Type] END) AS lowest_expense_type
FROM expense_rank
GROUP BY City;

-- 6. Percentage contribution of spends by females for each expense type
WITH female_spend AS (
    SELECT 
        [Exp Type],
        SUM(CASE WHEN Gender = 'F' THEN Amount ELSE 0 END) AS female_amount,
        SUM(Amount) AS total_amount
    FROM credit_card_transactions
    GROUP BY [Exp Type]
)
SELECT 
    [Exp Type],
    (female_amount * 100.0 / total_amount) AS female_percentage
FROM female_spend
ORDER BY female_percentage DESC;

-- 7. Card and expense type combination with highest month-over-month growth in Jan-2014
WITH monthly_spend AS (
    SELECT 
        [Card Type],
        [Exp Type],
        DATEPART(YEAR, Date) AS Year,
        DATEPART(MONTH, Date) AS Month,
        SUM(Amount) AS monthly_amount
    FROM credit_card_transactions
    GROUP BY [Card Type], [Exp Type], DATEPART(YEAR, Date), DATEPART(MONTH, Date)
),
growth_calc AS (
    SELECT 
        [Card Type],
        [Exp Type],
        Year,
        Month,
        monthly_amount,
        LAG(monthly_amount) OVER (PARTITION BY [Card Type], [Exp Type] ORDER BY Year, Month) AS prev_month_amount,
        (monthly_amount - LAG(monthly_amount) OVER (PARTITION BY [Card Type], [Exp Type] ORDER BY Year, Month)) / 
            NULLIF(LAG(monthly_amount) OVER (PARTITION BY [Card Type], [Exp Type] ORDER BY Year, Month), 0) * 100 AS growth_percentage
    FROM monthly_spend
)
SELECT TOP 1
    [Card Type],
    [Exp Type],
    growth_percentage
FROM growth_calc
WHERE Year = 2014 AND Month = 1
ORDER BY growth_percentage DESC;

-- 8. City with highest total spend to total transactions ratio during weekends
WITH weekend_spend AS (
    SELECT 
        City,
        SUM(Amount) AS total_spend,
        COUNT(*) AS total_transactions
    FROM credit_card_transactions
    WHERE DATEPART(WEEKDAY, Date) IN (1, 7)  -- Assuming 1 is Sunday and 7 is Saturday
    GROUP BY City
)
SELECT TOP 1
    City,
    (total_spend * 1.0 / total_transactions) AS spend_transaction_ratio
FROM weekend_spend
ORDER BY spend_transaction_ratio DESC;

-- 9. City that took least number of days to reach its 500th transaction
WITH city_transactions AS (
    SELECT 
        City,
        Date,
        ROW_NUMBER() OVER (PARTITION BY City ORDER BY Date) AS transaction_number,
        DATEDIFF(DAY, MIN(Date) OVER (PARTITION BY City), Date) AS days_since_first_transaction
    FROM credit_card_transactions
)
SELECT TOP 1
    City,
    days_since_first_transaction AS days_to_500th_transaction
FROM city_transactions
WHERE transaction_number = 500
ORDER BY days_to_500th_transaction ASC;
