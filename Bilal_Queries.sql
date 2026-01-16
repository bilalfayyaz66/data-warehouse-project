-- ============================================================
-- TBWalmart Data Warehouse Analysis Queries
-- ============================================================
use tbwalmart;
-- Query 1: Total Revenue and Sales Count by Product Category
SELECT 
    p.Product_Category,
    COUNT(s.Sales_ID) AS Total_Sales,
    SUM(s.Quantity) AS Total_Quantity_Sold,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Average_Order_Value,
    MIN(s.Revenue) AS Min_Order_Value,
    MAX(s.Revenue) AS Max_Order_Value
FROM Sales_Fact s
JOIN Product_Dim p ON s.Product_ID = p.Product_ID
GROUP BY p.Product_Category
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 2: Customer Demographics Analysis (Gender and Age Distribution)
SELECT 
    c.Gender,
    c.Age_Range,
    COUNT(DISTINCT c.Customer_ID) AS Total_Customers,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value,
    SUM(s.Quantity) AS Total_Items_Purchased
FROM Customer_Dim c
LEFT JOIN Sales_Fact s ON c.Customer_ID = s.Customer_ID
GROUP BY c.Gender, c.Age_Range
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 3: Monthly Sales Trend Analysis
SELECT 
    d.Year,
    d.Month,
    d.Month_Name,
    COUNT(s.Sales_ID) AS Monthly_Orders,
    SUM(s.Quantity) AS Monthly_Quantity,
    SUM(s.Revenue) AS Monthly_Revenue,
    AVG(s.Revenue) AS Avg_Monthly_Order_Value
FROM Sales_Fact s
JOIN Date_Dim d ON s.Date_ID = d.Date_ID
GROUP BY d.Year, d.Month, d.Month_Name
ORDER BY d.Year, d.Month;

-- ============================================================

-- Query 4: Top 10 Customers by Revenue
SELECT 
    c.Customer_ID,
    c.Gender,
    c.Age_Range,
    c.City_Category,
    c.Occupation,
    c.Marital_Status,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Items,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value
FROM Customer_Dim c
JOIN Sales_Fact s ON c.Customer_ID = s.Customer_ID
GROUP BY c.Customer_ID, c.Gender, c.Age_Range, c.City_Category, c.Occupation, c.Marital_Status
ORDER BY Total_Revenue DESC
LIMIT 10;

-- ============================================================

-- Query 5: Store Performance Comparison
SELECT 
    st.Store_ID,
    st.Store_Name,
    COUNT(s.Sales_ID) AS Total_Sales,
    COUNT(DISTINCT s.Customer_ID) AS Unique_Customers,
    SUM(s.Quantity) AS Total_Quantity_Sold,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Transaction_Value,
    SUM(s.Revenue) / COUNT(DISTINCT s.Customer_ID) AS Revenue_Per_Customer
FROM Store_Dim st
JOIN Sales_Fact s ON st.Store_ID = s.Store_ID
GROUP BY st.Store_ID, st.Store_Name
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 6: Supplier Performance Analysis
SELECT 
    sup.Supplier_ID,
    sup.Supplier_Name,
    COUNT(DISTINCT s.Product_ID) AS Products_Supplied,
    COUNT(s.Sales_ID) AS Total_Sales,
    SUM(s.Quantity) AS Total_Quantity,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Sale_Value
FROM Supplier_Dim sup
JOIN Sales_Fact s ON sup.Supplier_ID = s.Supplier_ID
GROUP BY sup.Supplier_ID, sup.Supplier_Name
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 7: Seasonal Sales Analysis
SELECT 
    d.Season,
    d.Year,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Quantity,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value
FROM Sales_Fact s
JOIN Date_Dim d ON s.Date_ID = d.Date_ID
GROUP BY d.Season, d.Year
ORDER BY d.Year, 
    CASE d.Season 
        WHEN 'Spring' THEN 1 
        WHEN 'Summer' THEN 2 
        WHEN 'Fall' THEN 3 
        WHEN 'Winter' THEN 4 
    END;

-- ============================================================

-- Query 8: Weekday vs Weekend Sales Comparison
SELECT 
    d.Weekday_Weekend,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Quantity,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value,
    SUM(s.Revenue) / COUNT(s.Sales_ID) AS Revenue_Per_Order
FROM Sales_Fact s
JOIN Date_Dim d ON s.Date_ID = d.Date_ID
GROUP BY d.Weekday_Weekend
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 9: Top 10 Best-Selling Products
SELECT 
    p.Product_ID,
    p.Product_Category,
    p.Price,
    st.Store_Name,
    sup.Supplier_Name,
    COUNT(s.Sales_ID) AS Times_Sold,
    SUM(s.Quantity) AS Total_Quantity_Sold,
    SUM(s.Revenue) AS Total_Revenue
FROM Product_Dim p
JOIN Sales_Fact s ON p.Product_ID = s.Product_ID
JOIN Store_Dim st ON p.Store_ID = st.Store_ID
JOIN Supplier_Dim sup ON p.Supplier_ID = sup.Supplier_ID
GROUP BY p.Product_ID, p.Product_Category, p.Price, st.Store_Name, sup.Supplier_Name
ORDER BY Total_Quantity_Sold DESC
LIMIT 10;

-- ============================================================

-- Query 10: Customer Segmentation by City Category
SELECT 
    c.City_Category,
    COUNT(DISTINCT c.Customer_ID) AS Total_Customers,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value,
    SUM(s.Revenue) / COUNT(DISTINCT c.Customer_ID) AS Revenue_Per_Customer
FROM Customer_Dim c
LEFT JOIN Sales_Fact s ON c.Customer_ID = s.Customer_ID
GROUP BY c.City_Category
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 11: Quarterly Sales Performance
SELECT 
    d.Year,
    d.Quarter,
    COUNT(s.Sales_ID) AS Quarterly_Orders,
    SUM(s.Quantity) AS Quarterly_Quantity,
    SUM(s.Revenue) AS Quarterly_Revenue,
    AVG(s.Revenue) AS Avg_Quarterly_Order_Value
FROM Sales_Fact s
JOIN Date_Dim d ON s.Date_ID = d.Date_ID
GROUP BY d.Year, d.Quarter
ORDER BY d.Year, d.Quarter;

-- ============================================================

-- Query 12: Marital Status Impact on Purchase Behavior
SELECT 
    c.Marital_Status,
    CASE 
        WHEN c.Marital_Status = 0 THEN 'Single'
        WHEN c.Marital_Status = 1 THEN 'Married'
        ELSE 'Unknown'
    END AS Marital_Status_Label,
    COUNT(DISTINCT c.Customer_ID) AS Total_Customers,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value,
    AVG(s.Quantity) AS Avg_Items_Per_Order
FROM Customer_Dim c
JOIN Sales_Fact s ON c.Customer_ID = s.Customer_ID
GROUP BY c.Marital_Status
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 13: Product Category Performance by Store
SELECT 
    st.Store_Name,
    p.Product_Category,
    COUNT(s.Sales_ID) AS Total_Sales,
    SUM(s.Quantity) AS Total_Quantity,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Sale_Value
FROM Sales_Fact s
JOIN Product_Dim p ON s.Product_ID = p.Product_ID
JOIN Store_Dim st ON s.Store_ID = st.Store_ID
GROUP BY st.Store_Name, p.Product_Category
ORDER BY st.Store_Name, Total_Revenue DESC;

-- ============================================================

-- Query 14: Customer Retention Analysis (Years in City)
SELECT 
    c.Stay_In_Current_City_Years,
    COUNT(DISTINCT c.Customer_ID) AS Total_Customers,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value,
    SUM(s.Revenue) / COUNT(DISTINCT c.Customer_ID) AS Revenue_Per_Customer
FROM Customer_Dim c
LEFT JOIN Sales_Fact s ON c.Customer_ID = s.Customer_ID
GROUP BY c.Stay_In_Current_City_Years
ORDER BY c.Stay_In_Current_City_Years;

-- ============================================================

-- Query 15: Year-over-Year Revenue Growth Analysis
SELECT 
    d.Year,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Revenue) AS Annual_Revenue,
    LAG(SUM(s.Revenue)) OVER (ORDER BY d.Year) AS Previous_Year_Revenue,
    SUM(s.Revenue) - LAG(SUM(s.Revenue)) OVER (ORDER BY d.Year) AS Revenue_Change,
    ROUND(((SUM(s.Revenue) - LAG(SUM(s.Revenue)) OVER (ORDER BY d.Year)) / 
           LAG(SUM(s.Revenue)) OVER (ORDER BY d.Year)) * 100, 2) AS Growth_Percentage
FROM Sales_Fact s
JOIN Date_Dim d ON s.Date_ID = d.Date_ID
GROUP BY d.Year
ORDER BY d.Year;

-- ============================================================

-- Query 16: Top Product Categories by Customer Age Group
SELECT 
    Age_Range,
    Product_Category,
    Purchase_Count,
    Total_Quantity,
    Total_Revenue,
    Category_Rank
FROM (
    SELECT 
        c.Age_Range,
        p.Product_Category,
        COUNT(s.Sales_ID) AS Purchase_Count,
        SUM(s.Quantity) AS Total_Quantity,
        SUM(s.Revenue) AS Total_Revenue,
        RANK() OVER (PARTITION BY c.Age_Range ORDER BY SUM(s.Revenue) DESC) AS Category_Rank
    FROM Sales_Fact s
    JOIN Customer_Dim c ON s.Customer_ID = c.Customer_ID
    JOIN Product_Dim p ON s.Product_ID = p.Product_ID
    GROUP BY c.Age_Range, p.Product_Category
) AS ranked_categories
WHERE Category_Rank <= 5
ORDER BY Age_Range, Category_Rank;

-- ============================================================

-- Query 17: Store and Supplier Collaboration Analysis
SELECT 
    st.Store_Name,
    sup.Supplier_Name,
    COUNT(DISTINCT p.Product_ID) AS Products_Count,
    COUNT(s.Sales_ID) AS Total_Sales,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Sale_Value
FROM Sales_Fact s
JOIN Store_Dim st ON s.Store_ID = st.Store_ID
JOIN Supplier_Dim sup ON s.Supplier_ID = sup.Supplier_ID
JOIN Product_Dim p ON s.Product_ID = p.Product_ID
GROUP BY st.Store_Name, sup.Supplier_Name
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 18: Occupation-Based Purchasing Patterns
SELECT 
    c.Occupation,
    COUNT(DISTINCT c.Customer_ID) AS Total_Customers,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Items,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value,
    SUM(s.Revenue) / COUNT(DISTINCT c.Customer_ID) AS Revenue_Per_Customer
FROM Customer_Dim c
JOIN Sales_Fact s ON c.Customer_ID = s.Customer_ID
GROUP BY c.Occupation
ORDER BY Total_Revenue DESC;

-- ============================================================

-- Query 19: Daily Sales Pattern Analysis (Day of Month)
SELECT 
    d.Day AS Day_Of_Month,
    COUNT(s.Sales_ID) AS Total_Orders,
    SUM(s.Quantity) AS Total_Quantity,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value
FROM Sales_Fact s
JOIN Date_Dim d ON s.Date_ID = d.Date_ID
GROUP BY d.Day
ORDER BY d.Day;

-- ============================================================

-- Query 20: Comprehensive Business Performance Dashboard
SELECT 
    d.Year,
    d.Month_Name,
    COUNT(DISTINCT s.Customer_ID) AS Active_Customers,
    COUNT(s.Sales_ID) AS Total_Orders,
    COUNT(DISTINCT s.Product_ID) AS Products_Sold,
    COUNT(DISTINCT s.Store_ID) AS Active_Stores,
    SUM(s.Quantity) AS Total_Items_Sold,
    SUM(s.Revenue) AS Total_Revenue,
    AVG(s.Revenue) AS Avg_Order_Value,
    MIN(s.Revenue) AS Min_Order_Value,
    MAX(s.Revenue) AS Max_Order_Value,
    ROUND(SUM(s.Revenue) / COUNT(DISTINCT s.Customer_ID), 2) AS Revenue_Per_Customer
FROM Sales_Fact s
JOIN Date_Dim d ON s.Date_ID = d.Date_ID
GROUP BY d.Year, d.Month_Name, d.Month
ORDER BY d.Year, d.Month;

-- ============================================================