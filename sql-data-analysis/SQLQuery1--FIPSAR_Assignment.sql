-- connection tests --
USE AdventureWorksDW2025;
GO

SELECT name
FROM sys.tables
ORDER BY name;

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'FactFinance';

-- assignment task queries --


-- ============================================================
-- SECTION 1: FactFinance
-- ============================================================

-- Q1: 
SELECT
    o.PercentageOfOwnership,
    COUNT(*)       AS RecordCount,
    SUM(f.Amount)  AS TotalAmount,
    AVG(f.Amount)  AS AvgAmount
FROM FactFinance f
JOIN DimOrganization o ON f.OrganizationKey = o.OrganizationKey
GROUP BY o.PercentageOfOwnership
ORDER BY o.PercentageOfOwnership;

-- Q2a: 
SELECT
    d.CalendarYear,
    d.CalendarQuarter,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.CalendarYear, d.CalendarQuarter
ORDER BY d.CalendarYear, d.CalendarQuarter;

-- Q2b: 
SELECT
    d.CalendarYear,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.CalendarYear, d.MonthNumberOfYear, d.EnglishMonthName
ORDER BY d.CalendarYear, d.MonthNumberOfYear;

-- Q2c: 
SELECT
    d.CalendarYear,
    d.WeekNumberOfYear,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.CalendarYear, d.WeekNumberOfYear
ORDER BY d.CalendarYear, d.WeekNumberOfYear;

-- Q3
SELECT TOP 5
    c.CurrencyName,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimOrganization o
    ON f.OrganizationKey = o.OrganizationKey
JOIN DimCurrency c
    ON o.CurrencyKey = c.CurrencyKey
GROUP BY c.CurrencyName
ORDER BY TotalAmount DESC;

-- Q4a
SELECT
    o.OrganizationName,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimOrganization o ON f.OrganizationKey = o.OrganizationKey
GROUP BY o.OrganizationName
ORDER BY TotalAmount DESC;
-- Q4b
SELECT
    dg.DepartmentGroupName,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimDepartmentGroup dg ON f.DepartmentGroupKey = dg.DepartmentGroupKey
GROUP BY dg.DepartmentGroupName
ORDER BY TotalAmount DESC;
-- Q4c
SELECT
    s.ScenarioName,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimScenario s ON f.ScenarioKey = s.ScenarioKey
GROUP BY s.ScenarioName
ORDER BY TotalAmount DESC;
-- Q4d
SELECT
    a.AccountDescription,
    a.AccountType,
    SUM(f.Amount) AS TotalAmount
FROM FactFinance f
JOIN DimAccount a ON f.AccountKey = a.AccountKey
GROUP BY a.AccountDescription, a.AccountType
ORDER BY TotalAmount DESC;


-- Q5
SELECT
    SUM(Calls)          AS TotalCalls,
    SUM(IssuesRaised)   AS TotalIssuesRaised,
    SUM(TotalOperators) AS TotalOperators
FROM FactCallCenter;
-- Q6
SELECT
    d.FullDateAlternateKey AS CallDate,
    f.Shift,
    SUM(f.Calls)           AS TotalCalls
FROM FactCallCenter f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.FullDateAlternateKey, f.Shift
ORDER BY d.FullDateAlternateKey DESC, f.Shift;
-- Q7
SELECT
    d.CalendarYear,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    f.WageType                         AS DayType,
    SUM(f.Calls)                       AS TotalCalls,
    AVG(f.AverageTimePerIssue)         AS AvgTimePerIssue
FROM FactCallCenter f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY
    d.CalendarYear,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    f.WageType
ORDER BY d.CalendarYear, d.MonthNumberOfYear, f.WageType;


-- Q8
SELECT
    c.Gender,
    COUNT(*) AS NumberOfResponses
FROM FactSurveyResponse f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY c.Gender
ORDER BY c.Gender;
-- Q9
SELECT
    d.CalendarYear          AS FirstPurchaseYear,
    d.MonthNumberOfYear     AS FirstPurchaseMonth,
    d.EnglishMonthName      AS MonthName,
    COUNT(DISTINCT c.CustomerKey) AS NumberOfCustomers
FROM DimCustomer c
JOIN DimDate d
    ON d.FullDateAlternateKey = CAST(c.DateFirstPurchase AS DATE)
GROUP BY d.CalendarYear, d.MonthNumberOfYear, d.EnglishMonthName
ORDER BY d.CalendarYear, d.MonthNumberOfYear;
-- Q10
SELECT
    d.CalendarYear,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    c.Gender,
    c.MaritalStatus,
    COUNT(DISTINCT f.ProductCategoryKey) AS NumberOfProducts
FROM FactSurveyResponse f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
JOIN DimDate d     ON f.DateKey = d.DateKey
GROUP BY
    d.CalendarYear,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    c.Gender,
    c.MaritalStatus
ORDER BY d.CalendarYear, d.MonthNumberOfYear, c.Gender, c.MaritalStatus;
-- Q11
SELECT
    CASE
        WHEN DATEDIFF(YEAR, c.BirthDate, GETDATE()) < 16  THEN '<16'
        WHEN DATEDIFF(YEAR, c.BirthDate, GETDATE()) <= 25 THEN '16-25'
        WHEN DATEDIFF(YEAR, c.BirthDate, GETDATE()) <= 40 THEN '26-40'
        ELSE '>40'
    END AS AgeRange,
    COUNT(DISTINCT f.ProductCategoryKey) AS NumberOfProducts
FROM FactSurveyResponse f
JOIN DimCustomer c
    ON f.CustomerKey = c.CustomerKey
GROUP BY
    CASE
        WHEN DATEDIFF(YEAR, c.BirthDate, GETDATE()) < 16  THEN '<16'
        WHEN DATEDIFF(YEAR, c.BirthDate, GETDATE()) <= 25 THEN '16-25'
        WHEN DATEDIFF(YEAR, c.BirthDate, GETDATE()) <= 40 THEN '26-40'
        ELSE '>40'
    END
ORDER BY AgeRange;


-- Q12
DECLARE @SelectedDate DATE = (
    SELECT MAX(MovementDate) FROM FactProductInventory
);

SELECT
    p.EnglishProductName,
    p.ListPrice,
    SUM(f.UnitsBalance)                  AS TotalUnitsBalance,
    SUM(f.UnitsBalance * f.UnitCost)     AS UnitCostValueOfUnitsBalance
FROM FactProductInventory f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
WHERE CAST(f.MovementDate AS DATE) = @SelectedDate
GROUP BY p.EnglishProductName, p.ListPrice
ORDER BY TotalUnitsBalance DESC;

-- Q13
DECLARE @ThresholdPct FLOAT = 80.0; 

SELECT
    p.EnglishProductName,
    p.SafetyStockLevel,
    SUM(f.UnitsBalance)                                               AS CurrentUnitsBalance,
    CAST(SUM(f.UnitsBalance) * 100.0
         / NULLIF(p.SafetyStockLevel, 0) AS DECIMAL(10,2))           AS PctOfSafetyStock,
    CASE
        WHEN SUM(f.UnitsBalance) * 100.0
             / NULLIF(p.SafetyStockLevel, 0) >= 100 THEN 'EXCEEDS Stock Level'
        ELSE 'Nearing Stock Level'
    END                                                               AS StockStatus
FROM FactProductInventory f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.EnglishProductName, p.SafetyStockLevel
HAVING SUM(f.UnitsBalance) * 100.0 / NULLIF(p.SafetyStockLevel, 0) >= @ThresholdPct
ORDER BY PctOfSafetyStock DESC;

-- Q14
SELECT TOP 5
    p.EnglishProductName,
    p.StandardCost,
    p.ListPrice,
    p.DealerPrice,
    SUM(f.UnitsBalance) AS TotalUnitsBalance
FROM FactProductInventory f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
WHERE p.FinishedGoodsFlag = 1
GROUP BY
    p.EnglishProductName,
    p.StandardCost,
    p.ListPrice,
    p.DealerPrice
ORDER BY TotalUnitsBalance DESC;
-- Q15
SELECT TOP 10
    p.EnglishProductName,
    SUM(f.UnitsOut) AS TotalUnitsOut
FROM FactProductInventory f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.EnglishProductName
ORDER BY TotalUnitsOut ASC;


-- ============================================================
-- SECTION 5: FactInternetSales
-- ============================================================

-- Q16a: 
SELECT
    d.CalendarYear,
    SUM(f.SalesAmount) AS TotalSalesAmount
FROM FactInternetSales f
JOIN DimDate d ON f.OrderDateKey = d.DateKey
GROUP BY d.CalendarYear
ORDER BY d.CalendarYear;

-- Q16b: 
SELECT
    d.CalendarYear,
    d.CalendarQuarter,
    SUM(f.SalesAmount) AS TotalSalesAmount
FROM FactInternetSales f
JOIN DimDate d ON f.OrderDateKey = d.DateKey
GROUP BY d.CalendarYear, d.CalendarQuarter
ORDER BY d.CalendarYear, d.CalendarQuarter;

-- Q16c: 
SELECT
    d.CalendarYear,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    SUM(f.SalesAmount) AS TotalSalesAmount
FROM FactInternetSales f
JOIN DimDate d ON f.OrderDateKey = d.DateKey
GROUP BY d.CalendarYear, d.MonthNumberOfYear, d.EnglishMonthName
ORDER BY d.CalendarYear, d.MonthNumberOfYear;

-- Q17: 
SELECT TOP 10
    p.EnglishProductName,
    SUM(f.SalesAmount) AS TotalSalesAmount
FROM FactInternetSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.EnglishProductName
ORDER BY TotalSalesAmount DESC;

-- Q18: 
DECLARE @TerritoryGroup NVARCHAR(50) = NULL; 

SELECT
    t.SalesTerritoryGroup,
    t.SalesTerritoryCountry,
    t.SalesTerritoryRegion,
    SUM(f.SalesAmount) AS TotalSalesAmount
FROM FactInternetSales f
JOIN DimSalesTerritory t ON f.SalesTerritoryKey = t.SalesTerritoryKey
WHERE (@TerritoryGroup IS NULL OR t.SalesTerritoryGroup = @TerritoryGroup)
GROUP BY
    t.SalesTerritoryGroup,
    t.SalesTerritoryCountry,
    t.SalesTerritoryRegion
ORDER BY TotalSalesAmount DESC;

-- Q19
SELECT
    f.SalesOrderNumber,
    f.SalesOrderLineNumber,
    f.SalesAmount,
    f.TaxAmt,
    f.Freight,
    (f.SalesAmount - f.TaxAmt - f.Freight) AS NetAmount
FROM FactInternetSales f
ORDER BY f.SalesOrderNumber;
-- Q20
SELECT
    f.SalesOrderNumber,
    f.OrderDate,
    f.ShipDate,
    DATEDIFF(DAY, f.OrderDate, f.ShipDate) AS DaysToShip
FROM FactInternetSales f
ORDER BY DaysToShip DESC;


-- ============================================================
-- SECTION 6: FactSalesQuota
-- ============================================================

-- Q21a: 
DECLARE @SelectedYear    INT = NULL;  
DECLARE @SelectedQuarter INT = NULL;  

-- Year level
SELECT
    fsq.CalendarYear,
    SUM(fsq.SalesAmountQuota) AS TotalSalesQuota
FROM FactSalesQuota fsq
WHERE @SelectedYear IS NULL AND @SelectedQuarter IS NULL
GROUP BY fsq.CalendarYear
ORDER BY fsq.CalendarYear;
-- Q21b
SELECT
    fsq.CalendarYear,
    fsq.CalendarQuarter,
    SUM(fsq.SalesAmountQuota) AS TotalSalesQuota
FROM FactSalesQuota fsq
WHERE fsq.CalendarYear = @SelectedYear
  AND @SelectedYear IS NOT NULL
  AND @SelectedQuarter IS NULL
GROUP BY fsq.CalendarYear, fsq.CalendarQuarter
ORDER BY fsq.CalendarYear, fsq.CalendarQuarter;
-- Q21c
SELECT
    fsq.CalendarYear,
    fsq.CalendarQuarter,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    SUM(fsq.SalesAmountQuota) AS TotalSalesQuota
FROM FactSalesQuota fsq
JOIN DimDate d ON fsq.DateKey = d.DateKey
WHERE fsq.CalendarYear    = @SelectedYear
  AND fsq.CalendarQuarter = @SelectedQuarter
  AND @SelectedYear IS NOT NULL
  AND @SelectedQuarter IS NOT NULL
GROUP BY fsq.CalendarYear, fsq.CalendarQuarter, d.MonthNumberOfYear, d.EnglishMonthName
ORDER BY fsq.CalendarYear, fsq.CalendarQuarter, d.MonthNumberOfYear;
-- Q22a
SELECT
    e.DepartmentName,
    SUM(fsq.SalesAmountQuota) AS TotalSalesQuota
FROM FactSalesQuota fsq
JOIN DimEmployee e ON fsq.EmployeeKey = e.EmployeeKey
GROUP BY e.DepartmentName
ORDER BY TotalSalesQuota DESC;
-- Q22b
SELECT
    e.FirstName,
    e.LastName,
    e.Title,
    SUM(fsq.SalesAmountQuota) AS TotalSalesQuota
FROM FactSalesQuota fsq
JOIN DimEmployee e ON fsq.EmployeeKey = e.EmployeeKey
GROUP BY e.FirstName, e.LastName, e.Title
ORDER BY TotalSalesQuota DESC;
-- Q23
SELECT
    mgr.FirstName + ' ' + mgr.LastName   AS ParentEmployee,
    emp.FirstName + ' ' + emp.LastName   AS ChildEmployee,
    fsq.CalendarYear,
    fsq.CalendarQuarter,
    d.MonthNumberOfYear,
    d.EnglishMonthName,
    SUM(fsq.SalesAmountQuota)            AS TotalSalesQuota
FROM FactSalesQuota fsq
JOIN DimEmployee emp ON fsq.EmployeeKey        = emp.EmployeeKey
JOIN DimEmployee mgr ON emp.ParentEmployeeKey  = mgr.EmployeeKey
JOIN DimDate d       ON fsq.DateKey            = d.DateKey
GROUP BY
    mgr.FirstName, mgr.LastName,
    emp.FirstName, emp.LastName,
    fsq.CalendarYear,
    fsq.CalendarQuarter,
    d.MonthNumberOfYear,
    d.EnglishMonthName
ORDER BY
    ParentEmployee,
    ChildEmployee,
    fsq.CalendarYear,
    fsq.CalendarQuarter,
    d.MonthNumberOfYear;
-- Q24
SELECT
    e.FirstName,
    e.LastName,
    CASE e.Gender
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'Unknown'
    END                              AS Gender,
    e.HireDate,
    e.DepartmentName                 AS Department,
    t.SalesTerritoryRegion,
    e.VacationHours,
    CAST(e.VacationHours * e.BaseRate AS DECIMAL(10,2)) AS VacationValueAmount
FROM DimEmployee e
LEFT JOIN DimSalesTerritory t ON e.SalesTerritoryKey = t.SalesTerritoryKey
ORDER BY e.LastName, e.FirstName;
-- Q25
SELECT
    CASE
        WHEN DATEDIFF(YEAR, e.BirthDate, GETDATE()) <= 30 THEN '<=30'
        WHEN DATEDIFF(YEAR, e.BirthDate, GETDATE()) <= 40 THEN '31-40'
        WHEN DATEDIFF(YEAR, e.BirthDate, GETDATE()) <= 50 THEN '41-50'
        ELSE '>50'
    END AS AgeRange,
    SUM(fsq.SalesAmountQuota) AS TotalSalesQuota
FROM FactSalesQuota fsq
JOIN DimEmployee e
    ON fsq.EmployeeKey = e.EmployeeKey
GROUP BY
    CASE
        WHEN DATEDIFF(YEAR, e.BirthDate, GETDATE()) <= 30 THEN '<=30'
        WHEN DATEDIFF(YEAR, e.BirthDate, GETDATE()) <= 40 THEN '31-40'
        WHEN DATEDIFF(YEAR, e.BirthDate, GETDATE()) <= 50 THEN '41-50'
        ELSE '>50'
    END
ORDER BY AgeRange;