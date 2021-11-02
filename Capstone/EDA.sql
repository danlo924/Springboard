/*FIND ALL COMPANIES AND RANGE OF DATES THAT HAVE PRICE DATA:*/
SELECT c.CompanyID
	,c.Name
    ,c.Ticker
    ,c.IsIndex
    ,c.Sector
    ,c.Industry
    ,c.Founded
    ,MIN(p.Date) AS MinDate
    ,MAX(p.Date) AS MaxDate
    ,COUNT(p.Date) AS PriceDays    
FROM companies c
	LEFT OUTER JOIN prices p ON c.CompanyID = p.CompanyID
GROUP BY c.CompanyID
ORDER BY c.IsIndex DESC
	,c.Name
