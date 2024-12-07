WITH yearly_visits AS (
  SELECT
    ParkName,
    Region,
    State,
    Year,
    SUM(CAST(RecreationVisits AS BIGINT)) AS TotalYearlyVisits
  FROM
    park_visits_by_year
  GROUP BY
    ParkName, Region, State, Year
),
monthly_visits AS (
  SELECT
    ParkName,
    UnitCode,
    ParkType,
    Region,
    State,
    Year,
    Month,
    SUM(CAST(RecreationVisits AS BIGINT)) AS TotalMonthlyVisits
  FROM
    park_visits_by_month
  GROUP BY
    ParkName, UnitCode, ParkType, Region, State, Year, Month
)
SELECT
  y.ParkName,
  y.Region,
  y.State,
  y.Year,
  y.TotalYearlyVisits,
  m.Month,
  m.TotalMonthlyVisits
FROM
  yearly_visits y
LEFT JOIN
  monthly_visits m
ON
  y.ParkName = m.ParkName AND y.Year = m.Year
ORDER BY
  y.ParkName, y.Year, m.Month;