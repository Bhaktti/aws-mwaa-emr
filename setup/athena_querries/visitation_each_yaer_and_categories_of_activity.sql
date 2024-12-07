WITH yearly_visits AS (
  SELECT
    ParkName,
    Year,
    SUM(CAST(RecreationVisits AS BIGINT)) AS TotalYearlyVisits
  FROM
    park_visits_by_year
  GROUP BY
    ParkName, Year
),
max_visits_per_park AS (
  SELECT
    ParkName,
    MAX(TotalYearlyVisits) AS MaxYearlyVisits
  FROM
    yearly_visits
  GROUP BY
    ParkName
),
most_visited_year AS (
  SELECT
    yv.ParkName,
    yv.Year,
    yv.TotalYearlyVisits
  FROM
    yearly_visits yv
  JOIN
    max_visits_per_park mv
  ON
    yv.ParkName = mv.ParkName AND yv.TotalYearlyVisits = mv.MaxYearlyVisits
)
SELECT
  mv.ParkName,
  mv.Year,
  mv.TotalYearlyVisits,
  pa.park_name,
  pa.id
FROM
  most_visited_year mv
JOIN
  parks_activity_data pa
ON
  mv.ParkName = pa.park_fullName
ORDER BY
  mv.TotalYearlyVisits DESC;