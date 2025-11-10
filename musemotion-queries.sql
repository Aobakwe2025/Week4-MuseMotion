CREATE TABLE musemotion_db (
  id INT AUTO_INCREMENT PRIMARY KEY,
  VIN VARCHAR(100),
  City VARCHAR(100),
  Year INT,
  Make VARCHAR(100),
  Model VARCHAR(100),
  Vehicle_Type VARCHAR(255),
  Eligibility VARCHAR(255),
  Electric_Range DOUBLE,
  Base_MSRP DOUBLE,
  Latitude DOUBLE,
  Longitude DOUBLE
  Utility VARCHAR(255),
);

USE musemotion_db;


-- READ
SELECT `vin`, `city`, `year`, `make`, `model`, `vehicle_type`, `eligibility_reason`, `odometer`, `some_id`, `geom_wkt`, `utility`
FROM `musemotion`
LIMIT 10;

-- FILTER + SORT
SELECT `vin`, `city`, `year`, `make`, `model`
FROM `musemotion`
WHERE `year` IS NOT NULL
ORDER BY `year` DESC, `city` ASC
LIMIT 100;

-- TRANSFORMATION example: compute vehicle_age (assuming column `year` is year)
SELECT `vin`, `year` AS year,
       (YEAR(CURDATE()) - CAST(`year` AS SIGNED)) AS vehicle_age
FROM `musemotion`
WHERE `year` IS NOT NULL
ORDER BY vehicle_age ASC
LIMIT 50;

-- AGGREGATION: COUNT by make
SELECT `make` AS make, COUNT(*) AS cnt
FROM `musemotion`
GROUP BY `make`
ORDER BY cnt DESC
LIMIT 20;

-- AGGREGATION with HAVING: average odometer per make (requires numeric odometer column `odometer`)
SELECT `make` AS make, COUNT(*) AS cnt, AVG(CAST(`odometer` AS DECIMAL)) AS avg_odometer
FROM `musemotion`
GROUP BY `make`
HAVING COUNT(*) > 10
ORDER BY avg_odometer DESC;

-- CRUD examples:
-- CREATE
INSERT INTO `musemotion` (`vin`, `city`, `year`, `make`, `model`, `vehicle_type`, `eligibility_reason`, `odometer`, `some_id`, `geom_wkt`, `utility`)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- READ by vin
SELECT * FROM `musemotion` WHERE `vin` = 'SAMPLEVIN123' LIMIT 1;

-- UPDATE
UPDATE `musemotion` SET `city` = 'NewCity' WHERE `vin` = 'SAMPLEVIN123';

-- DELETE
DELETE FROM `musemotion` WHERE `vin` = 'SAMPLEVIN123';

-- SUBQUERY example
SELECT t.* FROM `musemotion` t
WHERE CAST(t.`odometer` AS DECIMAL) > (
    SELECT AVG(CAST(x.`odometer` AS DECIMAL)) FROM `musemotion` x WHERE x.`odometer` IS NOT NULL
)
ORDER BY CAST(t.`odometer` AS DECIMAL) DESC
LIMIT 50;

-- CTE example
WITH top_makes AS (
  SELECT `make` AS make, COUNT(*) AS cnt
  FROM `musemotion`
  GROUP BY `make`
  ORDER BY cnt DESC
  LIMIT 5
)
SELECT m.*
FROM `musemotion` m
JOIN top_makes t ON m.`make` = t.make
ORDER BY t.cnt DESC, m.`year` DESC
LIMIT 200;

-- JOIN templates (requires utilities table)
CREATE TABLE IF NOT EXISTS utilities (
  utility_id INT AUTO_INCREMENT PRIMARY KEY,
  utility_name VARCHAR(255) UNIQUE,
  region VARCHAR(100)
);

-- INNER JOIN
SELECT v.`vin`, v.`city`, v.`make` AS make, u.region
FROM `musemotion` v
INNER JOIN utilities u ON v.`utility` = u.utility_name
LIMIT 50;

-- LEFT JOIN
SELECT v.`vin`, v.`city`, v.`make` AS make, u.region
FROM `musemotion` v
LEFT JOIN utilities u ON v.`utility` = u.utility_name
ORDER BY v.`year` DESC
LIMIT 100;

-- RIGHT JOIN
SELECT u.utility_name, u.region, v.`vin`, v.`make` AS make
FROM `musemotion` v
RIGHT JOIN utilities u ON v.`utility` = u.utility_name;

