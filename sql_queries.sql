-- This query counts the number of taxi trips for each hour of the day to identify peak demand times.
SELECT
    strftime('%H', tpep_pickup_datetime) AS pickup_hour,
    COUNT(*) AS trip_count
FROM
    taxi_trips
WHERE
    tpep_pickup_datetime LIKE '2025-07%'
GROUP BY
    pickup_hour
ORDER BY
    trip_count DESC;
	

-- This query calculates the average trip duration for each day of the week to analyze traffic patterns.
SELECT
    CASE strftime('%w', tpep_pickup_datetime)
        WHEN '0' THEN 'Sunday'
        WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday'
    END AS day_of_week,
    AVG(trip_duration_minutes) AS avg_trip_duration
FROM
    taxi_trips
WHERE
    tpep_pickup_datetime LIKE '2025-07%'
GROUP BY
    day_of_week
ORDER BY
    avg_trip_duration DESC;

	
-- This query identifies the top 10 busiest taxi pickup locations by joining with the taxi_zones lookup table.
SELECT
    zones.Zone AS pickup_location,
    COUNT(*) AS total_trips
FROM
    taxi_trips AS trips
JOIN
    taxi_zones AS zones
    ON trips.PULocationID = zones.LocationID
WHERE
    trips.tpep_pickup_datetime LIKE '2025-07%'
GROUP BY
    pickup_location
ORDER BY
    total_trips DESC
LIMIT 10;


-- This query calculates the percentage of trips for each payment type to understand customer behavior.
SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
    END AS payment_method,
    COUNT(*) AS total_transactions,
    ROUND(CAST(COUNT(*) AS REAL) * 100 / (SELECT COUNT(*) FROM taxi_trips), 2) AS percentage
FROM
    taxi_trips
WHERE
    tpep_pickup_datetime LIKE '2025-07%'
GROUP BY
    payment_type
ORDER BY
    total_transactions DESC;
	
	
-- This query calculates a key business metric: the average fare amount divided by the average trip distance.
SELECT
    ROUND(AVG(fare_amount) / AVG(trip_distance), 2) AS avg_cost_per_mile
FROM
    taxi_trips
WHERE
    trip_distance > 0 AND fare_amount > 0;