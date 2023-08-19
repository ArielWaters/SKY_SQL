QUERY_FLIGHT_BY_ID = """
SELECT 
    flights.*,
    airlines.airline,
    flights.id as FLIGHT_ID,
    flights.departure_delay as DELAY
FROM flights JOIN airlines ON flights.airline = airlines.id 
WHERE flights.id = :id;
"""

QUERY_FLIGHTS_BY_DATE = """
SELECT
    flights.*,
    airlines.airline,
    flights.id as FLIGHT_ID,
    flights.DEPARTURE_DELAY as DELAY
FROM flights JOIN airlines ON flights.airline = airlines.id 
WHERE flights.day = :day AND flights.month = :month AND flights.year = :year;
"""

QUERY_DELAYED_FLIGHTS_BY_AIRLINE = """
SELECT 
    flights.*,
    airlines.airline,
    flights.id as FLIGHT_ID,
    flights.departure_delay as DELAY
FROM flights JOIN airlines ON flights.airline = airlines.id 
WHERE LOWER(airlines.airline) = LOWER(:airline) AND DELAY > 0 AND DELAY <> '';
"""

QUERY_DELAYED_FLIGHTS_BY_AIRPORT = """
SELECT 
    flights.*, 
    airlines.airline, 
    flights.ID as FLIGHT_ID, 
    flights.DEPARTURE_DELAY as DELAY 
FROM flights JOIN airlines ON flights.airline = airlines.id 
WHERE LOWER(flights.origin_airport) = LOWER(:airport) 
    AND DELAY > 0 AND DELAY <> ''
"""