import sqlalchemy
from sqlalchemy import create_engine
import quaries


class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file, which remains active
    until the object is destroyed.
    """

    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)

    def _execute_query(self, query, params):
        """
        Execute an SQL query with the params provided in a dictionary, and returns a list of records (dictionary-like
        objects).
        If an exception was raised, prints the error and returns an empty list.
        """
        try:
            with self._engine.connect() as connection:
                query = sqlalchemy.text(query)
                result = connection.execute(query, params)
                column_names = result.keys()
                # creating list of dict where each key is name of a column
                rows = [dict(zip(column_names, row)) for row in result.fetchall()]
            return rows

        except Exception as exc_e:
            print(exc_e)
            return []

    def get_delayed_flights_by_airport(self, airport_code: str) -> list:
        """
        Returns list of dictionary where each dictionary is a delayed flight from a specific airport
        """
        params = {'airport': airport_code}
        return self._execute_query(quaries.QUERY_FLIGHT_BY_ID, params)

    def get_delayed_flights_by_airline(self, airline: str) -> list:
        """
        Searches for delayed flights by specific airline name.
        If the name matches, it returns a list of delayed flights by that airline
        """
        params = {'airline': airline}
        return self._execute_query(quaries.QUERY_DELAYED_FLIGHTS_BY_AIRLINE, params)

    def get_flights_by_date(self, day, month, year) -> list:
        """
        Searches for flight details by date.
        If the date is in our database, it returns a list of flights for that day
        """
        params = {'day': day, 'month': month, 'year': year}
        return self._execute_query(quaries.QUERY_FLIGHTS_BY_DATE, params)

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, it returns a list with a single record
        """
        params = {'id': flight_id}
        return self._execute_query(quaries.QUERY_FLIGHT_BY_ID, params)

    def __del__(self):
        """
        Closes the connection to the database when the object is about to be destroyed
        """
        self._engine.dispose()
