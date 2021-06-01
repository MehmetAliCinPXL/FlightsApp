package be.pxl.paj.flights;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Allows clients to query and update the database in order to log in, search
 * for flights, reserve seats, show reservations, and cancel reservations.
 */
public class FlightsDB {

	/**
	 * Maximum number of reservations to allow on one flight.
	 */
	private static int MAX_FLIGHT_BOOKINGS = 3;
	private static final Logger LOGGER = LogManager.getLogger(FlightsDB.class);
	/**
	 * Holds the connection to the database.
	 **/
	private Connection conn;
	private Scanner scanner = new Scanner(System.in);
	private PreparedStatement selectCustomer;
	private PreparedStatement directFlights;
	private PreparedStatement twoHopFlights;
	private PreparedStatement countUserReservations;
	private PreparedStatement countReservationsForFlight;
	private PreparedStatement insertReservation;
	private PreparedStatement selectReservationsOfUser;
	private PreparedStatement removeReservationsOfUser;

	/**
	 * Opens a connection to the database using the given settings.
	 */
	public void open(Properties settings) throws Exception {
		// Make sure the JDBC driver is loaded.
		// Open a connection to our database.
		conn = DriverManager.getConnection(
				settings.getProperty("flightservice.url"),
				settings.getProperty("flightservice.username"),
				settings.getProperty("flightservice.password"));
	}

	/**
	 * Closes the connection to the database.
	 */
	public void close() throws SQLException {
		conn.close();
		conn = null;
	}

	/**
	 * Performs additional preparation after the connection is opened.
	 */
	public void init() throws SQLException {
		// TODO: create prepared statements here
		selectCustomer = conn.prepareStatement("SELECT * FROM CUSTOMER WHERE handle = ? AND password = ?");
		directFlights = conn.prepareStatement("SELECT fid, name, flight_num, origin_city, dest_city, actual_time" +
				" FROM FLIGHTS " +
				" INNER JOIN CARRIERS c ON carrier_id = c.cid " +
				"WHERE actual_time IS NOT NULL AND " +
				"    year = ? AND month_id = ? AND day_of_month = ? AND " +
				"    origin_city = ? AND dest_city = ? " +
				"ORDER BY actual_time ASC LIMIT 99");
		twoHopFlights = conn.prepareStatement("SELECT F1.fid as fid1, C1.name as name1, " +
				"    F1.flight_num as flight_num1, F1.origin_city as origin_city1, " +
				"    F1.dest_city as dest_city1, F1.actual_time as actual_time1, " +
				"    F2.fid as fid2, C2.name as name2, " +
				"    F2.flight_num as flight_num2, F2.origin_city as origin_city2, " +
				"    F2.dest_city as dest_city2, F2.actual_time as actual_time2 " +
				"FROM FLIGHTS F1, FLIGHTS F2, CARRIERS C1, CARRIERS C2 " +
				"WHERE F1.carrier_id = C1.cid AND F1.actual_time IS NOT NULL AND " +
				"    F2.carrier_id = C2.cid AND F2.actual_time IS NOT NULL AND " +
				"    F1.year = ? AND F1.month_id = ? AND F1.day_of_month = ? AND " +
				"    F2.year = ? AND F2.month_id = ? AND F2.day_of_month = ? AND " +
				"    F1.origin_city = ? AND F2.dest_city = ? AND" +
				"    F1.dest_city = F2.origin_city " +
				"ORDER BY F1.actual_time + F2.actual_time ASC LIMIT 99");
		countUserReservations = conn.prepareStatement("SELECT COUNT(r.uid) AS count " +
				"FROM RESERVATIONS r " +
				"INNER JOIN FLIGHTS f on r.id = r.id "+
				"WHERE r.uid = ? AND f.YEAR = ? AND f.month_id = ? AND f.day_of_month = ?");
		countReservationsForFlight = conn.prepareStatement("SELECT COUNT(*) AS count FROM RESERVATIONS WHERE fid = ?");
		insertReservation = conn.prepareStatement("INSERT INTO RESERVATION ('uid', 'fid') "+
				"VALUES(?, ?)");
		selectReservationsOfUser = conn.prepareStatement("SELECT fid, name, flight_num, origin_city, dest_city, actual_time, YEAR, "+
						"month_id, day_of_month FROM FLIGHTS f "+
				"INNER JOIN RESERVATION r ON fid = r.fid "+
				"INNER JOIN CARRIERS c ON carrier_id = c.cid "+
				"WHERE r.uid = ?");
		removeReservationsOfUser = conn.prepareStatement("DELETE FROM RESERVATION WHERE uid = ? AND fid = ?");
	}

	/**
	 * Tries to log in as the given user.
	 *
	 * @return The authenticated user or null if login failed.
	 */
	public User logIn(String handle, String password) throws SQLException {
		// TODO: implement this properly
		selectCustomer.setString(1, handle);
		selectCustomer.setString(2, password);

		ResultSet result = selectCustomer.executeQuery();
		if (result.next()) {
			return new User(result.getInt("uid"), result.getString("handle"), result.getString("name"));
		} else {
			LOGGER.error("Invalid credentials: " + handle + ", " + password);
			return null;
		}
	}

	/**
	 * Returns the list of all flights between the given cities on the given day.
	 */
	public List<Flight[]> getFlights(LocalDate date, String originCity, String destCity) throws SQLException {
		List<Flight[]> results = new ArrayList<>();
		directFlights.setInt(1, date.getYear());
		directFlights.setInt(2, date.getMonthValue());
		directFlights.setInt(3, date.getDayOfMonth());
		directFlights.setString(4, originCity);
		directFlights.setString(5, destCity);

		ResultSet directResults = directFlights.executeQuery();

		while (directResults.next()) {
			results.add(new Flight[] {
					new Flight(directResults.getInt("fid"), date,
							directResults.getString("name"),
							directResults.getString("flight_num"),
							directResults.getString("origin_city"),
							directResults.getString("dest_city"),
							(int) directResults.getFloat("actual_time"))});
		}
		directResults.close();


		twoHopFlights.setInt(1, date.getYear());
		twoHopFlights.setInt(2, date.getMonthValue());
		twoHopFlights.setInt(3, date.getDayOfMonth());
		twoHopFlights.setString(4, originCity);
		twoHopFlights.setString(5, destCity);
		System.out.println(twoHopFlights.toString());
		ResultSet twoHopResults = twoHopFlights.executeQuery();
		while (twoHopResults.next()) {
			results.add(new Flight[] {
					new Flight(twoHopResults.getInt("fid1"), date,
							twoHopResults.getString("name1"),
							twoHopResults.getString("flight_num1"),
							twoHopResults.getString("origin_city1"),
							twoHopResults.getString("dest_city1"),
							(int) twoHopResults.getFloat("actual_time1")),
					new Flight(twoHopResults.getInt("fid2"), date,
							twoHopResults.getString("name2"),
							twoHopResults.getString("flight_num2"),
							twoHopResults.getString("origin_city2"),
							twoHopResults.getString("dest_city2"),
							(int) twoHopResults.getFloat("actual_time2"))});
		}
		twoHopResults.close();
		return results;
	}

	/**
	 * Returns the list of all flights reserved by the given user.
	 */
	public List<Flight> getReservations(User user) throws SQLException {
		// TODO: implement this properly
		List<Flight> vluchten = new ArrayList<>();
		selectReservationsOfUser.setInt(1, user.getId());
		ResultSet res = selectReservationsOfUser.executeQuery();
		while (res.next()){
			LocalDate reservationDate = LocalDate.of(res.getInt("YEAR"), res.getInt("month_id"), res.getInt("day_of_month"));
			vluchten.add(new Flight(res.getInt("fid"), reservationDate, res.getString("name"),
					res.getString("flight_num"), res.getString("origin_city"),
					res.getString("dest_city"), (int)res.getFloat("actual_time")));
		}
		res.close();
		return vluchten;
	}

	/**
	 * Indicates that a reservation was added successfully.
	 */
	public static final int RESERVATION_ADDED = 1;

	/**
	 * Indicates the reservation could not be made because the flight is full
	 * (i.e., 3 users have already booked).
	 */
	public static final int RESERVATION_FLIGHT_FULL = 2;

	/**
	 * Indicates the reservation could not be made because the user already has a
	 * reservation on that day.
	 */
	public static final int RESERVATION_DAY_FULL = 3;

	/**
	 * Attempts to add a reservation for the given user on the given flights, all
	 * occurring on the given day.
	 *
	 * @return One of the {@code RESERVATION_*} codes above.
	 */
	public int addReservations(User user, LocalDate date, List<Flight> flights)
			throws SQLException {

		// TODO: implement this in a transaction
		conn.setAutoCommit(false);
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		//count user reservaties
		countUserReservations.setLong(1, user.getId());
		countUserReservations.setInt(2, date.getYear());
		countUserReservations.setInt(3, date.getMonthValue());
		countUserReservations.setInt(4, date.getDayOfMonth());
		int numberOfReservations = -1;
		ResultSet countResult = countUserReservations.executeQuery();
		if (countResult.next()){
			numberOfReservations = countResult.getInt("count");
		}
		if (numberOfReservations > 0) {
			return RESERVATION_DAY_FULL;
		}
		// count flightreservaties
		for (Flight flight : flights) {
			ResultSet countFlightResult = countReservationsForFlight.executeQuery();
			if (countFlightResult.next()){
				numberOfReservations = countResult.getInt("count");
			}
			if (numberOfReservations >= MAX_FLIGHT_BOOKINGS) {
				conn.rollback();
				return RESERVATION_FLIGHT_FULL;
			}
			insertReservation.setLong(1, user.getId());
			insertReservation.setLong(2, flight.getId());
			insertReservation.execute();
		}
		System.out.println("Press enter to continue..");
		scanner.nextLine();
		conn.commit();
		conn.setAutoCommit(false);
		conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		return RESERVATION_ADDED;
	}

	/**
	 * Cancels all reservations for the given user on the given flights.
	 */
	public void removeReservations(User user, List<Flight> flights)
			throws SQLException {
		// TODO: implement this in a transaction
		conn.setAutoCommit(false);
		try {
			for (Flight f : flights) {
				removeReservationsOfUser.setInt(1, user.getId());
				removeReservationsOfUser.setInt(2, f.getId());
				removeReservationsOfUser.executeQuery();
			}
			conn.commit();
		} catch (SQLException e) {
			conn.rollback();
		} finally {
			conn.setAutoCommit(false);
		}
	}
}
