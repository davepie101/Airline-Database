/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//ch2
import javax.swing.*;
import java.awt.*;
import java.awt.Graphics;



/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");

			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}

	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 *
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException {
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 *
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;

		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 *
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 * obtains the metadata object for the returned result set.  The metadata
		 * contains row and column info.
		*/
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;

		//iterates through the result set and saves the data returned by the query.
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>();
		while (rs.next()){
			List<String> record = new ArrayList<String>();
			for (int i=1; i<=numCol; ++i)
				record.add(rs.getString (i));
			result.add(record);
		}//end while
		stmt.close ();
		return result;
	}//end executeQueryAndReturnResult

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 *
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}

	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current
	 * value of sequence used for autogenerated keys
	 *
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */

	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();

		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

//gui testing
//public class Label{
//		public static void main(String[] args)
//		 JLabel label = new JLabel("hello");
//		 frame.add(label);
//	}
//}





	/**
	 * The main execution method
	 *
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if

		DBproject esql = null;

		try{
			System.out.println("(1)");

			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}

			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];

			esql = new DBproject (dbname, dbport, user, "");

			boolean keepon = true;

			while(keepon){
				System.out.println("\033[36m");
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT\n");
				System.out.println("\033[0m");

				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.println("\033[1;31m");
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
					System.out.println("\033[0m");
				}//end if
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.println("\033[36m");
			System.out.print("Please make your choice: ");
			System.out.println("\033[0m");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input is invalid!");
				System.out.println("\033[0m");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddPlane(DBproject esql) {//1
		int id;
		String make;
		int age;
		int seats;
		String model;

		System.out.println("\033[36m");
		System.out.print("You have selected to add a plane to the database. Please enter the following information.\n");
		System.out.println("\033[0m");

		do {
			System.out.println("\033[32m");
			//Asking for make of plane
			System.out.print("What is the make of the plane?\n");
			System.out.println("\033[0m");

			try {
				make = in.readLine();

				Pattern pattern = Pattern.compile("[a-zA-z ]*");
				Matcher matcher = pattern.matcher(make);
				boolean match = matcher.matches();


				if (!match) {
					System.out.println("\033[1;31m");
					System.out.println("ERROR: Your input for make has to consist of alphabets.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for make of the plane is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			//Asking for model of plane
			System.out.println("\033[32m");
			System.out.print("What is the model of the plane?\n");
			System.out.println("\033[0m");
			try {
				model = in.readLine();
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for model of the plane is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			//Asking for the age of the plane
			System.out.println("\033[32m");
			System.out.print("What is the age of the plane?\n");
			System.out.println("\033[0m");
			try {
				age = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for age of the plane is invalid! Please try again.");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			//Asking for the number of seats on the plane
			System.out.println("\033[32m");
			System.out.print("How many seats does the plane contain?\n");
			System.out.println("\033[0m");
			try {
				seats = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for number of seats of the plane is invalid! Please try again.");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		try {
			//Inserting plane into database
			String query = String.format("SELECT id FROM Plane");
			List<List<String>> data_id = esql.executeQueryAndReturnResult(query);
			id = data_id.size();

			query = String.format("INSERT INTO Plane (id, make, model, age, seats) VALUES (%d, '%s', '%s', %d, %d)", id, make, model, age, seats);
			esql.executeUpdate(query);
			System.out.println("\033[1;31m");
			System.out.print("You have successfully added a plane to the database.\n\n");
			System.out.println("\033[0m");

		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	public static void AddPilot(DBproject esql) {//2
		String fullname;
		String nationality;

		System.out.println("\033[36m");
		System.out.print("You have selected to add a pilot to the database. Please enter the following information.\n");
		System.out.println("\033[0m");

		do {
			//Asking for name of the pilot
			System.out.println("\033[32m");
			System.out.print("What is the full name of the pilot?\n");
			System.out.println("\033[0m");
			try {
				fullname = in.readLine();

				Pattern pattern = Pattern.compile("[a-zA-z ]*");
				Matcher matcher = pattern.matcher(fullname);
				boolean match = matcher.matches();

				if (!match) {
					System.out.println("\033[1;31m");
					System.out.println("ERROR: Your input for full name has to consist of alphabets.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the full name of the pilot is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			//Asking for nationality of the pilot
			System.out.println("\033[32m");
			System.out.print("What is the nationality of the pilot?\n");
			System.out.println("\033[0m");

			try {
				nationality = in.readLine();
				Pattern pattern = Pattern.compile("[a-zA-z ]*");
				Matcher matcher = pattern.matcher(nationality);
				boolean match = matcher.matches();

				if (!match) {
					System.out.println("\033[1;31m");
					System.out.println("ERROR: Your input for nationality has to consist of alphabets.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}

			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the nationality of the pilot is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		try {
			//Inserting pilot into database
			String query = String.format("SELECT id FROM Pilot");
			List<List<String>> data_id = esql.executeQueryAndReturnResult(query);
			int id = data_id.size();

			query = String.format("INSERT INTO Pilot (id, fullname, nationality) VALUES (%d, '%s', '%s')", id, fullname, nationality);
			esql.executeUpdate(query);
			System.out.println("\033[1;31m");
			System.out.print("You have successfully added a pilot to the database.\n\n");
			System.out.println("\033[0m");
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}

	}

	public static void AddFlight(DBproject esql) {//3
		int cost;
		int stops;
		String depart_date;
		String arrival_date;
		String source;
		String destination;

		System.out.println("\033[36m");
		System.out.print("You have selected to add a flight to the database. Please enter the following information.\n");
		System.out.println("\033[0m");

		do {
			//Asking cost of ticket
			System.out.println("\033[32m");
			System.out.print("What is the cost of a ticket for this flight?\n");
			System.out.println("\033[0m");

			try {
				cost = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the cost of the ticket is invalid! Please try again.");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			// Asking for the departure date
			System.out.println("\033[32m");
			System.out.print("What is the departure date for this flight?(i.e. YYYY-MM-DD)\n");
			System.out.println("\033[0m");

			try {
				depart_date = in.readLine();

				String [] dateArr = depart_date.split("-", 3);
				if (dateArr.length != 3) {
					System.out.println("\033[1;31m");
					System.out.println("Error: Invalid date format.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}

			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the departure date of the flight is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			// Asking for the arrival date
			System.out.println("\033[32m");
			System.out.print("What is the arrival date for this flight?(i.e. YYYY-MM-DD)\n");
			System.out.println("\033[0m");

			try {
				arrival_date = in.readLine();

				String [] dateArr = arrival_date.split("-", 3);
				if (dateArr.length != 3) {
					System.out.println("\033[1;31m");
					System.out.println("Error: Invalid date format.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the arrival date of the flight is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			// Asking for the airport source
			System.out.println("\033[32m");
			System.out.print("What is the 5 character code for the arrival airport?\n");
			System.out.println("\033[0m");

			try {
				source = in.readLine();

				if (source.length() != 5) {
					System.out.println("\033[1;31m");
					System.out.println("Error: Arrival code must be 5 characters.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the arrival airport code is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			// Asking for the airport destination
			System.out.println("\033[32m");
			System.out.print("What is the 5 character code for the departure airport?\n");
			System.out.println("\033[0m");

			try {
				destination = in.readLine();

				if (destination.length() != 5) {
					System.out.println("\033[1;31m");
					System.out.println("Error: Departure code must be 5 characters.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the departure airport code is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			// Asking for the number of stops for this flight
			System.out.println("\033[32m");
			System.out.print("How many stops will there be for this flight?\n");
			System.out.println("\033[0m");

			try {
				stops = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the number of stops is invalid! Please try again.");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		try {
			String query = String.format("SELECT fnum FROM Flight");
			List<List<String>> data_id = esql.executeQueryAndReturnResult(query);
			int fnum = data_id.size();

			query = String.format("INSERT INTO Flight (fnum, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_airport, departure_airport) VALUES (%d, %d, 0, %d, '%s', '%s', '%s', '%s')", fnum, cost, stops, depart_date, arrival_date, source, destination);
			esql.executeUpdate(query);
			System.out.println("\033[1;31m");
			System.out.print("You have successfully added a flight to the database.\n\n");
			System.out.println("\033[0m");
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	public static void AddTechnician(DBproject esql) {//4
		int id;
		String full_name;

		do {
			System.out.println("\033[32m");
			System.out.print("You have selected to add a technician to the database. Please enter the following information. \n");
			System.out.print("What is the full name of the technician?\n");
			System.out.println("\033[0m");

			try {
				full_name = in.readLine();

				Pattern pattern = Pattern.compile("[a-zA-z ]*");
				Matcher matcher = pattern.matcher(full_name);
				boolean match = matcher.matches();

				if (!match) {
					System.out.println("\033[1;31m");
					System.out.println("ERROR: Your input for full name has to consist of alphabets.");
					System.out.println("\033[0m");
				}
				else {
					break;
				}
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the full name of the technician is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		try {
			String query = String.format("SELECT id FROM Technician");
			List<List<String>> data_id = esql.executeQueryAndReturnResult(query);
			id = data_id.size();

			query = String.format("INSERT INTO Technician (id, full_name) VALUES (%d, '%s')", id, full_name);
			esql.executeUpdate(query);
			System.out.println("\033[1;31m");
			System.out.print("You have successfully added a technician to the database\n\n");
			System.out.println("\033[0m");
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	public static void BookFlight(DBproject esql) {//5
		int id;
		int fnum;

		System.out.println("\033[36m");
		System.out.print("You have chosen to book a flight. Please enter the following information.\n");
		System.out.println("\033[0m");

		do {
			System.out.println("\033[32m");
			System.out.print("What is the id of the customer?\n");
			System.out.println("\033[0m");

			try {
				id = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the customer id is invalid! Please try again.");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		do {
			System.out.println("\033[32m");
			System.out.print("What is the number of the flight that you wish to book?\n");
			System.out.println("\033[0m");

			try {
				fnum = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the flight number is invalid! Please try again. ");
				System.out.println("\033[0m");
				continue;
			}
		}while(true);

		try {
			String temp = String.format("SELECT * FROM Reservation");
			List<List<String>> rnum_data = esql.executeQueryAndReturnResult(temp);
			int rnum = rnum_data.size();

			String query = String.format("INSERT INTO Reservation(rnum, cid, fid, status) VALUES (%d, %d, %d, CASE WHEN (SELECT P.seats - F.num_sold FROM Plane P, Flight F, FlightInfo FL WHERE P.id = FL.plane_id AND F.fnum = FL.flight_id AND F.fnum = %d) > 0 THEN 'C' ELSE 'W' END)", rnum, id, fnum, fnum);
			esql.executeUpdate(query);
			query = String.format("UPDATE Flight SET num_sold = num_sold + 1 WHERE fnum = %d", fnum);
			esql.executeUpdate(query);

			rnum_data = esql.executeQueryAndReturnResult(temp);
			String new_status = rnum_data.get(rnum).get(3);

			if (new_status.equals("C")) {
				System.out.println("\033[1;31m");
				System.out.println("The flight that you wish to book still has seats available. Your reservation is now confirmed.");
				System.out.println("\033[0m");
			}
			else {
				System.out.println("\033[1;31m");
				System.out.println("The flight that you wish to book is sold out. You have now been waitlisted.");
				System.out.println("\033[0m");
			}
			System.out.print("\n");
		}catch(Exception e) {
			System.err.println(e.getMessage());
		}



	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
		int flight_num;

		System.out.println("\033[36m");
		System.out.print("To find the number of seats for a given flight, please enter the following information. \n");
		System.out.println("\033[0m");

		do {
			//Asking for flight number
			System.out.println("\033[32m");
			System.out.print("What is the flight number?\n");
			System.out.println("\033[0m");

			try {
				flight_num = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for the flight number is invalid! Please try again.");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		try {
			String query1 = String.format("SELECT P.seats - (SELECT F.num_sold FROM Plane P, Flight F, FlightInfo FL WHERE P.id = FL.plane_id AND FL.flight_id = F.fnum and F.fnum = %d)", flight_num);
			String query2 = String.format(" as Available_seats FROM Plane P, Flight F, FlightInfo FL WHERE P.id = FL.plane_id AND FL.flight_id = F.fnum AND F.fnum = %d", flight_num);

			query1 = query1 + query2;
			System.out.print("\n");
			System.out.println("\033[1;31m");
			esql.executeQueryAndPrintResult(query1);
			System.out.println("\033[0m");
			System.out.print("\n");
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}

	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
		System.out.println("\033[36m");
		System.out.print("You have selected to find the number of repairs per plane in descending order.\n");
		System.out.println("\033[0m");

		try {
			String query = "SELECT P.id, COUNT(R.rid) AS Repair_Count FROM Plane P, Repairs R WHERE R.plane_id = P.id GROUP BY P.id ORDER BY Count(R.rid) DESC";
			System.out.println("\033[40m");
			System.out.println("\033[4;31m");
			esql.executeQueryAndPrintResult(query);
			System.out.println("\033[0m");
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}

		System.out.print("\n\n");
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		System.out.println("\033[32m");
		System.out.print("You have selected to view the number of repairs per year in ascending order\n");
		System.out.println("\033[30m");

		try {
			String query = "SELECT DISTINCT EXTRACT(year FROM repair_date) AS YEAR, COUNT(rid) FROM Repairs GROUP BY EXTRACT(year FROM repair_date) ORDER BY COUNT(rid) ASC";
			System.out.println("\033[40m");
			System.out.println("\033[4;31m");
			esql.executeQueryAndPrintResult(query);
			System.out.println("\033[0m");
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
		System.out.print("\n\n");
	}

	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		String status;
		int fnum;

		System.out.println("\033[32m");
		System.out.print("You have selected to find the total number of passengers with a given status. Please enter the following information.\n");
		System.out.print("What is the status of the passenger? Please input one of the following: W-Waitlisted, C-Confirmed, R-Reserved\n");
		System.out.println("\033[30m");

		do {
			Scanner myObj = new Scanner(System.in);
			status = myObj.nextLine();

			if (status.equals("W") || status.equals("C") || status.equals("R")) {
				break;
			}
			else {
				System.out.println("\033[1;31m");
				System.out.println("The input you have entered is invalid. Please enter W, C, or R.");
				System.out.println("\033[0m");
			}

		}while (true);

		do {
			System.out.println("\033[32m");
			System.out.print("What is the flight number?\n");
			System.out.println("\033[0m");

			try {
				fnum = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("\033[1;31m");
				System.out.println("Your input for flight number is invalid!");
				System.out.println("\033[0m");
				continue;
			}
		}while (true);

		try {
			String query;
			List<List<String>> list_passengers;
			int num_pass = 0;
			String end_message;
			if (status.equals("W")) {
				query = String.format("SELECT * FROM Reservation WHERE fid = %d AND status = 'W'", fnum);
				list_passengers = esql.executeQueryAndReturnResult(query);
				num_pass = list_passengers.size();
				System.out.println("\033[1;31m");
				end_message = String.format("The number of passengers that are waitlisted for flight %d is %d.", fnum, num_pass);
				System.out.print(end_message + "\n\n");
				System.out.println("\033[0m");
			}
			else if (status.equals("R")) {
				query = String.format("SELECT * FROM Reservation WHERE fid = %d AND status = 'R'", fnum);
				list_passengers = esql.executeQueryAndReturnResult(query);
				num_pass = list_passengers.size();
				System.out.println("\033[1;31m");
				end_message = String.format("The number of passengers that are reserved for flight %d is %d.", fnum, num_pass);
				System.out.print(end_message + "\n\n");
				System.out.println("\033[0m");
			}
			else {
				query = String.format("SELECT * FROM Reservation WHERE fid = %d AND status = 'C'", fnum);
				list_passengers = esql.executeQueryAndReturnResult(query);
				num_pass = list_passengers.size();
				System.out.println("\033[1;31m");
				end_message = String.format("The number of passengers that are confirmed for flight %d is %d.", fnum, num_pass);
				System.out.print(end_message + "\n\n");
				System.out.println("\033[0m");
			}
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
