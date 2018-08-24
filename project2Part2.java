import java.io.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;


public class project2Part2 {
	private static final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String DB_CONNECTION = "jdbc:oracle:thin:@localhost:1521:CSE1";
	private static final String DB_USER = "uta_netid";
	private static final String DB_PASSWORD = "password for db";

	public static void main(String[] argv) throws ParseException, IOException, SQLException {

		cPopHistInsertion("UnitedStates", "2014", "4646446464");
		//sPopHistInsertion("California", "2015", "5656556656");
		//cYpopUpdation_q4("Mexico", "200", "12");
		//sYpopUpdation_q4("Illinois", "2013", "12");
	}

	private static Connection getDBConnection() {

		Connection dbConnection = null;

		try {

			Class.forName(DB_DRIVER);

		} catch (ClassNotFoundException e) {

			System.out.println(e.getMessage());

		}

		try {

			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
			return dbConnection;

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		}

		return dbConnection;

	}

	/* TO UPDATE THE COUNTRY POPULATION HISTORY TABLE */

	private static void cPopHistInsertion(String country, String year, String population)
			throws SQLException, ParseException, IOException {

		Connection dbConnection = null;
		PreparedStatement preparedUpdateStatement = null;

		boolean isPreviousYearExsist = checkYearCondition(country, "country", year);
		
		if (isPreviousYearExsist) {

			String insertCountryPopulation = "INSERT INTO C_POP_HIST"
					+ "(country,year,population) VALUES" + "(?,?,?)";

			try {
				dbConnection = getDBConnection();
				preparedUpdateStatement = dbConnection.prepareStatement(insertCountryPopulation);

				preparedUpdateStatement.setString(1, country);
				preparedUpdateStatement.setString(2, year);
				preparedUpdateStatement.setString(3, population);

				// execute update SQL statement
				preparedUpdateStatement.executeUpdate();

				System.out.println("Updated Record into C_POP_HIST table!");

			} catch (SQLException e) {

				System.out.println(e.getMessage());

			} finally {

				if (preparedUpdateStatement != null) {
					preparedUpdateStatement.close();
				}

				if (dbConnection != null) {
					dbConnection.close();
				}

			}
		}

		else {
			System.out.println("There is a existing record for this year or you are entering old values");
		}
	}

	/* TO UPDATE STATE POPULATION HISTORY TABLE */

	private static void sPopHistInsertion(String state, String year, String population)
			throws SQLException, NumberFormatException, IOException, ParseException {

		Connection dbConnection = null;
		PreparedStatement preparedUpdateStatement = null;
		
		boolean isPreviousYearExsist = checkYearCondition(state, "state", year);

		if (isPreviousYearExsist) {

		String insertStatePopulation = "INSERT INTO S_POP_HIST" + "(state,year,population) VALUES"
				+ "(?,?,?)";

		try {
			dbConnection = getDBConnection();
			preparedUpdateStatement = dbConnection.prepareStatement(insertStatePopulation);

			preparedUpdateStatement.setString(1, state);
			preparedUpdateStatement.setString(2, year);
			preparedUpdateStatement.setString(3, population);

			// execute update SQL statement
			preparedUpdateStatement.executeUpdate();

			System.out.println("Updated Record into S_POP_HIST table!");

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (preparedUpdateStatement != null) {
				preparedUpdateStatement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}
	}
		
		else {
			System.out.println("There is a existing record for this year or you are entering old values");
		}
	}
	
	

	private static boolean checkYearCondition(String countryOrStateName, String methodName, String year) throws SQLException {
		String getYearData = null;
		Connection dbConnection = null;
		PreparedStatement preparedUpdateStatement = null;
		if (methodName == "country") {
			getYearData = "Select year from C_POP_HIST where country=?"+"Order by year desc";
			dbConnection = getDBConnection();
			preparedUpdateStatement = dbConnection.prepareStatement(getYearData);
			preparedUpdateStatement.setString(1, countryOrStateName);

		} else if (methodName == "state") {
			getYearData = "Select year from S_POP_HIST where state=?"+" ORDER BY year DESC";
			dbConnection = getDBConnection();
			preparedUpdateStatement = dbConnection.prepareStatement(getYearData);
			preparedUpdateStatement.setString(1, countryOrStateName);
		}

		try {
			ResultSet resultSet = preparedUpdateStatement.executeQuery();
			 while (resultSet.next()) {
			String allYear = resultSet.getString("year");
			Integer topYear = Integer.parseInt(allYear);
			Integer queryYear = Integer.parseInt(year);
			if (topYear >= queryYear) {
				return false;
			}
			return true;
			 }
				return false;

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (preparedUpdateStatement != null) {
				preparedUpdateStatement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}
		return true;
	}
	
		public static void cYpopUpdation_q4 ( String country, String year , String population) throws SQLException{
			Connection dbConnection = null;
			PreparedStatement preparedSelectStatement = null;
			PreparedStatement preparedUpdateStatement = null;
			ResultSet resultSetObject = null; 
			
			String previousResult = "Select * from C_POP_HIST where country = ? and year = ? ";
			String updateCountryPopulation = "UPDATE C_POP_HIST SET population = ? "
			        + " WHERE country = ? And year = ? ";
			
	
			try {
				dbConnection = getDBConnection();
				preparedSelectStatement = dbConnection.prepareStatement(previousResult);
				preparedUpdateStatement = dbConnection.prepareStatement(updateCountryPopulation);
				preparedSelectStatement.setString(1, country);
				preparedSelectStatement.setString(2, year);
				preparedUpdateStatement.setString(1, population);
				preparedUpdateStatement.setString(2, country);
				preparedUpdateStatement.setString(3, year);
				// execute update SQL statement
				resultSetObject = preparedSelectStatement.executeQuery()	;
				 
				 if(resultSetObject != null){
					 System.out.println("Data in the Database was:");
				 getCountryData(resultSetObject);
				 Integer successOrFailure = preparedUpdateStatement.executeUpdate();
				 	if(successOrFailure == 1){
				 		resultSetObject = preparedSelectStatement.executeQuery();
				 		System.out.println("Updated Data in Database is:");
							 getCountryData(resultSetObject);
				 	}
				 	else {
				 		System.out.println("Updation did't happen");
				 	}
				 	
				 }else {
					 System.out.println("Record not available for coutry and year");
				 }

			} catch (SQLException e) {

				System.out.println(e.getMessage());

			} finally {

				if (preparedSelectStatement != null) {
					preparedSelectStatement.close();
				}

				if (dbConnection != null) {
					dbConnection.close();
				}
				
				if (resultSetObject != null) {
					resultSetObject.close();
				}

			}
			
		}
			public static void sYpopUpdation_q4 ( String state, String year , String population) throws SQLException{
				Connection dbConnection = null;
				PreparedStatement preparedSelectStatement = null;
				PreparedStatement preparedUpdateStatement = null;
				ResultSet resultSetObject = null; 
				
				String previousResult = "Select * from S_POP_HIST where state = ? and year = ? ";
				String updateStatePopulation = "UPDATE S_POP_HIST SET population = ? "
				        + " WHERE state = ? And year = ? ";
				
		
				try {
					dbConnection = getDBConnection();
					preparedSelectStatement = dbConnection.prepareStatement(previousResult);
					preparedUpdateStatement = dbConnection.prepareStatement(updateStatePopulation);
					preparedSelectStatement.setString(1, state);
					preparedSelectStatement.setString(2, year);
					preparedUpdateStatement.setString(1, population);
					preparedUpdateStatement.setString(2, state);
					preparedUpdateStatement.setString(3, year);
					// execute update SQL statement
					resultSetObject = preparedSelectStatement.executeQuery()	;
					 
					 if(resultSetObject != null){
						 System.out.println("Data in the Database was:");
						 getStateData(resultSetObject);
					 Integer successOrFailure = preparedUpdateStatement.executeUpdate();
					 	if(successOrFailure == 1){
					 		resultSetObject = preparedSelectStatement.executeQuery();
					 		System.out.println("Updated Data in Database is:");
					 		getStateData(resultSetObject);
					 	}
					 	else {
					 		System.out.println("Updation did't happen");
					 	}
					 	
					 }else {
						 System.out.println("Record not available for coutry and year");
					 }

				} catch (SQLException e) {

					System.out.println(e.getMessage());

				} finally {

					if (preparedSelectStatement != null) {
						preparedSelectStatement.close();
					}

					if (dbConnection != null) {
						dbConnection.close();
					}
					
					if (resultSetObject != null) {
						resultSetObject.close();
					}

				}
}

		private static void getCountryData(ResultSet resultSet) throws SQLException {
			while(resultSet.next()){
			     //Retrieve by column name
			     String updatedCountry = resultSet.getString("country");
                             String updatedYear  = resultSet.getString("year");
			     String updatedPopulation = resultSet.getString("population");

			     //Display values
			     System.out.print(", COUNTRY: " + updatedCountry);
                             System.out.print("YEAR: " + updatedYear);
                             System.out.print(", POPULATION: " + updatedPopulation +"\n");
			  }
		}
		
		private static void getStateData(ResultSet resultSet) throws SQLException {
			while(resultSet.next()){
			     //Retrieve by column name
			     String updatedState = resultSet.getString("state");
                             String updatedYear  = resultSet.getString("year");
			     String updatedPopulation = resultSet.getString("population");

			     //Display values
			     System.out.print(", STATE: " + updatedState);
                             System.out.print("YEAR: " + updatedYear);
			     System.out.print(", POPULATION: " + updatedPopulation +"\n");
			  }
		}
}




