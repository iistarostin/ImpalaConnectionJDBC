package sbt.ImpalaConnection;
import java.sql.*;
public class App {

	static String connectionURL = "jdbc:impala://quickstart.cloudera:21050";
	static int nInserts = 1000;
	public static void main(String[] args) {
		System.out.println("Starting...");
		try {
			Class.forName("com.cloudera.impala.jdbc41.Driver");
			System.out.println("Driver loaded");
			Connection conn = DriverManager.getConnection(connectionURL);
			System.out.println("Connection established");
			Statement stmt = conn.createStatement();
			stmt.execute("DROP TABLE IF EXISTS TestFromJava;");
			stmt.execute("CREATE TABLE TestFromJava (a integer);");
			System.out.println("Table created");
			for (int i = 0; i < nInserts; ++i)
			{
				PreparedStatement pstmt = conn.prepareStatement("INSERT INTO TestFromJava (a) VALUES (?)");
				pstmt.setInt(1, i);
				pstmt.executeUpdate();
			}
			System.out.println("Values inserted into table");
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM TestFromJava ORDER BY a");

			for (int i = 0; i < nInserts; ++i)
			{
				if (!rs.next() || rs.getInt(1) != i) {
					System.out.println("Error retrieving values");
					return;
				}
			}
			System.out.println("Values retrieved");
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
