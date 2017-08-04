package hello;

import java.sql.*;
import java.util.ArrayList;

/**
 *  Purpose of this tool is to create multi-threaded database connections to TIME/TOA/SCHEDULER application
 *  and constantly query the database in an attempt to open all the tables in the database.
 */
public class OpenTables implements Runnable {

	public static void main2(String[] args) throws SQLException {
		(new Thread(new OpenTables("jdbc:mysql://lb-mysql-wfm.paas.mia.ulti.io:3306/cf_7f909cea_c94d_4fa6_893e_a9d4f12590b0?" +
				"user=<REDACTED>" +
				"password=<REDACTED>" +
				"connectionCollation=utf8_general_ci&" +
				"characterSetResults=utf8&" +
				"characterEncoding=utf-8&" +
				"useLegacyDatetimeCode=false&" +
				"serverTimezone=UTC&" +
				"useFractionalSeconds=true", "TOA"))).start();
	}

	private Connection conn;
	private String dbName;
	private String appName;
	private ArrayList<String> tables;

	@Override
	public void run() {
		// Keep opening tables in a loop
		for (int i = 1; i <= 10; i++) {
			System.out.println(appName + ": " + i);
			try {
				for (String tableName : tables) {
					openTable(tableName);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private OpenTables(String connectionUrl, String appName) throws SQLException {
		tables = new ArrayList<>();
		conn = DriverManager.getConnection(connectionUrl);
		dbName = conn.getCatalog();
		this.appName = appName;
		getTableNames();
	}

	private void getTableNames() throws SQLException {
		String getTablesSql = "SELECT table_name " +
				"FROM information_schema.columns " +
				"WHERE table_schema = '" + dbName + "' " +
				"AND column_name = 'id'";
		ResultSet tableNames = getTable(getTablesSql, conn);

		while (tableNames.next()) {
			tables.add(tableNames.getObject(1).toString());
		}

		tableNames.close();
	}

	private ResultSet getTable(String sqlCommand, Connection conn) throws SQLException {
		PreparedStatement pStatement = conn.prepareStatement(sqlCommand, ResultSet.TYPE_SCROLL_SENSITIVE);
		return pStatement.executeQuery();
	}

	private void openTable(String tableName) throws SQLException {
		String sql = String.format("SELECT * FROM %s WHERE id = ? LIMIT 1", tableName);

		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setInt(1, -10000);
		ResultSet rs = ps.executeQuery();
		rs.close();
		ps.close();
	}
}
