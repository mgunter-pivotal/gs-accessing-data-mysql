package hello;

import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

import io.pivotal.labs.cfenv.CloudFoundryEnvironment;
import io.pivotal.labs.cfenv.CloudFoundryEnvironmentException;
import io.pivotal.labs.cfenv.CloudFoundryService;

/**
 *  Purpose of this tool is to create multi-threaded database connections to TIME/TOA/SCHEDULER application
 *  and constantly query the database in an attempt to open all the tables in the database.
 */
public class OpenTables implements Runnable {

	public static void main2(String[] args) throws Exception {
		(new Thread(new OpenTables("jdbc:mysql://odZjiaoWn7jBaf7t:SbT2wcgkvQjGd2xq@10.0.0.70:3306/cf_8354bc6b_d19d_43b5_88bb_1286d978a4a2?reconnect=true" +
				"user=odZjiaoWn7jBaf7t" +
				"password=SbT2wcgkvQjGd2xq" +
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

	private OpenTables(String connectionUrl, String appName) throws Exception {
		tables = new ArrayList<>();
		CloudFoundryEnvironment environment;
        try {
            environment = new CloudFoundryEnvironment(System::getenv);
        } catch (CloudFoundryEnvironmentException e) {
            throw new Exception(e);
        }


        try {
            for (String serviceName : environment.getServiceNames()) {
                CloudFoundryService service = environment.getService(serviceName);
                System.out.println("[" + service.getName() + "]");
                System.out.println("label = " + service.getLabel());
                if (service.getPlan() != null) System.out.println("plan = " + service.getPlan());
                System.out.println("tags = " + service.getTags().stream().collect(Collectors.joining(", ")));
                java.util.Map<String, Object> credentials = service.getCredentials();
                credentials.forEach((name, value) -> System.out.println("credentials." + name + " = " + value));
                System.out.println();
            }
		conn = DriverManager.getConnection(connectionUrl);
		dbName = conn.getCatalog();
		this.appName = appName;
		getTableNames();
		}
        catch(Exception e){ e.printStackTrace();}
		finally{}
        
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
