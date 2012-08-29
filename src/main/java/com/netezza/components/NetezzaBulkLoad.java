package com.netezza.components;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.snaplogic.cc.Capabilities;
import org.snaplogic.cc.Capability;
import org.snaplogic.cc.ComponentAPI;
import org.snaplogic.cc.InputView;
import org.snaplogic.cc.OutputView;
import org.snaplogic.cc.prop.SimpleProp;
import org.snaplogic.cc.prop.SimpleProp.SimplePropType;
import org.snaplogic.common.ComponentResourceErr;
import org.snaplogic.common.exceptions.SnapComponentException;
import org.snaplogic.snapi.PropertyConstraint;
import org.snaplogic.snapi.PropertyConstraint.Type;
import org.snaplogic.snapi.ResDef;

import com.netezza.util.NetezzaDBHelper;

public class NetezzaBulkLoad extends ComponentAPI {

	private Capabilities capabilities;

	public static final String NETEZZA_CONNECTION_REF = "netezza_connection_ref";
	private static final String PROP_DELIMITER = "Delimiter";
	private static final String PROP_TABLE_NAME = "TableName";
	public final static String CONNECTION_CATEGORY_KEY = "connection.netezza";
	private static final String PROP_MAX_ROWS = "Max_Rows";

	public NetezzaBulkLoad() {
		capabilities = new Capabilities();
		capabilities.put(Capability.INPUT_VIEW_LOWER_LIMIT, 1);
		capabilities.put(Capability.INPUT_VIEW_UPPER_LIMIT, 1);
		capabilities.put(Capability.OUTPUT_VIEW_LOWER_LIMIT, 0);
		capabilities.put(Capability.OUTPUT_VIEW_UPPER_LIMIT, 0);
	}

	public Capabilities getCapabilities() {
		return capabilities;
	}

	public String getAPIVersion() {
		return "1.0";
	}

	public String getComponentVersion() {
		return "1.0";
	}

	@Override
	public String getDocURI() {
		// Do nothing by default.
		return super.getDocURI();
	}

	@Override
	public String getDescription() {
		return "Netezza Bulk Loader performs Load operations on a Netezza table using named pipe";
	}

	@Override
	public String getLabel() {
		return "Netezza Bulk Loader";
	}

	@Override
	public void createResourceTemplate() {
		// Do nothing by default
		super.createResourceTemplate();

		// Delimiter
		setPropertyDef(PROP_DELIMITER, new SimpleProp("Delimiter",
				SimplePropType.SnapString,
				"Delimiter to be used while pumping the data into Netezza",
				null, true));
		setPropertyValue(PROP_DELIMITER, ",");

		// Table name
		setPropertyDef(PROP_TABLE_NAME, new SimpleProp("Table Name",
				SimplePropType.SnapString,
				"The name of the DB table being modified", true));

		// Connection reference
		List<String> cats = new ArrayList<String>();
		cats.add(CONNECTION_CATEGORY_KEY);
		addResourceRefDef(NetezzaDBHelper.PROP_DB_CONNECT,
				"Netezza Connection", cats, true);

		// Max rows
		setPropertyDef(
				PROP_MAX_ROWS,
				new SimpleProp(
						"Max rows",
						SimplePropType.SnapString,
						"Specifies stopping the processing when the specified number of records are in the database",
						null, true));
		setPropertyValue(PROP_MAX_ROWS, "2");
	}

	

	@Override
	public void suggestResourceValues(ComponentResourceErr resdefError) {
		// Do nothing by default
		super.suggestResourceValues(resdefError);
	}

	@Override
	public void suggestErrorViews() {
		// Do nothing by default
		super.suggestErrorViews();
	}

	/**
	 * Attempt to connect to the database with current properties. If current
	 * properties are not parameterized and connection cannot be made, then an
	 * error is set.
	 * 
	 * 
	 */
	@Override
	public void validate(ComponentResourceErr resdefError) {
		// Do nothing by default
		super.validate(resdefError);

		

	}

	

	@Override
	public void validateConfigFile() {
		// Do nothing by default
		super.validateConfigFile();
	}

	@Override
	public void stop() {
		super.stop();
	}

	@Override
	public void execute(Map<String, InputView> inputViews,
			Map<String, OutputView> outputViews) {
		
		String server = "172.20.5.121";
		String port = "5480";
		String dbName = "system";
		String url = "jdbc:netezza://" + server + ":" + port + "/" + dbName;
		String user = "nmaniyar";
		String pwd = "blueberry";
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// Load the driver class
			Class.forName("org.netezza.Driver");
			System.out.println(" Connecting ... ");

			// create connection
			conn = DriverManager.getConnection(url, user, pwd);
			System.out.println(" Connected " + conn);

			// Create a Statement class to execute the SQL statement
			stmt = conn.createStatement();
			
			
			// Check if external table exists
			ResultSet tables = conn.getMetaData()
					.getTables(null, null, "extsltest", null);
			if (tables.next()) {
				// extsltest exists
				// drop extsltest
				String truncateExtTbl = "DROP TABLE extsltest";
				// Execute the SQL statement and get the result in resu
				stmt.executeUpdate(truncateExtTbl);
			}
			// create an external table
			String createExtTableSQL = "CREATE EXTERNAL TABLE extsltest SAMEAS SLtest"+
			" USING (DATAOBJECT ('/Users/admin/Documents/workspace/NetezzaMain/bin/testData.dat')" + 
			" MAXROWS 2" + 
			" REMOTESOURCE 'JDBC'" + 
			" DELIM ',' fillRecord)";
			// Execute the SQL statement and get the result in resu
			stmt.executeUpdate(createExtTableSQL);

			// upload data from external table into SLTest

			// first truncate table sltest
			String truncateTbl = "TRUNCATE TABLE SLtest";
			stmt.executeUpdate(truncateTbl);

			// then insert from external table into sltest
			String uploadExtTbl = "INSERT INTO SLtest SELECT * FROM extsltest";
			stmt.executeUpdate(uploadExtTbl);

			String sql = "select col1,col2,col3 from SLtest";
			rs = stmt.executeQuery(sql);

			if (rs.next()) {
				System.out.println(rs.getString(1));
			} else {
				System.out.println(" No data found");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}

	}

}