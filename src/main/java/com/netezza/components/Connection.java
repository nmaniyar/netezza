package com.netezza.components;


import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
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

public class Connection extends ComponentAPI {

	private Capabilities capabilities;

	public static final String NETEZZA_CONNECTION_REF = "netezza_connection_ref";

	public Connection() {
		capabilities = new Capabilities();
		capabilities.put(Capability.INPUT_VIEW_LOWER_LIMIT, 0);
		capabilities.put(Capability.INPUT_VIEW_UPPER_LIMIT, 0);
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
		return "This component provides connection to Netezza";
	}

	@Override
	public String getLabel() {
		return "Netezza Connection";
	}

	@Override
	public void createResourceTemplate() {
		// Do nothing by default
		super.createResourceTemplate();

		setPropertyDef(NetezzaDBHelper.PROP_DB_HOST, new SimpleProp("Host",
				SimplePropType.SnapString, "Hostname", true));
		setPropertyDef(NetezzaDBHelper.PROP_DB_PORT, new SimpleProp("Port",
				SimplePropType.SnapNumber, "Port", true));
		// TODO change the default port
		setPropertyValue(NetezzaDBHelper.PROP_DB_PORT, 3306);
		setPropertyDef(NetezzaDBHelper.PROP_DB_DB, new SimpleProp("Database",
				SimplePropType.SnapString, "Database", true));
		setPropertyDef(NetezzaDBHelper.PROP_DB_USER, new SimpleProp("User",
				SimplePropType.SnapString, "Username", true));
		PropertyConstraint passwdConstraint = new PropertyConstraint(
				Type.OBFUSCATE, 0);
		setPropertyDef(NetezzaDBHelper.PROP_DB_PASSWORD, new SimpleProp(
				"Password", SimplePropType.SnapString, "Password",
				passwdConstraint, false));
		setCats();
	}

	/**
	 * Set the category to "connection.db.<DB-type>"
	 */
	protected void setCats() {
		String cat = "connection.netezza";
		List<String> catList = new ArrayList<String>();
		catList.add(cat);
		setCategories(catList, false);
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

		Connection con = null;
		ResDef resdef = this.getResdef();
		for (String propName : resdef.listPropertyNames()) {
			Object propVal = getPropertyValue(propName);
			if (hasParam(propVal)) {
				return;
			}
		}

		try {

			if (!isValidHostPort(resdefError, resdef)) {
				return;
			}
			// con = helper.getConnection();
			// if (helper.canAutoCommit()) {
			// con.setAutoCommit(true);
			// }
		} catch (Exception e) {
			resdefError.setMessage("Connection Error", e.getMessage());
			elog(e);
		} finally {
			// if (helper != null) {
			// helper.closeQuietly(con);
			// }
		}

	}

	// checks if the host and port are valid
	protected boolean isValidHostPort(ComponentResourceErr err, ResDef resdef) {

		String host = null;
		host = (String) this.getPropertyValue(NetezzaDBHelper.PROP_DB_HOST);
		Integer port = null;
		try {
			port = (Integer) this
					.getPropertyValue(NetezzaDBHelper.PROP_DB_PORT);
		} catch (SnapComponentException e) {
			err.getPropertyErr(NetezzaDBHelper.PROP_DB_PORT).setMessage(
					e.getMessage());
			return false;
		}

		if (host == null || host.isEmpty()) {
			// report error
			err.getPropertyErr(NetezzaDBHelper.PROP_DB_HOST).setMessage(
					"Hostname expected");
			return false;
		}

		if (port == null) {
			err.getPropertyErr(NetezzaDBHelper.PROP_DB_PORT).setMessage(
					"Port expected");
			return false;
		}

		if (port <= 0 || port > 65535) {
			err.getPropertyErr(NetezzaDBHelper.PROP_DB_PORT).setMessage(
					"Port out of range");
			return false;
		}

		try {
			Socket s = new Socket(host, port);
			s.close();
			return true;
		} catch (UnknownHostException e) {
			err.getPropertyErr(NetezzaDBHelper.PROP_DB_HOST).setMessage(
					"Unknown Host", host);
			return false;
		} catch (UnknownServiceException e) {
			err.getPropertyErr(NetezzaDBHelper.PROP_DB_HOST).setMessage(
					"Unknown service", host, port, e.getMessage());
			return false;
		} catch (IOException e) {
			err.getPropertyErr(NetezzaDBHelper.PROP_DB_HOST).setMessage(
					"Error connecting to host", host, port, e.getMessage());
			return false;
		}

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

	}

}