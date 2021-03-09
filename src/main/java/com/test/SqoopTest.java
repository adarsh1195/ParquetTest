package com.test;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.validation.Status;

public class SqoopTest {

	public static void main(String[] args) {

		String url = "http://10.41.133.101:12000/sqoop/";
		SqoopClient client = new SqoopClient(url);

		 createConnection(client);

		// modifyConnection(client);

	}

	/**
	 * 
	 * @param client
	 */
	private static void modifyConnection(SqoopClient client) {
		MConnection connection = client.getConnection(2);
		connection.setName("mycon");
		client.updateConnection(connection);
	}

	/**
	 * 
	 * @param client
	 */
	private static void createConnection(SqoopClient client) {
		// Dummy connection object
		MConnection newCon = client.newConnection(1);

		// Get connection and framework forms. Set name for connection
		MConnectionForms conForms = newCon.getConnectorPart();
		MConnectionForms frameworkForms = newCon.getFrameworkPart();
		newCon.setName("MyConnection");

		// Set connection forms values
		conForms.getStringInput("connection.connectionString").setValue("jdbc:mysql://10.41.2.15:3306/customer_info");
		conForms.getStringInput("connection.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		conForms.getStringInput("connection.username").setValue("root");
		conForms.getStringInput("connection.password").setValue("slk@SOFT123");

		frameworkForms.getIntegerInput("security.maxConnections").setValue(10);

		Status status = client.createConnection(newCon);
		if (status.canProceed()) {
			System.out.println("Created. New Connection ID : " + newCon.getPersistenceId());
		} else {
			System.out.println("Check for status and forms error ");
		}
	}

}
