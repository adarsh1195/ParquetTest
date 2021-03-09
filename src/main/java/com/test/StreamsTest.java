package com.test;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

public class StreamsTest {

	public static void main(String[] args) throws SQLException, IOException {

		String columns = "*";
		Connection con = DriverManager.getConnection("jdbc:mysql://10.41.2.15/customer_info?user=root&password=slk@SOFT123");
		String sql = "select " + columns + " from customers";
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter("C:\\Users\\praveen.kumar3\\Desktop\\New folder (3)\\sample.txt"));

		fileWriter.write("qwer");

		new QueryRunner().query(con, sql, new ArrayListHandler()).stream().forEach(array -> {
			int size = array.length;
			//String line = String.format("\"%s", array[0]);
			//String line = String.format("\"%s", array[0]);
			//String line = String.format("\"%s\",%s", array[0], array[1]);
			System.out.println(array[2]);
			String[] stringArray = Arrays.copyOf(array, array.length, String[].class);
			String line = StringUtils.join(stringArray, ",");
			System.out.println(line);
			
			String result = StringUtils.join(array, " - ");
			
			System.out.println(result);
			
			try {
				fileWriter.newLine();
				fileWriter.write(line);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		});

		System.out.println(con.getMetaData().getURL());

		fileWriter.close();
		con.close();

	}

}
