package com.rads.scheduler;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.BreakIterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import backtype.storm.command.list;

public class DBHelper {

	private transient Connection conn = null;

	public boolean InsertSchedule(String tpname, String supervisor, String port) {

		if (listcheckSchedule(supervisor, port).size() > 0) {

			deleteSchedule(supervisor, port);
		}
		boolean b = true;
		try {

			Connection conn = getConn();

			java.sql.Statement stmt = conn.createStatement();
			// String sql = "INSERT INTO demo.DemoAnalysis VALUES ('a')";

			String sql = "insert into demo.tpschedule (tpname, supervisor, port) values ('" + tpname + "','"
					+ supervisor + "','" + port + "')";

			stmt.executeUpdate(sql);
			// the mysql insert statement

			conn.close();
		} catch (Exception e) {
			b = false;
			System.err.println("Got an exception!");
			System.err.println(e.getMessage());
		}
		return b;
	}

	public String checkSchedule(String supervisor, String port) {
		ArrayList<String> al = listcheckSchedule(supervisor, port);

		if (al.size() > 0) {
			return al.get(0);
		} else
			return "none";

	}

	public ArrayList<String> listcheckSchedule(String supervisor, String port) {
		ArrayList<String> al = new ArrayList<String>();

		try {
			Connection conn = getConn();

			java.sql.Statement stmt = conn.createStatement();

			String sql = "select tpname from demo.tpschedule where supervisor='" + supervisor + "' and port ='" + port
					+ "'";
			ResultSet rs = stmt.executeQuery(sql);
			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name

				al.add(rs.getString("tpname"));

			}
			rs.close();
		}

		catch (Exception e) {
			System.out.println("DB Exception " + e.getMessage());
		}
		return al;

	}

	public int deleteSchedule(String supervisor, String port) {
		// ArrayList<String> al = new ArrayList<String>();
		int rtn = 0;
		try {
			Connection conn = getConn();

			java.sql.Statement stmt = conn.createStatement();

			String sql = "delete from demo.tpschedule where supervisor='" + supervisor + "' and port ='" + port + "'";
			rtn = stmt.executeUpdate(sql);
		} catch (Exception e) {
			System.out.println("DB Exception " + e.getMessage());
		}
		return rtn;

	}

	public ArrayList<PriorityRegister> getPriorityRegister() {
		ArrayList<PriorityRegister> al = new ArrayList<PriorityRegister>();

		try {
			Connection conn = getConn();

			java.sql.Statement stmt = conn.createStatement();

			String sql = "select tpname, priority, active, defaultpriority from demo.priorityregister";

			ResultSet rs = stmt.executeQuery(sql);
			// STEP 5: Extract data from result set
			while (rs.next()) {
				PriorityRegister pr = new PriorityRegister();

				pr.setActive((rs.getString("active").equals("1") ? true : false));
				pr.setDefaultpriority(Integer.parseInt(rs.getString("defaultpriority")));
				pr.setPriority(Integer.parseInt(rs.getString("priority")));
				pr.setTpname(rs.getString("tpname"));
				al.add(pr);

			}
			rs.close();
		}

		catch (Exception e) {
			System.out.println("DB Exception " + e.getMessage());
		}
		return al;

	}

	private Connection getConn() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return null;
		}

		// System.out.println("MySQL JDBC Driver Registered!");
		this.conn = null;

		try {
			this.conn = DriverManager.getConnection(
					"jdbc:mysql://192.168.2.13:3308/demo?autoReconnect=true&useSSL=false", "root", "root");

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}

		if (this.conn != null) {
			// System.out.println("You made it, take control your database
			// now!");
			return this.conn;
		} else {
			System.out.println("Failed to make connection!");
			return null;
		}

	}

}
