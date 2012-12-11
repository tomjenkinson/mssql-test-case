package org.jboss.qe;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;

/**
 * Unit test for simple App.
 */
public class TestMSSQLReturnsCorrectFormatIDXid {
	String tableName = "tomsTest";

	@Before
	public void beforeTest() throws SQLException {
		SQLServerDataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();

		dataSource.setURL(System.getProperty("url"));
		Connection connection = dataSource.getConnection(
				System.getProperty("user"), System.getProperty("password"));
		Statement stmt = connection.createStatement();

		try {
			stmt.executeUpdate("DROP TABLE " + tableName);
		} catch (SQLException ex) {
			if (!ex.getSQLState().equals("S0005") && ex.getErrorCode() != 3701) {
				throw ex;
			}
		}

		String statement = "CREATE TABLE " + tableName
				+ " (tomsInt INTEGER NOT NULL, PRIMARY KEY(tomsInt))";
		stmt.executeUpdate(statement);
		connection.close();
	}

	@Test
	public void test() throws SQLException, XAException {
		SQLServerXADataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerXADataSource();

		dataSource.setURL(System.getProperty("url"));
		XAConnection xaConnection = dataSource.getXAConnection(
				System.getProperty("user"), System.getProperty("password"));
		XAResource xaResource = xaConnection.getXAResource();
		Xid xid = new XidImple();
		xaResource.start(xid, 0);
		Statement stmt = xaConnection.getConnection().createStatement();
		stmt.execute("INSERT INTO " + tableName + " VALUES (1)");
		xaResource.prepare(xid);
		Xid[] recover = xaResource.recover(XAResource.TMSTARTRSCAN);
		xaResource.recover(XAResource.TMENDRSCAN);
		assertTrue(recover.length == 1);
		assertTrue(Arrays.equals(recover[0].getBranchQualifier(),
				xid.getBranchQualifier()));
		assertTrue(Arrays.equals(recover[0].getGlobalTransactionId(),
				xid.getGlobalTransactionId()));
		assertTrue(recover[0].getFormatId() == xid.getFormatId());
		xaResource.commit(xid, true);
		xaConnection.close();
	}

	@After
	public void afterTest() throws SQLException, XAException {

		{
			SQLServerXADataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerXADataSource();

			dataSource.setURL(System.getProperty("url"));
			XAConnection xaConnection = dataSource.getXAConnection(
					System.getProperty("user"), System.getProperty("password"));
			XAResource xaResource = xaConnection.getXAResource();
			Xid[] recover = xaResource.recover(XAResource.TMSTARTRSCAN);
			for (int i = 0; i < recover.length; i++) {
				xaResource.rollback(recover[i]);
			}
			recover = xaResource.recover(XAResource.TMENDRSCAN);
			xaConnection.close();
		}
		{
			SQLServerDataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();
			dataSource.setURL(System.getProperty("url"));
			Connection connection = dataSource.getConnection(
					System.getProperty("user"), System.getProperty("password"));
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("DROP TABLE " + tableName);
			connection.close();
		}
	}

	private class XidImple implements javax.transaction.xa.Xid {
		private int formatid = 1234567;
		private String bqual;
		private String gtid;

		public XidImple() {
			bqual = String.valueOf(new Random().nextInt(100));
			gtid = String.valueOf(new Random().nextInt(100));
		}

		public byte[] getBranchQualifier() {
			System.out.println("bqual: " + bqual);
			return bqual.getBytes();
		}

		public int getFormatId() {
			System.out.println("formatid: " + formatid);
			return formatid;
		}

		public byte[] getGlobalTransactionId() {
			System.out.println("gtid: " + gtid);
			return gtid.getBytes();
		}

	}
}
