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

import org.junit.Test;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;

/**
 * Unit test for simple App.
 */
public class TestMSSQLReturnsCorrectFormatIDXid {

	@Test
	public void test() throws SQLException, XAException {

		{
			SQLServerDataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();

			dataSource.setURL(System.getProperty("url"));
			Connection connection = dataSource.getConnection(
					System.getProperty("user"), System.getProperty("password"));
			Statement stmt = connection.createStatement();

			try {
				stmt.executeUpdate("DROP TABLE tomsTest");
			} catch (SQLException ex) {
				if (!ex.getSQLState().equals("S0005")
						&& ex.getErrorCode() != 3701) {
					throw ex;
				}
			}

			String statement = "CREATE TABLE tomsTest (tomsInt INTEGER NOT NULL, PRIMARY KEY(tomsInt))";
			stmt.executeUpdate(statement);
			connection.close();
		}

		{
			SQLServerXADataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerXADataSource();

			dataSource.setURL(System.getProperty("url"));
			XAConnection xaConnection = dataSource.getXAConnection(
					System.getProperty("user"), System.getProperty("password"));
			XAResource xaResource = xaConnection.getXAResource();
			Xid xid = new XidImple();
			xaResource.start(xid, 0);
			Statement stmt = xaConnection.getConnection().createStatement();
			stmt.execute("INSERT INTO tomsTest VALUES (1)");
			xaResource.prepare(xid);
			Xid[] recover = xaResource.recover(0);
			assertTrue(Arrays.asList(recover).contains(xid));
			xaConnection.close();
		}

		{
			SQLServerDataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();
			dataSource.setURL(System.getProperty("url"));
			Connection connection = dataSource.getConnection(
					System.getProperty("user"), System.getProperty("password"));
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("DROP TABLE tomsTest");
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
