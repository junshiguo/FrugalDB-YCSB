package frugaldb.server.loader.retriever;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import frugaldb.server.loader.utility.DBManager;

/**
 * retrive data for tenants whose id is tenantId
 * @author jsguo
 *
 */
public class CustomerRetriever extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int tenantId;
	public int volumnId;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public CustomerRetriever(String url, String username, String password, String voltdbServer, int tenantId, int volumnId){
		this.url = url;
		this.username = username;
		this.password = password;
		this.voltdbServer = voltdbServer;
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}

	@Override
	public void run() {
		try {
			conn = DBManager.connectDB(url, username, password);
			stmt = conn.createStatement();
			conn.setAutoCommit(false);
			statements = new PreparedStatement[2];
			statements[0] = conn.prepareStatement("UPDATE customer"+tenantId+" SET c_id = ?, c_d_id = ?,c_w_id = ?, c_first = ?, c_middle = ?, c_last = ?, c_street_1 = ?, c_street_2 = ?,c_city = ?,"
				+ "c_state = ?,c_zip = ?, c_phone = ?,c_since = ?, c_credit = ?, c_credit_lim = ?, c_discount = ?, c_balance = ?, c_ytd_payment = ?,c_payment_cnt = ?, c_delivery_cnt = ?, c_data = ? "
				+ "WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?");
			statements[1] = conn.prepareStatement("INSERT INTO customer"+tenantId+" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		} catch (Exception e1) {
			System.out.println("error in creating or preparing mysql statement for retriving data...");
		}
		voltdbConn = DBManager.connectVoltdb(voltdbServer);
		if(voltdbConn == null){
			System.out.println("error connecting to voltdb while retriving data...");
		}
		//******************************************************************************//
		ClientResponse response = null;
		VoltTable result = null;
		VoltTableRow row = null;
		try {
			response = voltdbConn.callProcedure("SelectCustomer_"+volumnId, tenantId, 0, 1);
			if (response.getStatus() == ClientResponse.SUCCESS	&& response.getResults()[0].getRowCount() != 0) {
				result = response.getResults()[0];
				for (int i = 0; i < result.getRowCount(); i++) {
					row = result.fetchRow(i);
					statements[0].setInt(1, (int) row.get("c_id", VoltType.INTEGER));
					statements[0].setInt(2, new Byte((byte) row.get("c_d_id", VoltType.TINYINT)).intValue());
					statements[0].setShort(3,	(short) row.get("c_w_id", VoltType.SMALLINT));
					statements[0].setString(4, row.getString("c_first"));
					statements[0].setString(5, row.getString("c_middle"));
					statements[0].setString(6, row.getString("c_last"));
					statements[0].setString(7, row.getString("c_street_1"));
					statements[0].setString(8, row.getString("c_street_2"));
					statements[0].setString(9, row.getString("c_city"));
					statements[0].setString(10, row.getString("c_state"));
					statements[0].setString(11, row.getString("c_zip"));
					statements[0].setString(12, row.getString("c_phone"));
					statements[0].setTimestamp(13, row.getTimestampAsSqlTimestamp("c_since"));
					statements[0].setString(14, row.getString("c_credit"));
					statements[0].setLong(15, (long) row.get("c_credit_lim", VoltType.BIGINT));
					statements[0].setBigDecimal(	16, row.getDecimalAsBigDecimal("c_discount").setScale(4, BigDecimal.ROUND_HALF_DOWN));
					statements[0].setBigDecimal(17, row.getDecimalAsBigDecimal("c_balance").setScale(12, BigDecimal.ROUND_HALF_DOWN));
					statements[0].setBigDecimal(18, row.getDecimalAsBigDecimal("c_ytd_payment").setScale(12, BigDecimal.ROUND_HALF_DOWN));
					statements[0].setShort(19, (short) row.get("c_payment_cnt", VoltType.SMALLINT));
					statements[0].setShort(20, (short) row.get("c_delivery_cnt", VoltType.SMALLINT));
					statements[0].setString(21, row.getString("c_data"));
					statements[0].setInt(22, (int) row.get("c_id", VoltType.INTEGER));
					statements[0].setInt(23, new Byte((byte) row.get("c_w_id", VoltType.TINYINT)).intValue());
					statements[0].setShort(24,	(short) row.get("c_d_id", VoltType.SMALLINT));
					statements[0].addBatch();
				}
				if(result.getRowCount() > 0) {
					statements[0].executeBatch();
					conn.commit();
				}
			}
			
			response = voltdbConn.callProcedure("SelectCustomer_"+volumnId, tenantId, 1, 0);
			if (response.getStatus() == ClientResponse.SUCCESS	&& response.getResults()[0].getRowCount() != 0) {
				result = response.getResults()[0];
				for (int i = 0; i < result.getRowCount(); i++) {
					row = result.fetchRow(i);
					statements[1].setInt(1, (int) row.get("c_id", VoltType.INTEGER));
					statements[1].setInt(2, new Byte((byte) row.get("c_d_id", VoltType.TINYINT)).intValue());
					statements[1].setShort(3,	(short) row.get("c_w_id", VoltType.SMALLINT));
					statements[1].setString(4, row.getString("c_first"));
					statements[1].setString(5, row.getString("c_middle"));
					statements[1].setString(6, row.getString("c_last"));
					statements[1].setString(7, row.getString("c_street_1"));
					statements[1].setString(8, row.getString("c_street_2"));
					statements[1].setString(9, row.getString("c_city"));
					statements[1].setString(10, row.getString("c_state"));
					statements[1].setString(11, row.getString("c_zip"));
					statements[1].setString(12, row.getString("c_phone"));
					statements[1].setTimestamp(13, row.getTimestampAsSqlTimestamp("c_since"));
					statements[1].setString(14, row.getString("c_credit"));
					statements[1].setLong(15, (long) row.get("c_credit_lim", VoltType.BIGINT));
					statements[1].setBigDecimal(	16, row.getDecimalAsBigDecimal("c_discount").setScale(4, BigDecimal.ROUND_HALF_DOWN));
					statements[1].setBigDecimal(17, row.getDecimalAsBigDecimal("c_balance").setScale(12, BigDecimal.ROUND_HALF_DOWN));
					statements[1].setBigDecimal(18, row.getDecimalAsBigDecimal("c_ytd_payment").setScale(12, BigDecimal.ROUND_HALF_DOWN));
					statements[1].setShort(19, (short) row.get("c_payment_cnt", VoltType.SMALLINT));
					statements[1].setShort(20, (short) row.get("c_delivery_cnt", VoltType.SMALLINT));
					statements[1].setString(21, row.getString("c_data"));
//					statements[1].addBatch();
					try{
						statements[1].execute();
						conn.commit();
					}catch(Exception e){}
				}
//				if(result.getRowCount() > 0) {
//					statements[1].executeBatch();
//				}
			}
			
			response = voltdbConn.callProcedure("SelectCustomer_"+volumnId, tenantId, 1, 1);
			if (response.getStatus() == ClientResponse.SUCCESS	&& response.getResults()[0].getRowCount() != 0) {
				result = response.getResults()[0];
				for (int i = 0; i < result.getRowCount(); i++) {
					row = result.fetchRow(i);
					statements[1].setInt(1, (int) row.get("c_id", VoltType.INTEGER));
					statements[1].setInt(2, new Byte((byte) row.get("c_d_id", VoltType.TINYINT)).intValue());
					statements[1].setShort(3,	(short) row.get("c_w_id", VoltType.SMALLINT));
					statements[1].setString(4, row.getString("c_first"));
					statements[1].setString(5, row.getString("c_middle"));
					statements[1].setString(6, row.getString("c_last"));
					statements[1].setString(7, row.getString("c_street_1"));
					statements[1].setString(8, row.getString("c_street_2"));
					statements[1].setString(9, row.getString("c_city"));
					statements[1].setString(10, row.getString("c_state"));
					statements[1].setString(11, row.getString("c_zip"));
					statements[1].setString(12, row.getString("c_phone"));
					statements[1].setTimestamp(13, row.getTimestampAsSqlTimestamp("c_since"));
					statements[1].setString(14, row.getString("c_credit"));
					statements[1].setLong(15, (long) row.get("c_credit_lim", VoltType.BIGINT));
					statements[1].setBigDecimal(	16, row.getDecimalAsBigDecimal("c_discount").setScale(4, BigDecimal.ROUND_HALF_DOWN));
					statements[1].setBigDecimal(17, row.getDecimalAsBigDecimal("c_balance").setScale(12, BigDecimal.ROUND_HALF_DOWN));
					statements[1].setBigDecimal(18, row.getDecimalAsBigDecimal("c_ytd_payment").setScale(12, BigDecimal.ROUND_HALF_DOWN));
					statements[1].setShort(19, (short) row.get("c_payment_cnt", VoltType.SMALLINT));
					statements[1].setShort(20, (short) row.get("c_delivery_cnt", VoltType.SMALLINT));
					statements[1].setString(21, row.getString("c_data"));
//					statements[1].addBatch();
					try{
						statements[1].execute();
						conn.commit();
					}catch(Exception e){}
				}
//				if(result.getRowCount() > 0) {
//					statements[1].executeBatch();
//				}
			}
			voltdbConn.callProcedure("@AdHoc", "DELETE FROM customer" + volumnId + " WHERE tenant_id = " + tenantId);
		} catch (IOException | ProcCallException | SQLException e) {
			e.printStackTrace();
//			System.out.println("************"+row.get("c_d_id", VoltType.TINYINT)+"************");
		}
//		System.out.println("\n customer: " + tenantId + " truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
		
	}

}
