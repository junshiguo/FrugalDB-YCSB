package frugaldb.server.loader.retriver;

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

public class OrderLineRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int tenantId;
	public int volumnId;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public OrderLineRetriver(String url, String username, String password, String voltdbServer, int tenantId, int volumnId){
		this.url = url;
		this.username = username;
		this.password = password;
		this.voltdbServer = voltdbServer;
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}
	
	@Override
	public void run(){
		try {
			conn = DBManager.connectDB(url, username, password);
			stmt = conn.createStatement();
			conn.setAutoCommit(false);
			statements = new PreparedStatement[2];
			statements[0] = conn.prepareStatement("UPDATE order_line"+tenantId+" SET ol_o_id = ?, ol_d_id = ?,ol_w_id = ?,ol_number = ?,ol_i_id = ?, ol_supply_w_id = ?,ol_delivery_d = ?, ol_quantity = ?, ol_amount = ?, ol_dist_info = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?");
			statements[1] = conn.prepareStatement("INSERT INTO order_line"+tenantId+" VALUES (?,?,?,?,?,?,?,?,?,?)");
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
			try{
				response = voltdbConn.callProcedure("SelectOrderLine_"+volumnId, tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[0].setInt(1, (int) row.get("ol_o_id", VoltType.INTEGER));
						statements[0].setInt(2, (int) row.get("ol_d_id", VoltType.INTEGER));
						statements[0].setInt(3, (int) row.get("ol_w_id", VoltType.INTEGER));
						statements[0].setInt(4, (int) row.get("ol_number", VoltType.INTEGER));
						statements[0].setInt(5, (int) row.get("ol_i_id", VoltType.INTEGER));
						statements[0].setInt(6, (int) row.get("ol_supply_w_id", VoltType.INTEGER));
						statements[0].setTimestamp(7, row.getTimestampAsSqlTimestamp("ol_delivery_d"));
						statements[0].setInt(8, (int) row.get("ol_quantity", VoltType.INTEGER));
						statements[0].setBigDecimal(9, row.getDecimalAsBigDecimal("ol_amount").setScale(6, BigDecimal.ROUND_HALF_DOWN));
						statements[0].setString(10, row.getString("ol_dist_info"));
						statements[0].setInt(11, (int) row.get("ol_w_id", VoltType.INTEGER));
						statements[0].setInt(12, (int) row.get("ol_d_id", VoltType.INTEGER));
						statements[0].setInt(13, (int) row.get("ol_o_id", VoltType.INTEGER));
						statements[0].setInt(14, (int) row.get("ol_number", VoltType.INTEGER));
						statements[0].addBatch();
					}
					if(result.getRowCount() > 0) {
						statements[0].executeBatch();
						conn.commit();
					}
				}
				
				response = voltdbConn.callProcedure("SelectOrderLine_"+volumnId, tenantId, 1, 0);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[1].setInt(1, (int) row.get("ol_o_id", VoltType.INTEGER));
						statements[1].setInt(2, (int) row.get("ol_d_id", VoltType.INTEGER));
						statements[1].setInt(3, (int) row.get("ol_w_id", VoltType.INTEGER));
						statements[1].setInt(4, (int) row.get("ol_number", VoltType.INTEGER));
						statements[1].setInt(5, (int) row.get("ol_i_id", VoltType.INTEGER));
						statements[1].setInt(6, (int) row.get("ol_supply_w_id", VoltType.INTEGER));
						statements[1].setTimestamp(7, row.getTimestampAsSqlTimestamp("ol_delivery_d"));
						statements[1].setInt(8, (int) row.get("ol_quantity", VoltType.INTEGER));
						statements[1].setBigDecimal(9, row.getDecimalAsBigDecimal("ol_amount").setScale(6, BigDecimal.ROUND_HALF_DOWN));
						statements[1].setString(10, row.getString("ol_dist_info"));
//						statements[1].addBatch();
						try{
							statements[1].execute();
							conn.commit();
						}catch(Exception e){}
					}
//					if(result.getRowCount() > 0) {
//						statements[1].executeBatch();
//					}
				}
				
				response = voltdbConn.callProcedure("SelectOrderLine_"+volumnId, tenantId, 1, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[1].setInt(1, (int) row.get("ol_o_id", VoltType.INTEGER));
						statements[1].setInt(2, (int) row.get("ol_d_id", VoltType.INTEGER));
						statements[1].setInt(3, (int) row.get("ol_w_id", VoltType.INTEGER));
						statements[1].setInt(4, (int) row.get("ol_number", VoltType.INTEGER));
						statements[1].setInt(5, (int) row.get("ol_i_id", VoltType.INTEGER));
						statements[1].setInt(6, (int) row.get("ol_supply_w_id", VoltType.INTEGER));
						statements[1].setTimestamp(7, row.getTimestampAsSqlTimestamp("ol_delivery_d"));
						statements[1].setInt(8, (int) row.get("ol_quantity", VoltType.INTEGER));
						statements[1].setBigDecimal(9, row.getDecimalAsBigDecimal("ol_amount").setScale(6, BigDecimal.ROUND_HALF_DOWN));
						statements[1].setString(10, row.getString("ol_dist_info"));
//						statements[1].addBatch();
						try{
							statements[1].execute();
							conn.commit();
						}catch(Exception e){}
					}
//					if(result.getRowCount() > 0) {
//						statements[1].executeBatch();
//					}
				}
				voltdbConn.callProcedure("@AdHoc", "DELETE FROM order_line"+volumnId+" WHERE tenant_id = "+tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				e.printStackTrace();
			}
//		System.out.println("\n order_line: "+tenantId+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
				
	}

}
