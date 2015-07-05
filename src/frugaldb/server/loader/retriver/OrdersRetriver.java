package frugaldb.server.loader.retriver;

import java.io.IOException;
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

public class OrdersRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int tenantId;
	public int volumnId;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public OrdersRetriver(String url, String username, String password, String voltdbServer, int tenantId, int volumnId){
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
			statements[0] = conn.prepareStatement("UPDATE orders"+tenantId+" SET o_id = ?, o_d_id = ?, o_w_id = ?,o_c_id = ?,o_entry_d = ?,o_carrier_id = ?,o_ol_cnt = ?, o_all_local = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
			statements[1] = conn.prepareStatement("INSERT INTO orders"+tenantId+" VALUES (?,?,?,?,?,?,?,?)");
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
				response = voltdbConn.callProcedure("SelectOrders_"+volumnId, tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[0].setInt(1, (int) row.get("o_id", VoltType.INTEGER));
						statements[0].setInt(2, (int) row.get("o_d_id", VoltType.INTEGER));
						statements[0].setInt(3, (int) row.get("o_w_id", VoltType.INTEGER));
						statements[0].setInt(4, (int) row.get("o_c_id", VoltType.INTEGER));
						statements[0].setTimestamp(5, row.getTimestampAsSqlTimestamp("o_entry_d"));
						statements[0].setInt(6, (int) row.get("o_carrier_id", VoltType.INTEGER));
						statements[0].setInt(7, (int) row.get("o_ol_cnt", VoltType.INTEGER));
						statements[0].setInt(8, (int) row.get("o_all_local", VoltType.INTEGER));
						statements[0].setInt(9, (int) row.get("o_w_id", VoltType.INTEGER));
						statements[0].setInt(10, (int) row.get("o_d_id", VoltType.INTEGER));
						statements[0].setInt(11, (int) row.get("o_id", VoltType.INTEGER));
						statements[0].addBatch();
					}
					if(result.getRowCount() > 0) {
						statements[0].executeBatch();
						conn.commit();
					}
				}
				
				response = voltdbConn.callProcedure("SelectOrders_"+volumnId, tenantId, 1, 0);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[1].setInt(1, (int) row.get("o_id", VoltType.INTEGER));
						statements[1].setInt(2, (int) row.get("o_d_id", VoltType.INTEGER));
						statements[1].setInt(3, (int) row.get("o_w_id", VoltType.INTEGER));
						statements[1].setInt(4, (int) row.get("o_c_id", VoltType.INTEGER));
						statements[1].setTimestamp(5, row.getTimestampAsSqlTimestamp("o_entry_d"));
						statements[1].setInt(6, (int) row.get("o_carrier_id", VoltType.INTEGER));
						statements[1].setInt(7, (int) row.get("o_ol_cnt", VoltType.INTEGER));
						statements[1].setInt(8, (int) row.get("o_all_local", VoltType.INTEGER));
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
				
				response = voltdbConn.callProcedure("SelectOrders_"+volumnId, tenantId, 1, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[1].setInt(1, (int) row.get("o_id", VoltType.INTEGER));
						statements[1].setInt(2, (int) row.get("o_d_id", VoltType.INTEGER));
						statements[1].setInt(3, (int) row.get("o_w_id", VoltType.INTEGER));
						statements[1].setInt(4, (int) row.get("o_c_id", VoltType.INTEGER));
						statements[1].setTimestamp(5, row.getTimestampAsSqlTimestamp("o_entry_d"));
						statements[1].setInt(6, (int) row.get("o_carrier_id", VoltType.INTEGER));
						statements[1].setInt(7, (int) row.get("o_ol_cnt", VoltType.INTEGER));
						statements[1].setInt(8, (int) row.get("o_all_local", VoltType.INTEGER));
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
				voltdbConn.callProcedure("@AdHoc", "DELETE FROM orders"+volumnId+" WHERE tenant_id = "+tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				e.printStackTrace();
			}
//		System.out.println("\n orders: "+tenantId+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
				
	}

}
