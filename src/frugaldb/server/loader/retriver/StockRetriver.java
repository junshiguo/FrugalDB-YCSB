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

public class StockRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int tenantId;
	public int volumnId;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public StockRetriver(String url, String username, String password, String voltdbServer, int tenantId, int volumnId){
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
			statements[0] = conn.prepareStatement("UPDATE stock"+tenantId+" SET s_i_id = ?, s_w_id = ?, s_quantity = ?, s_dist_01 = ?, s_dist_02 = ?,s_dist_03 = ?,s_dist_04 = ?, s_dist_05 = ?, s_dist_06 = ?, s_dist_07 = ?, s_dist_08 = ?, s_dist_09 = ?, s_dist_10 = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?,s_data = ? WHERE s_w_id = ? AND s_i_id = ?");
			statements[1] = conn.prepareStatement("INSERT INTO stock"+tenantId+" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
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
				response = voltdbConn.callProcedure("SelectStock_"+volumnId, tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[0].setInt(1, (int) row.get("s_i_id", VoltType.INTEGER));
						statements[0].setInt(2, (int) row.get("s_w_id", VoltType.INTEGER));
						statements[0].setInt(3, (int) row.get("s_quantity", VoltType.INTEGER));
						statements[0].setString(4, row.getString("s_dist_01"));
						statements[0].setString(5, row.getString("s_dist_02"));
						statements[0].setString(6, row.getString("s_dist_03"));
						statements[0].setString(7, row.getString("s_dist_04"));
						statements[0].setString(8, row.getString("s_dist_05"));
						statements[0].setString(9, row.getString("s_dist_06"));
						statements[0].setString(10, row.getString("s_dist_07"));
						statements[0].setString(11, row.getString("s_dist_08"));
						statements[0].setString(12, row.getString("s_dist_09"));
						statements[0].setString(13, row.getString("s_dist_10"));
						statements[0].setBigDecimal(14, row.getDecimalAsBigDecimal("s_ytd").setScale(8, BigDecimal.ROUND_HALF_DOWN));
						statements[0].setInt(15, (int) row.get("s_order_cnt", VoltType.INTEGER));
						statements[0].setInt(16, (int) row.get("s_remote_cnt", VoltType.INTEGER));
						statements[0].setString(17, row.getString("s_data"));
						statements[0].setInt(18, (int) row.get("s_w_id", VoltType.INTEGER));
						statements[0].setInt(19, (int) row.get("s_i_id", VoltType.INTEGER));
						statements[0].addBatch();
					}
					if(result.getRowCount() > 0) {
						statements[0].executeBatch();
						conn.commit();
					}
				}
				
				response = voltdbConn.callProcedure("SelectStock_"+volumnId, tenantId, 1, 0);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[1].setInt(1, (int) row.get("s_i_id", VoltType.INTEGER));
						statements[1].setInt(2, (int) row.get("s_w_id", VoltType.INTEGER));
						statements[1].setInt(3, (int) row.get("s_quantity", VoltType.INTEGER));
						statements[1].setString(4, row.getString("s_dist_01"));
						statements[1].setString(5, row.getString("s_dist_02"));
						statements[1].setString(6, row.getString("s_dist_03"));
						statements[1].setString(7, row.getString("s_dist_04"));
						statements[1].setString(8, row.getString("s_dist_05"));
						statements[1].setString(9, row.getString("s_dist_06"));
						statements[1].setString(10, row.getString("s_dist_07"));
						statements[1].setString(11, row.getString("s_dist_08"));
						statements[1].setString(12, row.getString("s_dist_09"));
						statements[1].setString(13, row.getString("s_dist_10"));
						statements[1].setBigDecimal(14, row.getDecimalAsBigDecimal("s_ytd").setScale(8, BigDecimal.ROUND_HALF_DOWN));
						statements[1].setInt(15, (int) row.get("s_order_cnt", VoltType.INTEGER));
						statements[1].setInt(16, (int) row.get("s_remote_cnt", VoltType.INTEGER));
						statements[1].setString(17, row.getString("s_data"));
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
				
				response = voltdbConn.callProcedure("SelectStock_"+volumnId, tenantId, 1, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[1].setInt(1, (int) row.get("s_i_id", VoltType.INTEGER));
						statements[1].setInt(2, (int) row.get("s_w_id", VoltType.INTEGER));
						statements[1].setInt(3, (int) row.get("s_quantity", VoltType.INTEGER));
						statements[1].setString(4, row.getString("s_dist_01"));
						statements[1].setString(5, row.getString("s_dist_02"));
						statements[1].setString(6, row.getString("s_dist_03"));
						statements[1].setString(7, row.getString("s_dist_04"));
						statements[1].setString(8, row.getString("s_dist_05"));
						statements[1].setString(9, row.getString("s_dist_06"));
						statements[1].setString(10, row.getString("s_dist_07"));
						statements[1].setString(11, row.getString("s_dist_08"));
						statements[1].setString(12, row.getString("s_dist_09"));
						statements[1].setString(13, row.getString("s_dist_10"));
						statements[1].setBigDecimal(14, row.getDecimalAsBigDecimal("s_ytd").setScale(8, BigDecimal.ROUND_HALF_DOWN));
						statements[1].setInt(15, (int) row.get("s_order_cnt", VoltType.INTEGER));
						statements[1].setInt(16, (int) row.get("s_remote_cnt", VoltType.INTEGER));
						statements[1].setString(17, row.getString("s_data"));
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
				voltdbConn.callProcedure("@AdHoc", "DELETE FROM stock"+volumnId+" WHERE tenant_id = "+tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				e.printStackTrace();
			}
//		System.out.println("\n stock: "+tenantId+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
				
	}

}
