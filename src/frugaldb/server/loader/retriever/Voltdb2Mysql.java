package frugaldb.server.loader.retriever;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class Voltdb2Mysql {
	
	public int tenantId, volumnId;
	public Connection conn;
	public Client client;
	
	public Voltdb2Mysql(int tenantId, int volumnId, Connection conn, Client client){
		this.tenantId = tenantId;
		this.volumnId = volumnId;
		this.conn = conn;
		this.client = client;
	}
	
	public void run(){
		try {
			conn.setAutoCommit(false);
			PreparedStatement[] statements = new PreparedStatement[2];
			statements[0] = conn.prepareStatement("UPDATE usertable"+tenantId+" SET field0=?, field1=?, field2=?, field3=?, field4=?, field5=?, field6=?, field7=?, field8=?, field9=? "
					+ "WHERE ycsb_key=?");
			statements[1] = conn.prepareStatement("INSERT INTO usertable"+tenantId+" VALUES(?,?,?,?,?,?,?,?,?,?,?)");
			
			ClientResponse response;
			VoltTable result;
			VoltTableRow row;
			response = client.callProcedure("SelectUsertable"+volumnId, tenantId, 0, 1);
			if (response.getStatus() == ClientResponse.SUCCESS	&& response.getResults()[0].getRowCount() != 0) {
				result = response.getResults()[0];
				for (int i = 0; i < result.getRowCount(); i++) {
					row = result.fetchRow(i);
					for(int j = 0; j < 10; j++){
						statements[0].setString(i+1, row.getString("field"+i));
					}
					statements[0].setString(11, row.getString("ycsb_key"));
					statements[0].addBatch();
				}
				if(result.getRowCount() > 0){
					statements[0].executeBatch();
					conn.commit();
				}
			}
			
			response = client.callProcedure("SelectUsertable"+volumnId, tenantId, 1, 0);
			if (response.getStatus() == ClientResponse.SUCCESS	&& response.getResults()[0].getRowCount() != 0) {
				result = response.getResults()[0];
				for (int i = 0; i < result.getRowCount(); i++) {
					row = result.fetchRow(i);
					for(int j = 0; j < 10; j++){
						statements[1].setString(i+2, row.getString("field"+i));
					}
					statements[1].setString(1, row.getString("ycsb_key"));
					statements[1].addBatch();
				}
				if(result.getRowCount() > 0){
					statements[1].executeBatch();
					conn.commit();
				}
			}
			
			response = client.callProcedure("SelectUsertable"+volumnId, tenantId, 1, 1);
			if (response.getStatus() == ClientResponse.SUCCESS	&& response.getResults()[0].getRowCount() != 0) {
				result = response.getResults()[0];
				for (int i = 0; i < result.getRowCount(); i++) {
					row = result.fetchRow(i);
					for(int j = 0; j < 10; j++){
						statements[1].setString(i+2, row.getString("field"+i));
					}
					statements[1].setString(1, row.getString("ycsb_key"));
					statements[1].addBatch();
				}
				if(result.getRowCount() > 0){
					statements[1].executeBatch();
					conn.commit();
				}
			}
			
			client.callProcedure("@AdHoc", "DELETE FROM usertable"+volumnId+" WHERE tenant_id = "+tenantId);
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
}
