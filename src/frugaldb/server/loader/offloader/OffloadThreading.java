package frugaldb.server.loader.offloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import frugaldb.server.loader.utility.DBManager;

/**
 * some earlier implementation of offload strategy.
 * currently not used.
 * @author guojunshi
 *
 */
public class OffloadThreading extends Thread {
	public int id;
	public int tenantId;
	public int volumnId;
	public Connection conn;
	public Client voltdbConn;
	
	public OffloadThreading(int id, int tenantId, int volumnId){
		this.id = id;
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}
	
	public void run(){
		conn = DBManager.checkMysqlConn(conn);
		voltdbConn = DBManager.checkVoltdbConn(voltdbConn);
		try {
			switch(this.id){
			case 0:
				offloading0();
				break;
			case 1:
				offloading1();
				break;
			case 2:
				offloading2();
				break;
			case 3:
				offloading3();
				break;
			case 4:
				offloading4();
				break;
			case 5:
				offloading5();
				break;
			case 6:
				offloading6();
				break;
			case 7:
				offloading7();
				break;
			case 8:
				offloading8();
				default:
			}
			conn.close();
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
	public void offloading0() throws SQLException, NoConnectionsException, IOException, ProcCallException{
			Statement stmt = conn.createStatement();
//			ClientResponse response = null;
			//********************load warehouse******************//
			ResultSet rs = stmt.executeQuery("SELECT  * FROM warehouse"+tenantId);
			while(rs.next()){
//				response = voltdbConn.callProcedure("WAREHOUSE"+volumnId+".select", rs.getInt("w_id"), tenantId);
//				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//					continue;
//				}
				try{
				voltdbConn.callProcedure("WAREHOUSE"+volumnId+".insert", rs.getInt("w_id"), 
						rs.getString("w_name"), rs.getString("w_street_1"), rs.getString("w_street_2"), rs.getString("w_city"), 
						rs.getString("w_state"), rs.getString("w_zip"), rs.getDouble("w_tax"), rs.getDouble("w_ytd"), 
						tenantId, 0, 0);
				}catch(Exception e){}
			}
	}
	public void offloading1() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
			//*******************load district************************//
		ResultSet rs = stmt.executeQuery("SELECT * FROM district"+tenantId);
			while(rs.next()){
//				response = voltdbConn.callProcedure("DISTRICT"+volumnId+".select", rs.getInt("d_w_id"), rs.getInt("d_id"), tenantId);
//				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//					continue;
//				}
				try{
				voltdbConn.callProcedure("DISTRICT"+volumnId+".insert", rs.getInt("d_id"), rs.getInt("d_w_id"), 
						rs.getString("d_name"), rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"), 
						rs.getString("d_state"), rs.getString("d_zip"),	rs.getDouble("d_tax"), rs.getDouble("d_ytd"), 
						rs.getInt("d_next_o_id"), tenantId, 0, 0);
				}catch(Exception e){}
			}
	}
	public void offloading2() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
			//******************load customer**************************//
			ResultSet rs = stmt.executeQuery("SELECT * FROM customer"+tenantId );
			while(rs.next()){
//				response = voltdbConn.callProcedure("CUSTOMER"+volumnId+".select", rs.getInt("c_id"), rs.getInt("c_w_id"), rs.getInt("c_d_id"), tenantId);
//				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//					continue;
//				}
				try{
				voltdbConn.callProcedure("CUSTOMER"+volumnId+".insert", rs.getInt("c_id"), rs.getInt("c_d_id"), 
						rs.getInt("c_w_id"), rs.getString("c_first"), rs.getString("c_middle"), rs.getString("c_last"),
						rs.getString("c_street_1"), rs.getString("c_street_2"), rs.getString("c_city"),rs.getString("c_state"),
						rs.getString("c_zip"), rs.getString("c_phone"), rs.getTimestamp("c_since"), 
						rs.getString("c_credit"), rs.getInt("c_credit_lim"), rs.getDouble("c_discount"), rs.getDouble("c_balance"), 
						rs.getDouble("c_ytd_payment"), rs.getInt("c_payment_cnt"), rs.getInt("c_delivery_cnt"), rs.getString("c_data"),
						tenantId, 0, 0);
				}catch(Exception e){}
			}
	}

	public void offloading3() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
//		ClientResponse response = null;
		ResultSet rs;
		//******************load history*****************************//
				rs = stmt.executeQuery("SELECT * FROM history"+tenantId );
				while(rs.next()){ 
//					response = voltdbConn.callProcedure("HISTORY"+volumnId+".select", rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), rs.getInt("h_c_w_id"), tenantId);
//					if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//						continue;
//					}
					try{
					voltdbConn.callProcedure("HISTORY"+volumnId+".insert", rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), 
							rs.getInt("h_c_w_id"), rs.getInt("h_d_id"), rs.getInt("h_w_id"), 
							rs.getTimestamp("h_date"), rs.getDouble("h_amount"), rs.getString("h_data"),
							tenantId, 0, 0);
					}catch(Exception e){}
				}
	}
	public void offloading4() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
		ResultSet rs;
				//*******************load new orders*************************//
				rs = stmt.executeQuery("SELECT * FROM new_orders"+tenantId );
				while(rs.next()){
//					response = voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".select", rs.getInt("no_w_id"), rs.getInt("no_d_id"), rs.getInt("no_o_id"), tenantId);
//					if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//						continue;
//					}		
					try{
					voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".insert", rs.getInt("no_o_id"), rs.getInt("no_d_id"), 
							rs.getInt("no_w_id"), tenantId, 0, 0);
					}catch(Exception e){}
				}
	}
	public void offloading5() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
		ResultSet rs;
				//******************load orders******************************//
				rs = stmt.executeQuery("SELECT * FROM orders"+tenantId );
				while(rs.next()){
//					response = voltdbConn.callProcedure("ORDERS"+volumnId+".select", rs.getInt("o_w_id"), rs.getInt("o_d_id"), rs.getInt("o_id"), tenantId);
//					if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//						continue;
//					}	
					try{
					voltdbConn.callProcedure("ORDERS"+volumnId+".insert", rs.getInt("o_id"), rs.getInt("o_d_id"),
							rs.getInt("o_w_id"), rs.getInt("o_c_id"),
							rs.getTimestamp("o_entry_d"), 
							rs.getInt("o_carrier_id"), rs.getInt("o_ol_cnt"), rs.getInt("o_all_local"), tenantId, 0, 0);
					}catch(Exception e){}
				}
	}
	
	public void offloading6() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
//		ClientResponse response = null;
		ResultSet rs;
		//***********************load order line********************//
				rs = stmt.executeQuery("SELECT * FROM order_line"+tenantId );
				while(rs.next()){
//					response = voltdbConn.callProcedure("ORDER_LINE"+volumnId+".select", rs.getInt("ol_w_id"), rs.getInt("ol_d_id"), rs.getInt("ol_o_id"), rs.getInt("ol_number"), tenantId);
//					if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//						continue;
//					}		
					try{
					voltdbConn.callProcedure("ORDER_LINE"+volumnId+".insert", rs.getInt("ol_o_id"), rs.getInt("ol_d_id"), 
							rs.getInt("ol_w_id"), rs.getInt("ol_number"), rs.getInt("ol_i_id"), rs.getInt("ol_supply_w_id"),
							rs.getTimestamp("ol_delivery_d"), 
							rs.getInt("ol_quantity"), rs.getDouble("ol_amount"), rs.getString("ol_dist_info"), tenantId, 0, 0);
					}catch(Exception e){}
				}
	}
	public void offloading7() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
		ResultSet rs;
				//********************load item***************************//
				rs = stmt.executeQuery("SELECT * FROM item"+tenantId );
				while(rs.next()){
//					response = voltdbConn.callProcedure("ITEM"+volumnId+".select", rs.getInt("i_id"), tenantId);
//					if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//						continue;
//					}		
					try{
					voltdbConn.callProcedure("ITEM"+volumnId+".insert", rs.getInt("i_id"), rs.getInt("i_im_id"), 
							rs.getString("i_name"), rs.getDouble("i_price"), rs.getString("i_data"), 
							tenantId, 0, 0);
					}catch(Exception e){}
				}
	}
	public void offloading8() throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
		ResultSet rs;
				//*********************load stock*************************//
				rs = stmt.executeQuery("SELECT * FROM stock"+tenantId );
				while(rs.next()){
//					response = voltdbConn.callProcedure("STOCK"+volumnId+".select", rs.getInt("s_w_id"), rs.getInt("s_i_id"), tenantId);
//					if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//						continue;
//					}		
					try{
					voltdbConn.callProcedure("STOCK"+volumnId+".insert", rs.getInt("s_i_id"), rs.getInt("s_w_id"),
							rs.getInt("s_quantity"), rs.getString("s_dist_01"), rs.getString("s_dist_02"), rs.getString("s_dist_03"), 
							rs.getString("s_dist_04"), rs.getString("s_dist_05"), rs.getString("s_dist_06"), rs.getString("s_dist_07"), 
							rs.getString("s_dist_08"), rs.getString("s_dist_09"), rs.getString("s_dist_10"), rs.getDouble("s_ytd"), 
							rs.getInt("s_order_cnt"), rs.getInt("s_remote_cnt"), rs.getString("s_data"), tenantId, 0, 0);
					}catch(Exception e){}
				}
	}
	
}
