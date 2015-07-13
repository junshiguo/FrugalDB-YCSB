package frugaldb.server.loader.offloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import frugaldb.server.loader.LoadConfig;
import frugaldb.server.loader.VMMatch;

public class Mysql2Voltdb {
	public Connection conn;
	public Client voltdbConn;
	public int tenantId;
	public int volumnId;
	
//	public static void main(String[] args){
//		new Mysql2Voltdb("jdbc:mysql://127.0.0.1/test", "remote", "remote", "127.0.0.1", 0).start();
//	}

	public Mysql2Voltdb(){}
	public Mysql2Voltdb(Connection conn, Client voltdbConn, int tenantId, int volumnId){
		this.conn = conn;
		this.voltdbConn = voltdbConn;
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}
	
	public void run(){
		int emptyVolumn = volumnId;
		if(emptyVolumn != -1){
//			conn = DBManager.checkMysqlConn(conn);
//			voltdbConn = DBManager.checkVoltdbConn(voltdbConn);
			long start = System.nanoTime();
			VMMatch.addMatch(emptyVolumn, tenantId);
			try {
				switch(LoadConfig.M2VMode){
				case 0:
					this.Mysql2VoltdbBulkProLine(tenantId, emptyVolumn);
					break;
				case 1:
					this.Mysql2VoltdbBulkPro(tenantId, emptyVolumn);
					break;
				case 2:
					this.Mysql2VoltdbBulk(tenantId, emptyVolumn);
					break;
				case 3:
					this.Mysql2VoltdbThreading(tenantId, emptyVolumn);
					break;
				case 4:
					this.Mysql2VoltdbBasic(tenantId, emptyVolumn);
					break;
					default:
				}
			} catch (InterruptedException | SQLException | IOException | ProcCallException e) {
				e.printStackTrace();
			}
			long end = System.nanoTime();
			DecimalFormat df = new DecimalFormat(".0000");
			System.out.println("Tenant "+tenantId+" MySQL ---> VoltDB! Time spent: "+df.format((end-start)/1000000000.0)+" seconds!");
			System.out.println(""+df.format(time1)+"\t"+df.format(time2)+"\t"+df.format(time3)+" ("+df.format(time5)+")\t"+df.format(time4));
		}
//		try {
//			conn.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
	}
	
	/**
	 *  export table into csv files, then execute insert in batch (self implemented VoltProcedure)
	 * @param tenantId
	 * @param volumnId
	 * @throws InterruptedException
	 */
	public void Mysql2VoltdbBulkPro(int tenantId, int volumnId) throws InterruptedException{
		OffloadInBulk[] loaders = new OffloadInBulk[9];
		for(int i = 0; i < 9; i++){
			loaders[i] = new OffloadInBulk(tenantId, volumnId, i, conn, voltdbConn);
			loaders[i].start();
		}
		for(int i = 0; i < 9; i++){
			loaders[i].join();
		}
	}
	/**
	 * currently in use. a linear version of Mysql2VoltdbBulkPro
	 * @param tenantId
	 * @param volumnId
	 */
	public double time1, time2, time3, time4, time5;
	public void Mysql2VoltdbBulkProLine(int tenantId, int volumnId){
		OffloadInBulk loader;
		time1 = time2= time3 = time4 = 0;
		for(int i = 0; i < 9; i++){
			loader = new OffloadInBulk(tenantId, volumnId, i, conn, voltdbConn);
			loader.load();
			time1 += loader.time1; //time to export data from mysql into csv files
			time2 += loader.time2; //time to empty voltdb table (in case of primary key exist error)
			time3 += loader.time3; //time to insert dsata into voltdb
			time4 += loader.time4; //time to delete tmp csv files
			time5 += loader.time5; //time to do voltdb transaction, included in time3
		}
	}
	
	/**
	 * use csvloader provided by voltdb. csvloader used for each table.
	 * @param tenantId
	 * @param volumnId
	 * @throws SQLException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ProcCallException
	 */
	public void Mysql2VoltdbBulk(int tenantId, int volumnId) throws SQLException, IOException, InterruptedException, ProcCallException{
		Statement stmt = conn.createStatement();
		String[] tables = {"customer", "district", "history", "item", "new_orders", "order_line", "orders", "stock", "warehouse"};
		String[] sql = new String[9];
		sql[0] = "select concat(c_id, ',', c_d_id, ',',c_w_id, ',', c_first, ',', c_middle, ',', c_last, ',', c_street_1, ',', c_street_2, ',',c_city, ',',c_state, ',',c_zip, ',', c_phone, ',',c_since, ',', c_credit, ',', c_credit_lim , ',', c_discount, ',', c_balance, ',', c_ytd_payment, ',',c_payment_cnt, ',', c_delivery_cnt, ',', c_data, ',','"+tenantId+"', ',', '0', ',', '0')"
				+ " from customer"+tenantId+" into outfile '"+LoadConfig.csvPath+"/customer"+tenantId+".csv'";
		sql[1] = "select concat(d_id, ',', d_w_id, ',', d_name, ',', d_street_1, ',', d_street_2, ',', d_city, ',', d_state, ',', d_zip, ',', d_tax, ',', d_ytd, ',', d_next_o_id, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from district"+tenantId+" into outfile '"+LoadConfig.csvPath+"/district"+tenantId+".csv'";
		sql[2] = "select concat(h_c_id , ',', h_c_d_id, ',', h_c_w_id, ',',h_d_id, ',',h_w_id, ',',h_date, ',',h_amount, ',',h_data, ',', '"+tenantId+"', ',', '0', ',', '0' ) "
				+ "from history"+tenantId+" into outfile '"+LoadConfig.csvPath+"/history"+tenantId+".csv'";
		sql[3] = "select concat(i_id, ',', i_im_id, ',', i_name, ',', i_price, ',', i_data, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from item"+tenantId+" into outfile '"+LoadConfig.csvPath+"/item"+tenantId+".csv'";
		sql[4] = "select concat(no_o_id, ',',no_d_id, ',',no_w_id, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from new_orders"+tenantId+" into outfile '"+LoadConfig.csvPath+"/new_orders"+tenantId+".csv'";
		sql[5] = "select concat(ol_o_id, ',', ol_d_id, ',',ol_w_id, ',',ol_number, ',',ol_i_id, ',', ol_supply_w_id, ',',ol_delivery_d, ',', ol_quantity, ',', ol_amount, ',', ol_dist_info, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from order_line"+tenantId+" into outfile '"+LoadConfig.csvPath+"/order_line"+tenantId+".csv'";
		sql[6] = "select concat(o_id, ',', o_d_id, ',', o_w_id, ',', o_c_id, ',', o_entry_d, ',', o_carrier_id, ',', o_ol_cnt, ',', o_all_local, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from orders"+tenantId+" into outfile '"+LoadConfig.csvPath+"/orders"+tenantId+".csv'";
		sql[7] = "select concat(s_i_id, ',', s_w_id, ',', s_quantity, ',', s_dist_01, ',', s_dist_02, ',',s_dist_03, ',',s_dist_04, ',', s_dist_05, ',', s_dist_06, ',', s_dist_07, ',', s_dist_08, ',', s_dist_09, ',', s_dist_10, ',', s_ytd, ',', s_order_cnt, ',', s_remote_cnt, ',',s_data, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from stock"+tenantId+" into outfile '"+LoadConfig.csvPath+"/stock"+tenantId+".csv'";
		sql[8] = "select concat(w_id, ',',	w_name, ',',w_street_1, ',',w_street_2, ',',w_city, ',',w_state, ',',w_zip, ',',w_tax, ',',	w_ytd, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from warehouse"+tenantId+" into outfile '"+LoadConfig.csvPath+"/warehouse"+tenantId+".csv'";
		Process[] pr = new Process[9];
		for(int i = 0; i < 9; i++){
			stmt.execute(sql[i]);
			voltdbConn.callProcedure("@AdHoc", "delete from "+tables[i]+volumnId+" where tenant_id = "+tenantId);
			pr[i] = Runtime.getRuntime().exec(getLoader(tables[i], tenantId, volumnId));
//			pr[i].waitFor();
		}
		for(int i = 0; i < 9; i++){
			pr[i].waitFor();
		}
		stmt.close();
		conn.close();
	}
	
	public String[] getLoader(String table, int tid, int vid){
		String[] ret = {"/bin/sh", "-c", "/usr/voltdb/bin/csvloader "+table+vid+" -f "+LoadConfig.csvPath+"/"+table+tid+".csv -r /tmp/hybrid/tmp"};
		return ret;
	}

	/**
	 * a threading version of Mysql2Voltdb()
	 * @param tenantId
	 * @param volumnId
	 */
	public void Mysql2VoltdbThreading(int tenantId, int volumnId){
		OffloadThreading[] offloading = new OffloadThreading[9];
		for(int i = 0; i < 9; i++){
			offloading[i] = new OffloadThreading(i, tenantId, volumnId);
			offloading[i].start();
		}
		try{
			for(int i = 0; i < 9; i++){
				offloading[i].join();
			}
		}catch(Exception e){}
	}
	
	/**
	 * select *  from mysql and insert into voltdb one line every time
	 * @param tenantId
	 * @param volumnId
	 * @throws SQLException
	 * @throws NoConnectionsException
	 * @throws IOException
	 * @throws ProcCallException
	 */
	public void Mysql2VoltdbBasic(int tenantId, int volumnId) throws SQLException, NoConnectionsException, IOException, ProcCallException{
		Statement stmt = conn.createStatement();
//		ClientResponse response = null;
		//********************load warehouse******************//
		ResultSet rs = stmt.executeQuery("SELECT  * FROM warehouse"+tenantId);
		while(rs.next()){
//			response = voltdbConn.callProcedure("WAREHOUSE"+volumnId+".select", rs.getInt("w_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
			try{
			voltdbConn.callProcedure("WAREHOUSE"+volumnId+".insert", rs.getInt("w_id"), 
					rs.getString("w_name"), rs.getString("w_street_1"), rs.getString("w_street_2"), rs.getString("w_city"), 
					rs.getString("w_state"), rs.getString("w_zip"), rs.getDouble("w_tax"), rs.getDouble("w_ytd"), 
					tenantId, 0, 0);
			}catch(Exception e){}
		}
		//*******************load district************************//
		rs = stmt.executeQuery("SELECT * FROM district"+tenantId);
		while(rs.next()){
//			response = voltdbConn.callProcedure("DISTRICT"+volumnId+".select", rs.getInt("d_w_id"), rs.getInt("d_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
			try{
			voltdbConn.callProcedure("DISTRICT"+volumnId+".insert", rs.getInt("d_id"), rs.getInt("d_w_id"), 
					rs.getString("d_name"), rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"), 
					rs.getString("d_state"), rs.getString("d_zip"),	rs.getDouble("d_tax"), rs.getDouble("d_ytd"), 
					rs.getInt("d_next_o_id"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//******************load customer**************************//
		rs = stmt.executeQuery("SELECT * FROM customer"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("CUSTOMER"+volumnId+".select", rs.getInt("c_id"), rs.getInt("c_w_id"), rs.getInt("c_d_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
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
		//******************load history*****************************//
		rs = stmt.executeQuery("SELECT * FROM history"+tenantId );
		while(rs.next()){ 
//			response = voltdbConn.callProcedure("HISTORY"+volumnId+".select", rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), rs.getInt("h_c_w_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
			try{
			voltdbConn.callProcedure("HISTORY"+volumnId+".insert", rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), 
					rs.getInt("h_c_w_id"), rs.getInt("h_d_id"), rs.getInt("h_w_id"), 
					rs.getTimestamp("h_date"), rs.getDouble("h_amount"), rs.getString("h_data"),
					tenantId, 0, 0);
			}catch(Exception e){}
		}
		//*******************load new orders*************************//
		rs = stmt.executeQuery("SELECT * FROM new_orders"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".select", rs.getInt("no_w_id"), rs.getInt("no_d_id"), rs.getInt("no_o_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}		
			try{
			voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".insert", rs.getInt("no_o_id"), rs.getInt("no_d_id"), 
					rs.getInt("no_w_id"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//******************load orders******************************//
		rs = stmt.executeQuery("SELECT * FROM orders"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("ORDERS"+volumnId+".select", rs.getInt("o_w_id"), rs.getInt("o_d_id"), rs.getInt("o_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}	
			try{
			voltdbConn.callProcedure("ORDERS"+volumnId+".insert", rs.getInt("o_id"), rs.getInt("o_d_id"),
					rs.getInt("o_w_id"), rs.getInt("o_c_id"),
					rs.getTimestamp("o_entry_d"), 
					rs.getInt("o_carrier_id"), rs.getInt("o_ol_cnt"), rs.getInt("o_all_local"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//***********************load order line********************//
		rs = stmt.executeQuery("SELECT * FROM order_line"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("ORDER_LINE"+volumnId+".select", rs.getInt("ol_w_id"), rs.getInt("ol_d_id"), rs.getInt("ol_o_id"), rs.getInt("ol_number"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}		
			try{
			voltdbConn.callProcedure("ORDER_LINE"+volumnId+".insert", rs.getInt("ol_o_id"), rs.getInt("ol_d_id"), 
					rs.getInt("ol_w_id"), rs.getInt("ol_number"), rs.getInt("ol_i_id"), rs.getInt("ol_supply_w_id"),
					rs.getTimestamp("ol_delivery_d"), 
					rs.getInt("ol_quantity"), rs.getDouble("ol_amount"), rs.getString("ol_dist_info"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//********************load item***************************//
		rs = stmt.executeQuery("SELECT * FROM item"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("ITEM"+volumnId+".select", rs.getInt("i_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}	
			try{
			voltdbConn.callProcedure("ITEM"+volumnId+".insert", rs.getInt("i_id"), rs.getInt("i_im_id"), 
					rs.getString("i_name"), rs.getDouble("i_price"), rs.getString("i_data"), 
					tenantId, 0, 0);
			}catch(Exception e){}
		}
		//*********************load stock*************************//
		rs = stmt.executeQuery("SELECT * FROM stock"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("STOCK"+volumnId+".select", rs.getInt("s_w_id"), rs.getInt("s_i_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}		
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
