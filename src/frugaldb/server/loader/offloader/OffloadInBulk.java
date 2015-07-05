package frugaldb.server.loader.offloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import frugaldb.server.loader.LoadConfig;

import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

import frugaldb.server.loader.utility.DBManager;

public class OffloadInBulk extends Thread {
	public int tenantId;
	public int volumnId;
	public int tableId;
	public Connection conn;
	public Client client;
	public Statement stmt = null;
	public double time2, time1, time3, time4, time5 = 0;
	
	public OffloadInBulk(int tenantId, int volumnId, int tableId, Connection conn, Client client){
		this.tenantId = tenantId;
		this.volumnId = volumnId;
		this.tableId = tableId;
		this.conn = conn;
		this.client = client;
	}
	
	public final static int CUSTOMER = 0;
	public final static int DISTRICT = 1;
	public final static int HISTORY = 2;
	public final static int ITEM = 3;
	public final static int NEW_ORDERS = 4;
	public final static int ORDER_LINE = 5;
	public final static int ORDERS = 6;
	public final static int STOCK = 7;
	public final static int WAREHOUSE = 8;
	public static final String[] tables = {"customer", "district", "history", "item", "new_orders", "order_line", "orders", "stock", "warehouse"};
	public static final String[] Tables = {"Customer", "District", "History", "Item", "NewOrders", "OrderLine", "Orders", "Stock", "Warehouse"};
	public String getSQL(int tableId){
		String ret = null;
		switch(tableId){
		case CUSTOMER:
			ret = "select concat(c_id, ',', c_d_id, ',',c_w_id, ',', c_first, ',', c_middle, ',', c_last, ',', c_street_1, ',', c_street_2, ',',c_city, ',',c_state, ',',c_zip, ',', c_phone, ',',c_since, ',', c_credit, ',', c_credit_lim , ',', c_discount, ',', c_balance, ',', c_ytd_payment, ',',c_payment_cnt, ',', c_delivery_cnt, ',', c_data, ',','"+tenantId+"', ',', '0', ',', '0')"
					+ " from customer"+tenantId+" into outfile '"+LoadConfig.csvPath+"/customer"+tenantId+".csv'";
			break;
		case DISTRICT:
			ret = "select concat(d_id, ',', d_w_id, ',', d_name, ',', d_street_1, ',', d_street_2, ',', d_city, ',', d_state, ',', d_zip, ',', d_tax, ',', d_ytd, ',', d_next_o_id, ',', '"+tenantId+"', ',', '0', ',', '0') "
					+ "from district"+tenantId+" into outfile '"+LoadConfig.csvPath+"/district"+tenantId+".csv'";
			break;
		case HISTORY:
			ret = "select concat(h_c_id , ',', h_c_d_id, ',', h_c_w_id, ',',h_d_id, ',',h_w_id, ',',h_date, ',',h_amount, ',',h_data, ',', '"+tenantId+"', ',', '0', ',', '0' ) "
					+ "from history"+tenantId+" into outfile '"+LoadConfig.csvPath+"/history"+tenantId+".csv'";
			break;
		case ITEM:
			ret = "select concat(i_id, ',', i_im_id, ',', i_name, ',', i_price, ',', i_data, ',', '"+tenantId+"', ',', '0', ',', '0') "
					+ "from item"+tenantId+" into outfile '"+LoadConfig.csvPath+"/item"+tenantId+".csv'";
			break;
		case NEW_ORDERS:
			ret = "select concat(no_o_id, ',',no_d_id, ',',no_w_id, ',', '"+tenantId+"', ',', '0', ',', '0') "
					+ "from new_orders"+tenantId+" into outfile '"+LoadConfig.csvPath+"/new_orders"+tenantId+".csv'";
			break;
		case ORDER_LINE:
			ret = "select concat(ol_o_id, ',', ol_d_id, ',',ol_w_id, ',',ol_number, ',',ol_i_id, ',', ol_supply_w_id, ',',ol_delivery_d, ',', ol_quantity, ',', ol_amount, ',', ol_dist_info, ',', '"+tenantId+"', ',', '0', ',', '0') "
					+ "from order_line"+tenantId+" into outfile '"+LoadConfig.csvPath+"/order_line"+tenantId+".csv'";
			break;
		case ORDERS:
			ret = "select concat(o_id, ',', o_d_id, ',', o_w_id, ',', o_c_id, ',', o_entry_d, ',', o_carrier_id, ',', o_ol_cnt, ',', o_all_local, ',', '"+tenantId+"', ',', '0', ',', '0') "
					+ "from orders"+tenantId+" into outfile '"+LoadConfig.csvPath+"/orders"+tenantId+".csv'";
			break;
		case STOCK:
			ret = "select concat(s_i_id, ',', s_w_id, ',', s_quantity, ',', s_dist_01, ',', s_dist_02, ',',s_dist_03, ',',s_dist_04, ',', s_dist_05, ',', s_dist_06, ',', s_dist_07, ',', s_dist_08, ',', s_dist_09, ',', s_dist_10, ',', s_ytd, ',', s_order_cnt, ',', s_remote_cnt, ',',s_data, ',', '"+tenantId+"', ',', '0', ',', '0') "
					+ "from stock"+tenantId+" into outfile '"+LoadConfig.csvPath+"/stock"+tenantId+".csv'";
			break;
		case WAREHOUSE:
			ret  = "select concat(w_id, ',',	w_name, ',',w_street_1, ',',w_street_2, ',',w_city, ',',w_state, ',',w_zip, ',',w_tax, ',',	w_ytd, ',', '"+tenantId+"', ',', '0', ',', '0') "
					+ "from warehouse"+tenantId+" into outfile '"+LoadConfig.csvPath+"/warehouse"+tenantId+".csv'";
			break;
			default:
		}
		return ret;
	}
	
	/**
	 * when run() is called, the thread will create its own connection to mysql and voltdb
	 */
	public void run(){
		try {
			conn = DBManager.checkMysqlConn(null);
			client = DBManager.checkVoltdbConn(null);
			stmt = conn.createStatement();
			
			//delete file before export to csv
//			File file = new File(LoadConfig.csvPath+"/"+tables[tableId]+tenantId+".csv");
//			file.delete();
			
			long start = System.nanoTime(); /////
			stmt.execute(getSQL(tableId));
			long end1 = System.nanoTime(); /////
//			client.callProcedure("@AdHoc", "delete from "+tables[tableId]+volumnId+" where tenant_id = "+tenantId);
			long end2 = System.nanoTime(); /////
			
			FileReader filereader = new FileReader(LoadConfig.csvPath+"/"+tables[tableId]+tenantId+".csv");
			BufferedReader reader = new BufferedReader(filereader);
			String[] lines = new String[LoadConfig.batch]; 
			String str;
			int count = 0;
			long ts, te, tsum = 0;
			while((str = reader.readLine()) != null){
				lines[count] = str;
				count++;
				ts = System.nanoTime();
				if(count == LoadConfig.batch){
					client.callProcedure("Offload"+Tables[tableId]+"_"+volumnId, tenantId, lines, count);
					count = 0;
				}
				te = System.nanoTime();
				tsum += te - ts;
			}
			if(count > 0)
				client.callProcedure("Offload"+Tables[tableId]+"_"+volumnId, tenantId, lines, count);
			filereader.close();
			reader.close();
			stmt.close();
			conn.close();
			long end3 = System.nanoTime();
//			File file = new File(LoadConfig.csvPath+"/"+tables[tableId]+tenantId+".csv");
//			file.delete();
			long end4 = System.nanoTime(); /////
			time1 = (end1 - start) / 1000000000.0;
			time2 = (end2 - end1) / 1000000000.0;
			time3 = (end3 - end2) / 1000000000.0;
			time4 = (end4 - end3) / 1000000000.0;
			time5 = tsum / 1000000000.0;
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * currently used version
	 * when load() is called, the method will use connection passed down from OffloadThread.
	 * used when linear offload is needed
	 */
	public void load(){
		try {
			stmt = conn.createStatement();
			

			//delete file before export to csv
//			File file = new File(LoadConfig.csvPath+"/"+tables[tableId]+tenantId+".csv");
//			file.delete();
			
			long start = System.nanoTime(); /////
			stmt.execute(getSQL(tableId));
			long end1 = System.nanoTime(); /////
			client.callProcedure("@AdHoc", "delete from "+tables[tableId]+volumnId+" where tenant_id = "+tenantId);
			long end2 = System.nanoTime(); /////
			
			FileReader filereader = new FileReader(LoadConfig.csvPath+"/"+tables[tableId]+tenantId+".csv");
			BufferedReader reader = new BufferedReader(filereader);
			String[] lines = new String[LoadConfig.batch]; 
			String str;
			int count = 0;
			long ts, te, tsum = 0;
			while((str = reader.readLine()) != null){
				lines[count] = str;
				count++;
				ts = System.nanoTime();
				if(count == LoadConfig.batch){
					client.callProcedure("Offload"+Tables[tableId]+"_"+volumnId, tenantId, lines, count);
					count = 0;
				}
				te = System.nanoTime();
				tsum += te - ts;
			}
			if(count > 0)
				client.callProcedure("Offload"+Tables[tableId]+"_"+volumnId, tenantId, lines, count);
			filereader.close();
			reader.close();
//			stmt.close();
//			conn.close();
			long end3 = System.nanoTime(); /////
//			File file = new File(LoadConfig.csvPath+"/"+tables[tableId]+tenantId+".csv");
//			file.delete();
			long end4 = System.nanoTime(); /////
			time1 = (end1 - start) / 1000000000.0;
			time2 = (end2 - end1) / 1000000000.0;
			time3 = (end3 - end2) / 1000000000.0;
			time4 = (end4 - end3) / 1000000000.0;
			time5 = tsum / 1000000000.0;
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
}
