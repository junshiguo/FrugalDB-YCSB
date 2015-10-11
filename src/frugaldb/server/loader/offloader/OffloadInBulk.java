package frugaldb.server.loader.offloader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import frugaldb.server.loader.LoadConfig;

import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

public class OffloadInBulk {
	public int tenantId;
	public int volumnId;
	public Connection conn;
	public Client client;
	public Statement stmt = null;
	
	public OffloadInBulk(int tenantId, int volumnId, Connection conn, Client client){
		this.tenantId = tenantId;
		this.volumnId = volumnId;
		this.conn = conn;
		this.client = client;
	}
	
	public String getSQL(){
		String ret = "select concat(ycsb_key, ',' ,field0, ',' ,field1, ',' ,field2, ',' ,field3, ',' ,field4, ',' ,field5, ',' ,field6,"
				+ " ',' ,field7, ',' ,field8, ',' ,field9) from usertable"+tenantId+" into outfile '"+LoadConfig.csvPath+"/usertable"+tenantId+".csv'";
		return ret;
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
			
			stmt.execute(getSQL());
			stmt.close();
			client.callProcedure("@AdHoc", "delete from usertable"+volumnId+" where tenant_id = "+tenantId);
			
			FileReader filereader = new FileReader(LoadConfig.csvPath+"/usertable"+tenantId+".csv");
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
					client.callProcedure("OffloadUsertable"+volumnId, tenantId, lines, count);
					count = 0;
				}
				te = System.nanoTime();
				tsum += te - ts;
			}
			if(count > 0)
				client.callProcedure("OffloadUsertable"+volumnId, tenantId, lines, count);
			filereader.close();
			reader.close();
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
}
