package frugaldb.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import frugaldb.utility.DBManager;

public class FrugalDBClient extends DB {
	public int idInMysql;
	public int idInVoltdb;
	
	/**
	 * _inVoltdb is set according to data transfered from server, after the data is offloaded or retrieved
	 */
	public boolean _inVoltdb = false;
	public boolean isInVoltdb(){
		return _inVoltdb;
	}
	public void setInVoltdb(int id){
		idInVoltdb = id;
		if(idInVoltdb == -1)	_inVoltdb = false;
		else _inVoltdb = true;
	}

	private Connection mysqlConn;
	private Statement mysqlStmt;
	private PreparedStatement preparedInsert;
	private PreparedStatement preparedUpdate;
	private Client voltdbConn;
	
	//check connection can be called before each operation on db
	public void checkMysqlConnection() throws SQLException{
		if(mysqlConn == null || mysqlConn.isClosed()){
			mysqlConn = DBManager.connectDB(this.getProperties().getProperty("db.url", "jdbc:mysql://127.0.0.1/ycsb"), 
					this.getProperties().getProperty("db.user", "remote"), this.getProperties().getProperty("db.passwd", "remote"));
			mysqlStmt = mysqlConn.createStatement();
		}else if(mysqlStmt == null){
			mysqlStmt = mysqlConn.createStatement();
		}
		_connected = true;
	}
	
	public void checkVoltdbConnection(){
		if(voltdbConn == null){
			voltdbConn = DBManager.connectVoltdb(this.getProperties().getProperty("voltdbserver", "127.0.0.1"));
		}
		_connected = true;
	}
	
	private boolean _connected =false;
	public void closeConnection(){
		if(_connected){
			try {
				cleanup();
			} catch (DBException e) {
			}
		}
	}
	
	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init(int id) throws DBException
	{
		this.idInMysql = id;
//		try {
//			checkMysqlConnection();
////			System.out.println(id+" "+this.getProperties().getProperty("db.url", "jdbc:mysql://127.0.0.1/ycsb"));
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		if(isInVoltdb()){
//			checkVoltdbConnection();
//		}
	}
	
	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
		try {
			if(mysqlStmt != null){
				mysqlStmt.close();
			}
			if(preparedInsert != null){
				preparedInsert.close();
			}
			if(preparedUpdate != null){
				preparedUpdate.close();
			}
			if(mysqlConn != null){
				mysqlConn.close();
			}
			if(voltdbConn != null){
				voltdbConn.close();
			}
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
			String sql = "SELECT ";
			if(fields == null){
				sql += "* ";
			}else{
				for(String f : fields){
					sql += f+",";
				}
				sql = sql.substring(0, sql.length()-1);
			}
			if(isInVoltdb() == false){
				checkMysqlConnection();
				sql += " FROM "+table+idInMysql+" WHERE ycsb_key = '"+key+"'";
				ResultSet rs = mysqlStmt.executeQuery(sql);
//				while(rs.next()){
//					ResultSetMetaData rsmd = rs.getMetaData();
//					for(int i = 0; i < rsmd.getColumnCount(); i++){
//						result.put(rsmd.getColumnName(i), new ByteArrayByteIterator(rs.getString(i).getBytes()));
//					}
//				}
			}else{
				checkVoltdbConnection();
				sql += " FROM "+table+idInVoltdb+" WHERE ycsb_key = '"+key+"' AND tenantId = "+idInMysql;
				ClientResponse response;
				if(fields == null){
					response = voltdbConn.callProcedure(table.toUpperCase()+idInVoltdb+".select", key, idInMysql);
				}else{
					response = voltdbConn.callProcedure("@AdHoc", sql);
				}
//				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//					VoltTable results = response.getResults()[0];
//					for (int i = 0; i < results.getRowCount(); i++) {
//						VoltTableRow row = results.fetchRow(i);
//						for(int i1 = 0; i1 < results.getColumnCount(); i1++){
//							result.put(results.getColumnName(i1), new ByteArrayByteIterator(row.getString(i1).getBytes()));
//						}
//					}
//				}
			}
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return 0;
		//TODO: key not integer
//		try {
//			if(checkInVoltdb(false, false)){
//				checkMysqlConnection();
//				String sql = "SELECT ";
//				if(fields == null){
//					sql += "*";
//				}else{
//					for(String f : fields){
//						sql += f+",";
//					}
//					sql = sql.substring(0, sql.length()-1);
//				}
//				sql += " FROM "+table+idInMysql+" WHERE key >= "+startkey+" AND key <= "+(startkey+recordcount);
//				ResultSet rs = mysqlStmt.executeQuery(sql);
////				while(rs.next()){
////					HashMap<String, ByteIterator> tmp = new HashMap<String, ByteIterator>();
////					ResultSetMetaData rsmd = rs.getMetaData();
////					for(int i = 0; i < rsmd.getColumnCount(); i++){
////						tmp.put(rsmd.getColumnName(i), new ByteArrayByteIterator(rs.getString(i).getBytes()));
////					}
////					result.addElement(tmp);
////				}
//			}else{
//				checkVoltdbConnection();
//				String sql = "SELECT ";
//				if(fields == null){
//					sql += "*";
//				}else{
//					for(String f : fields){
//						sql += f+",";
//					}
//					sql = sql.substring(0, sql.length()-1);
//				}
//				sql += " FROM "+table+idInVoltdb+" WHERE key >= "+startkey+" AND key <= "+(startkey+recordcount)+" AND tenantId = "+idInMysql;
//				ClientResponse response = voltdbConn.callProcedure("@AdHoc", sql);
////				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
////					VoltTable results = response.getResults()[0];
////					for (int i = 0; i < results.getRowCount(); i++) {
////						VoltTableRow row = results.fetchRow(i);
////						HashMap<String, ByteIterator> tmp = new HashMap<String, ByteIterator>();
////						for(int i1 = 0; i1 < results.getColumnCount(); i1++){
////							tmp.put(results.getColumnName(i1), new ByteArrayByteIterator(row.getString(i1).getBytes()));
////						}
////						result.addElement(tmp);
////					}
////				}
//			}
//		} catch (SQLException | IOException | ProcCallException e) {
//			e.printStackTrace();
//			return 1;
//		}
//		return 0;
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		try {
			if(isInVoltdb() == false){
				checkMysqlConnection();
				String sql = "UPDATE "+table+idInMysql+" SET ";
				for (String k : values.keySet()){
					sql += k + " = ?,";
				}
				sql = sql.substring(0, sql.length()-1);
				sql += " WHERE ycsb_key = '"+key+"'";
				preparedUpdate = mysqlConn.prepareStatement(sql);
				int index = 1;
				for(String k : values.keySet()){
					preparedUpdate.setString(index, values.get(k).toString());
					index++;
				}
				preparedUpdate.execute();
			}else{
				checkVoltdbConnection();
				String sql = "UPDATE "+table+idInVoltdb+" SET ";
				for (String k : values.keySet()){
					sql += k+ " = '"+values.get(k).toString()+"',";
				}
				sql = sql.substring(0, sql.length()-1);
				sql += " WHERE ycsb_key = '"+key+"' AND tenantId = "+idInMysql;
				voltdbConn.callProcedure("@AdHoc", sql);
			}
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		try {
			if (isInVoltdb() == false) {
				checkMysqlConnection();
//				if(preparedInsert == null){
					String sql = "INSERT INTO "+table+idInMysql+" (ycsb_key";
					for(String k : values.keySet()){
						sql += ","+k;
					}
					sql += ") VALUES ('"+key+"'";
					for(int i = 0; i < values.size(); i++){
						sql += ",?";
					}
					sql += ")";
					preparedInsert = mysqlConn.prepareStatement(sql);
//				}
				int index = 1;
				for (String k : values.keySet())
				{
					preparedInsert.setString(index, values.get(k).toString());
					index++;
				}
				preparedInsert.executeUpdate();
			} else {
				checkVoltdbConnection();
				Object[] v =  values.values().toArray();
				Object[] para = new Object[v.length+1];
				for(int i = 0; i < v.length; i++){
					para[i] = v[i];
				}
				para[v.length] = idInMysql;
				voltdbConn.callProcedure(table.toUpperCase()+idInVoltdb+".insert", para);
			}
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int delete(String table, String key) {
		try {
			if(isInVoltdb() == false){
				checkMysqlConnection();
				String sql = "DELETE FROM "+table+idInMysql+" WHERE ycsb_key = '"+key+"'";
				mysqlStmt.execute(sql);
			}else{
				checkVoltdbConnection();
				voltdbConn.callProcedure(table.toUpperCase()+idInVoltdb+".delete", key, idInMysql);
			}
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

}
