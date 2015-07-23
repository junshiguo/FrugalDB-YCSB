package frugaldb.utility;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class VoltProcedureGenerator {
	public static void main(String[] args) throws IOException{
		int volumnNumber = 50;
		String path = "voltdbTable/procedures/";
		for(int id = 0; id < volumnNumber; id++){
			String filename = "SelectUsertable"+id;
			BufferedWriter writer = new BufferedWriter(new FileWriter(path+filename+".java"));
			writer.write("import org.voltdb.*;\n"
					+ "public class "+filename+" extends VoltProcedure{\n"
					+ "	public final SQLStmt sql = new SQLStmt(\"SELECT * FROM usertable"+id+" WHERE tenant_id = ? AND is_insert = ? AND is_update = ?\");\n"
							+ "	public VoltTable[] run(int tenant_id, int is_insert, int is_update) throws VoltAbortException {\n"
							+ "		voltQueueSQL(sql, tenant_id, is_insert, is_update);\n"
							+ "		return voltExecuteSQL();\n	}\n}");
			writer.close();
			
			filename = "OffloadUsertable"+id;
			writer = new BufferedWriter(new FileWriter(path+filename+".java"));
			writer.write("import org.voltdb.*;\n"
					+ "public class "+filename+" extends VoltProcedure {\n"
					+ "	public final SQLStmt sql = new SQLStmt(\"insert into usertable"+id+" values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)\");\n"
							+ "	public VoltTable[] run(int tenantId, String[] lines, int length) throws VoltAbortException {\n"
							+ "		String[] values;\n"
							+ "		for(int i = 0; i < length; i++){\n"
							+ "			values = lines[i].split(\",\");\n"
									+ "			voltQueueSQL(sql, values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], values[10], tenantId, 0, 0);\n"
									+ "		}\n		voltExecuteSQL();\n		return null;\n	}\n}");
			writer.close();
		}
		
	}

}
