import org.voltdb.*;
public class OffloadUsertable24 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("insert into usertable24 values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	public VoltTable[] run(int tenantId, String[] lines, int length) throws VoltAbortException {
		String[] values;
		for(int i = 0; i < length; i++){
			values = lines[i].split(",");
			voltQueueSQL(sql, values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], values[10], tenantId, 0, 0);
		}
		voltExecuteSQL();
		return null;
	}
}