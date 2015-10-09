package frugaldb.utility;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class VoltdbTable {
	public static void main(String[] args) throws IOException {
		int volumnNumber = 50;
		String path = "voltdbTable/";
		BufferedWriter writer = new BufferedWriter(new FileWriter(path
				+ "table.sql"));
		for (int id = 0; id < volumnNumber; id++) {
			writer.write("create table usertable"
					+ id
					+ " (ycsb_key VARCHAR (255), field0 varchar(100), field1 varchar(100), field2 varchar(100), field3 varchar(100), field4 varchar(100), field5 varchar(100), field6 varchar(100), field7 varchar(100), field8 varchar(100), field9 varchar(100),"
					+ "tenant_id int not null, is_insert int, is_update int, CONSTRAINT user"+id+"  PRIMARY KEY (ycsb_key, tenant_id));");
			writer.newLine();
		}
		for(int id = 0; id < volumnNumber; id++){
			writer.write("PARTITION TABLE usertable"+id+" ON COLUMN tenant_id;");
			writer.newLine();
		}
		for(int id = 0; id < volumnNumber; id++){
			writer.write("CREATE PROCEDURE FROM CLASS SelectUsertable"+id+";");
			writer.newLine();
		}
		for(int id = 0; id < volumnNumber; id++){
			writer.write("CREATE PROCEDURE FROM CLASS OffloadUsertable"+id+";");
			writer.newLine();
		}
		writer.close();
	}

}
