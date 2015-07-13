package frugaldb.server;

import java.io.IOException;
import java.io.Writer;
import java.net.Socket;

public class FSocketSend {
	private Socket socket;
	private Writer writer;
	
	public FSocketSend(Socket socket, Writer writer){
		this.socket = socket;
		this.writer = writer;
	}
	
	public void send(boolean isM2V, int idM, int idV) throws IOException{
		if(isM2V){
			writer.write("M2V "+idM+" "+idV+"\n");
		}else{
			writer.write("V2M "+idM+" "+idV+"\n");
		}
		writer.flush();
	}
	
	public void clean() throws IOException{
		writer.close();
		socket.close();
	}

}
