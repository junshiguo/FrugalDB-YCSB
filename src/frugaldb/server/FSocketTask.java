package frugaldb.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.Socket;

/**
 * receive the list of tenant ids to be loaded or to be written back
 * @author guojunshi
 *
 */
public class FSocketTask extends Thread {
	private Socket socket;
	private BufferedReader reader;
	private Writer writer;
	
	public FSocketTask(Socket socket, BufferedReader reader, Writer writer) throws UnsupportedEncodingException, IOException{
		this.socket = socket;
		this.reader = reader;
		this.writer = writer;
	}
	
	//send task
	public void sendM2V(int mid, int vid) throws IOException{
		writer.write("M2V "+mid+" "+vid+"\n");
		writer.flush();
	}
	public void sendV2M(int mid) throws IOException{
		writer.write("V2M "+mid+" -1\n");
		writer.flush();
	}
	public void sendEnd() throws IOException{
		writer.write("end\n");
		writer.flush();
	}
	public void clean() throws IOException{
		sendEnd();
		writer.close();
		reader.close();
		socket.close();
	}
	
	//receive task
	public void run(){
		try {
			String message = null;
			String[] info = null;
			while ((message = reader.readLine()) != null) {
				info = message.trim().split("\\s+");
				if (info[0].equalsIgnoreCase("interval")) {
					FServer.setIntervalId(Integer.parseInt(info[1]));
				}else{
					FServer.checkActive(-1);
					break;
				}
			}
			reader.close();
			writer.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
