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
public class FSocketReceive extends Thread {
	private Socket socket;
	private BufferedReader reader;
	
	public FSocketReceive(Socket socket, BufferedReader reader) throws UnsupportedEncodingException, IOException{
		this.socket = socket;
		this.reader = reader;
	}
	
	public void run(){
		try {
			String message = null;
			String[] info = null;
			while ((message = reader.readLine()) != null) {
				info = message.trim().split("\\s+");
				if (info[0].equalsIgnoreCase("interval")) {
					FServer.setIntervalId(Integer.parseInt(info[1]));
				}else{
					reader.close();
					socket.close();
					FServer.checkActive(-1);
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
