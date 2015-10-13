package frugaldb.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
	public void send(String line) throws IOException{
		writer.write(line+"\n");
		writer.flush();
	}
	public void sendM2V(int mid, int vid) throws IOException{
		send("M2V "+mid+" "+vid);
	}
	public void sendV2M(int mid) throws IOException{
		send("V2M "+mid+" -1");
	}
	public void sendV2C(int mid) throws IOException{
		send("V2C "+mid);
	}
	public void sendEnd() throws IOException{
		send("end");
	}
	public void clean() throws IOException{
		sendEnd();
		writer.close();
		reader.close();
		socket.close();
	}
	
	//receive task
	@Override
	public void run(){
		try {
			String message = null;
			String[] info = null;
			while ((message = reader.readLine()) != null) {
				info = message.trim().split("\\s+");
				System.out.println("Receive: "+message);
				if (info[0].equalsIgnoreCase("dodataload")) {
					FServer.loadThread.addSemaphore();
					System.out.println("starting loading data...");
				}else{
					//end this socket, does not end the FServer process
					System.out.println("receive: "+message+". Current test ends...");
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
