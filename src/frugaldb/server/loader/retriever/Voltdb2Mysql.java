package frugaldb.server.loader.retriever;

import frugaldb.server.loader.LoadConfig;

public class Voltdb2Mysql extends Thread{
	
	public int tenantId, volumnId;
	
	public Voltdb2Mysql(int tenantId, int volumnId){
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}
	
	public void run(){
		CustomerRetriever cr = new CustomerRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		cr.start();
		DistrictRetriever dr = new DistrictRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		dr.start();
		HistoryRetriever hr = new HistoryRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		hr.start();
		NewOrdersRetriever nor = new NewOrdersRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		nor.start();
		OrderLineRetriever olr = new OrderLineRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		olr.start();
		OrdersRetriever or = new OrdersRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		or.start();
		StockRetriever sr = new StockRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		sr.start();
		WarehouseRetriever wr = new WarehouseRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		wr.start();
		ItemRetriever ir = new ItemRetriever(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		ir.start();
		try {
			cr.join();
			dr.join();
			hr.join();
			nor.join();
			olr.join();
			or.join();
			sr.join();
			wr.join();
			ir.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
