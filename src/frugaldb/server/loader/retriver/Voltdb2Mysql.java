package frugaldb.server.loader.retriver;

import frugaldb.server.loader.LoadConfig;

public class Voltdb2Mysql extends Thread{
	
	public int tenantId, volumnId;
	
	public Voltdb2Mysql(int tenantId, int volumnId){
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}
	
	public void run(){
		CustomerRetriver cr = new CustomerRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		cr.start();
		DistrictRetriver dr = new DistrictRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		dr.start();
		HistoryRetriver hr = new HistoryRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		hr.start();
		NewOrdersRetriver nor = new NewOrdersRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		nor.start();
		OrderLineRetriver olr = new OrderLineRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		olr.start();
		OrdersRetriver or = new OrdersRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		or.start();
		StockRetriver sr = new StockRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		sr.start();
		WarehouseRetriver wr = new WarehouseRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
		wr.start();
		ItemRetriver ir = new ItemRetriver(LoadConfig.url, LoadConfig.username, LoadConfig.password, LoadConfig.voltdbServer, tenantId, volumnId);
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
