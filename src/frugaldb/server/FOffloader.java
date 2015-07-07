package frugaldb.server;

import java.util.ArrayList;

import newhybrid.util.AbstractTenant;
import newhybrid.util.Offloader;

public class FOffloader extends Offloader {

	public FOffloader(ArrayList<AbstractTenant> tenants) {
		super(tenants);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getWorkloadLimitInMysql() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTotSpaceInVoltdb() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNewVoltdbIDForTenant(int tenantID) {
		// TODO Auto-generated method stub
		return 0;
	}

}
