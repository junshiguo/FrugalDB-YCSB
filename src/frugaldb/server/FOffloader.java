package frugaldb.server;

import java.util.ArrayList;

import newhybrid.util.AbstractTenant;
import newhybrid.util.Offloader;

public class FOffloader extends Offloader {

	public FOffloader(ArrayList<AbstractTenant> tenants) {
		super(tenants);
	}

	@Override
	public int getWorkloadLimitInMysql() {
		return 30000;
	}

	@Override
	public int getTotSpaceInVoltdb() {
		return 2000;
	}

	@Override
	public int getNewVoltdbIDForTenant(int tenantID) {
		// TODO Auto-generated method stub
		return 0;
	}

}
