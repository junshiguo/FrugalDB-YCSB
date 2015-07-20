package frugaldb.utility;

public class IdMatch {
	public static int totalTenant = 3000;
	public static int TenantPerType1000[] = {
		60, 90, 100, 150, 40, 60,
		36, 54, 60, 90, 24, 36,
		24, 36, 40, 60, 16, 24
	};
	public static int[] TenantIdRange = {
		180, 450, 750, 1200, 1320,
		1500, 1608, 1770, 1950, 2220, 2292,
		2400, 2472, 2580, 2700, 2880, 2928,
		3000
	};
	
	/**
	 * this function must be called before using getId()
	 * @param totaltenant
	 */
	public static void init(int totaltenant){
		totalTenant = totaltenant;
		initStartId();
	}
	
	//start from 0
	public static int getId(int tenantId){
//		return 0;
		if(totalTenant % 1000 == 0){
			int multi = totalTenant / 1000;
			int sum = 0;
			for(int i = 0; i < 18; i++){
				if(sum + TenantPerType1000[i] * multi > tenantId){
					if(i == 0)	return tenantId;
					return TenantIdRange[i-1]+tenantId - sum;
				}else{
					sum += TenantPerType1000[i] * multi;
				}
			}
		}else{
			for(int i = 0; i < 17; i++){
				if(startId[i+1] > tenantId){
					if(i == 0)	return tenantId;
					return TenantIdRange[i-1]+tenantId - startId[i];
				}
			}
		}
		return tenantId - startId[17] + TenantIdRange[16];
	}
	
	public static int[] startId = new int[18];
	public static int[] tenantPerType = new int[18];
	public static int[][] tenantGroup = new int[3][3];
	public static void initStartId(){
		int multi = totalTenant / 1000;
		double multid = totalTenant / 1000.0;
		int sumtmp = 0;
		for(int i = 0; i < 18; i++){
			if(totalTenant % 1000 == 0){
				tenantPerType[i] = TenantPerType1000[i] * multi;
			}else{
				if(i == 17){
					tenantPerType[i] = totalTenant - sumtmp;
					break;
				}
				tenantPerType[i] = (int) (TenantPerType1000[i] * multid);
				sumtmp += tenantPerType[i];
			}
		}
		tenantGroup[0][0] = tenantPerType[0] + tenantPerType[1]; tenantGroup[0][1] = tenantPerType[2] + tenantPerType[3]; tenantGroup[0][2] = tenantPerType[4] + tenantPerType[5];
		tenantGroup[1][0] = tenantPerType[6] + tenantPerType[7]; tenantGroup[1][1] = tenantPerType[8] + tenantPerType[9]; tenantGroup[1][2] = tenantPerType[10] + tenantPerType[11];
		tenantGroup[2][0] = tenantPerType[12] + tenantPerType[13]; tenantGroup[2][1] = tenantPerType[14] + tenantPerType[15]; tenantGroup[2][2] = tenantPerType[16] + tenantPerType[17];
		
		startId[0] = 0;
		for(int i = 1; i < 18; i++){
			startId[i] = startId[i-1] + tenantPerType[i-1];
		}
	}
	
	public static int getRecordCount(int id){
		if(getId(id) < 1500)	return 5000;
		else if(getId(id) < 2400)	return 10000;
		else return 15000;
	}

}
