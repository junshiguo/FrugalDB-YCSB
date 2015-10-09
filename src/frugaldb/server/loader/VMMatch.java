package frugaldb.server.loader;

public class VMMatch {
	public static int maxTenantPerVolume;
	public static int[][] vmMatch; //tenant id in voltdb table volume
	public static int[] tenantPerVolume; //number of tenants in each voltdb volume
	public static boolean isInitiated = false;
	
	public static void init(){
		maxTenantPerVolume = 10;
		vmMatch = new int[LoadConfig.VTableNumber][maxTenantPerVolume];
		tenantPerVolume = new int[LoadConfig.VTableNumber];
		for(int i = 0; i < LoadConfig.VTableNumber; i++){
			tenantPerVolume[i] = 0;
			for(int j = 0; j < maxTenantPerVolume; j++){
				vmMatch[i][j] = -1;
			}
		}
		isInitiated = true;
	}
	
	public static int findTenant(int tenantId){
		for(int j = 0; j < maxTenantPerVolume; j++){
			for(int i = 0; i < 50; i++){
				if(vmMatch[i][j] == tenantId)
					return i;
			}
		}
		return -1;
	}
	
	public static int findVolumn(){
		if(isInitiated == false){
			init();
		}
		for(int j = 0; j < maxTenantPerVolume; j++){
			for(int i = 0; i < 50; i++){
				if(vmMatch[i][j] == -1)
					return i;
			}
		}
		return -1;
	}
	//add before actually offloaded data
	public static boolean addMatch(int volumnId, int tenantId){
		if(isInitiated == false){
			init();
		}
		if(tenantPerVolume[volumnId] >= maxTenantPerVolume){
			return false;
		}
		vmMatch[volumnId][tenantPerVolume[volumnId]] = tenantId;
		tenantPerVolume[volumnId] ++;
		return true;
	}
	//delete after actually retrived data
	public static boolean deleteMatch(int volumnId, int tenantId){
		if(isInitiated == false){
			init();
		}
		if(tenantPerVolume[volumnId] <= 0){
			return false;
		}
		for(int i = 0; i < tenantPerVolume[volumnId]; i++){
			if(vmMatch[volumnId][i] == tenantId){
				vmMatch[volumnId][i] = vmMatch[volumnId][tenantPerVolume[volumnId] - 1];
				tenantPerVolume[volumnId]--;
				return true;
			}
		}
		return false;
	}

}
