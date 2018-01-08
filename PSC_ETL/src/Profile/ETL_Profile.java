package Profile;

public class ETL_Profile {
	// ETL系列程式   設定檔  2017.12.07 TimJhang
	
	// **E階段設定參數
	
	// 基本檢核參數(開/關)
	public final static boolean BasicCheck = true;
	// 比對檢核參數(開/關)
	public final static boolean AdvancedCheck = true;
	
	// **連線DB2 設定參數(預計有 7 + 1組)
	
	// Driver, Url, User, Password
	public final static String db2Driver = "com.ibm.db2.jcc.DB2Driver";
	private final static String db2SPSchema = "ADMINISTRATOR";
	public final static String db2TableSchema = "ADMINISTRATOR";
//	public final static String db2Url = 
//			"jdbc:db2://172.18.21.206:50000/ETLDB600:" +
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
//	public final static String db2User = "Administrator";
//	public final static String db2Password = "9ol.)P:?";
	public final static String db2Url = 
			"jdbc:db2://localhost:50000/sample:" +
			"currentschema=" + db2SPSchema + ";" +
			"currentFunctionPath=" + db2SPSchema + ";";
	public final static String db2User = "db2admin";
	public final static String db2Password = "timPSC2017";
	
	// Error Log寫入域值
	public final static int ErrorLog_Stage = 10000;
	
	// Data 寫入域值
	public final static int Data_Stage = 10000;
	
	// 業務別
	public final static String Foreign_Currency = "FR"; // 外幣 
	
	// 新北市農會附設北區農會電腦共用中心  951  相關參數
	
	// 財團法人農漁會南區資訊中心  952  相關參數
	
	// 板橋區農會電腦共用中心  928  相關參數
	
	// 財團法人農漁會聯合資訊中心  910  相關參數
	
	// 高雄市農會  605  相關參數
	
	// 農漁會資訊共用系統  600  相關參數
	
	// 018金庫  018  相關參數
	
}
