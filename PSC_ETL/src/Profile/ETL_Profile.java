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
	public final static String db2Url = 
			"jdbc:db2://localhost:50000/ETLDB002:" +
			"currentschema=" + db2SPSchema + ";" +
			"currentFunctionPath=" + db2SPSchema + ";";
	
//	public final static String db2Url = 
//			"jdbc:db2://172.18.21.206:50000/ETLDB600:" +
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
	public final static String db2User = "Administrator";
	public final static String db2Password = "9ol.)P:?";
	
	
//	public final static String db2Url = 
//			"jdbc:db2://localhost:50000/sample:" +
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
//	public final static String db2User = "db2admin";
//	public final static String db2Password = "timPSC2017";
	
	// Error Log寫入域值
	public final static int ErrorLog_Stage = 10000;
	
	// Data 寫入域值
	public final static int Data_Stage = 10000;
	
	// 業務別
	public final static String Foreign_Currency = "FR"; // 外幣
	
	// 難字表excel檔存放路徑
	public final static String DifficultWords_Lists_Path = "C:/DifficultWords/%s.xlsx";
	
	// 新北市農會附設北區農會電腦共用中心  951  相關參數
	
	// 財團法人農漁會南區資訊中心  952  相關參數
	
	// 板橋區農會電腦共用中心  928  相關參數
	
	// 財團法人農漁會聯合資訊中心  910  相關參數
	
	// 高雄市農會  605  相關參數
	
	// 農漁會資訊共用系統  600  相關參數
	
	// 018金庫  018  相關參數
	
	// 各資料讀檔緩衝區大小
//	public final static int ETL_E_PARTY = 620;
//	public final static int ETL_E_PARTY_PARTY_REL = 203;
//	public final static int ETL_E_PARTY_PHONE = 43;
//	public final static int ETL_E_PARTY_ADDRESS = 137;
//	public final static int ETL_E_ACCOUNT = 113;
//	public final static int ETL_E_TRANSACTION = 484;
//	public final static int ETL_E_LOAN_DETAIL = 112;
//	public final static int ETL_E_LOAN = 214;
//	public final static int ETL_E_COLLATERAL = 203;
//	public final static int ETL_E_GUARANTOR = 124;
//	public final static int ETL_E_FX_RATE = 30;
//	public final static int ETL_E_SERVICE = 1434;
//	public final static int ETL_E_TRANSFER = 723;
//	public final static int ETL_E_FCX = 251;
//	public final static int ETL_E_CALENDAR = 23;
	public final static int ETL_E_PARTY = 310;
	public final static int ETL_E_PARTY_PARTY_REL = 101;
	public final static int ETL_E_PARTY_PHONE = 22;
	public final static int ETL_E_PARTY_ADDRESS = 63;
	public final static int ETL_E_ACCOUNT = 56;
	public final static int ETL_E_TRANSACTION = 242;
	public final static int ETL_E_LOAN_DETAIL = 56;
	public final static int ETL_E_LOAN = 107;
	public final static int ETL_E_COLLATERAL = 101;
	public final static int ETL_E_GUARANTOR = 62;
	public final static int ETL_E_FX_RATE = 15;
	public final static int ETL_E_SERVICE = 717;
	public final static int ETL_E_TRANSFER = 361;
	public final static int ETL_E_FCX = 125;
	public final static int ETL_E_CALENDAR = 11;

	// 讀檔筆數域值
	public final static int ETL_E_Stage = 10000;
}
