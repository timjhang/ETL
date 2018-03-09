package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

import DB.ConnectionHelper;
import Profile.ETL_Profile;

public class ETL_C_PROCESS {
	
	// 執行ETL
	public static boolean executeETL(String[] etlServerInfo, String batch_No, String central_no, Date record_Date) {
		
		System.out.println("#### ETL_C_PROCESS  Start");
		
		// for test
		System.out.println("Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2]);
		System.out.println("Batch_No = " + batch_No);
		System.out.println("Central_no = " + central_no);
		System.out.println("Record_Date = " + record_Date);
		
		String exeInfo = "Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2] + "\n";
		exeInfo = exeInfo + "Batch_No = " + batch_No + "\n";
		exeInfo = exeInfo + "Central_no = " + central_no + "\n";
		exeInfo = exeInfo + "Record_Date = " + record_Date + "\n";
		
		// ETL Server下載特定中心資料
		String[] fileInfo = new String[3];
		String exc_record_date = "";
		String upload_no = "";
		boolean exeResult = true;
		
		// for test
		exc_record_date = "20180227";
		upload_no = "001";
		
		
		// **更新 Server 狀態使用中
		try {
			update_Server_Status(etlServerInfo[0], "U");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"使用中\"失敗");
			ex.printStackTrace();
			return false;
		}
		
		try {
			
			// 執行下載
	//		if (!ETL_C_CallWS.call_ETL_Server_getUploadFileInfo(etlServerInfo[2], central_no, fileInfo)) {
	//			System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_getUploadFileInfo 發生錯誤！");
	//			return false;
	//		}
	//		exc_record_date = fileInfo[0];
	//		upload_no = fileInfo[1];
			
			// for test
//			Date testDate = new SimpleDateFormat("yyyyMMdd").parse("20180227");
			record_Date = new SimpleDateFormat("yyyyMMdd").parse("20180227");

			
			// 更新報送單位狀態"使用中"
			updateCentralTime(central_no, record_Date, upload_no, "Start");

			
//			// 寫入E Master Log
//			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "E", etlServerInfo[0])) {
//	//			System.out.println("E Master Log已存在\n" + exeInfo);
//				return false;
//			};
//	//		// 進行E系列程式
//	//		if (!ETL_C_CallWS.call_ETL_Server_Efunction(etlServerInfo[2], "", batch_No, central_no, exc_record_date, upload_no)) {
//	//			System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Efunction 發生錯誤！");
//	//			return false;
//	//		}
//			// 更新 E Master Log
//			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "E", "E", "Y", "");
//			
//			// 寫入T Master Log
//			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "T", etlServerInfo[0])) {
//	//			System.out.println("T Master Log已存在\n" + exeInfo);
//				return false;
//			}
//	//		// 進行T系列程式
//	//		if (!ETL_C_CallWS.call_ETL_Server_Tfunction(etlServerInfo[2], "", batch_No, central_no, exc_record_date, upload_no)) {
//	//			System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Tfunction 發生錯誤！");
//	//			return false;
//	//		}
//			// 更新 T Master Log
//			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "T", "E", "Y", "");
//			
//			
//			// 寫入L Master Log
//			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "L", etlServerInfo[0])) {
//	//			System.out.println("L Master Log已存在\n" + exeInfo);
//				return false;
//			}
//			// 進行L系列程式  ????
//			
//			// 更新 T Master Log
//			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "L", "E", "Y", "");
			
		
		} catch (Exception ex) {
			ex.printStackTrace();
			exeResult = false;
		} finally {
			// 更新報送單位狀態"執行完畢"
			try {
				updateCentralTime(central_no, record_Date, upload_no, "End");
			} catch (Exception ex) {
				System.out.println("更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗");
				ex.printStackTrace();
				exeResult = false;
			}
		}
		
		// **更新 Server 狀態可使用
		try {
			update_Server_Status(etlServerInfo[0], "Y");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"可使用\"失敗");
			ex.printStackTrace();
			exeResult = false;
		}
		
		System.out.println("#### ETL_C_PROCESS  End");
		
		return exeResult;
		
	}
	
	// 寫入Master Log
	private static boolean writeMasterLog(String batch_No, String central_No, Date record_Date,
			String upload_No, String step_Type, String server_No) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.write_Master_Log(?,?,?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, batch_No);
			cstmt.setString(3, central_No);
			cstmt.setDate(4, new java.sql.Date(record_Date.getTime()));
			cstmt.setString(5, upload_No);
			cstmt.setString(6, step_Type);
			cstmt.setString(7, server_No);
			cstmt.registerOutParameter(8, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(8);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//			            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return false;
			}
			
			return true;
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
		
	}
	
	// 更新Master Log
	private static boolean updateMasterLog(String batch_No, String central_No, Date record_Date, String upload_No, String step_Type,
			String exe_Status, String exe_Result, String exe_Result_Description) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_Master_Log(?,?,?,?,?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, batch_No);
			cstmt.setString(3, central_No);
			cstmt.setDate(4, new java.sql.Date(record_Date.getTime()));
			cstmt.setString(5, upload_No);
			cstmt.setString(6, step_Type);
			cstmt.setString(7, exe_Status);
			cstmt.setString(8, exe_Result);
			cstmt.setString(9, exe_Result_Description);
			cstmt.registerOutParameter(10, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(8);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//			            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return false;
			}
			
			return true;
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
		
	}
	
	// 更新 Server 狀態使用中
	private static void update_Server_Status(String server_No, String usable_Status) throws Exception {
		
		String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_Server_Status(?,?,?,?)}";
		
		Connection con = ConnectionHelper.getDB2Connection();
		CallableStatement cstmt = con.prepareCall(sql);
		
		cstmt.registerOutParameter(1, Types.INTEGER);
		cstmt.setString(2, server_No);
		cstmt.setString(3, usable_Status);
		cstmt.registerOutParameter(4, Types.VARCHAR);
		
		cstmt.execute();
		
		int returnCode = cstmt.getInt(1);
		
		// 有錯誤釋出錯誤訊息   不往下繼續進行
		if (returnCode != 0) {
			String errorMessage = cstmt.getString(4);
			System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
			
	}
	
	// 更新報送單位狀態
	private static void updateCentralTime(String central_No, Date record_Date, String upload_No, String status) throws Exception {
		
		String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_Central_Time(?,?,?,?,?,?)}";
		
		Connection con = ConnectionHelper.getDB2Connection();
		CallableStatement cstmt = con.prepareCall(sql);
		
		cstmt.registerOutParameter(1, Types.INTEGER);
		cstmt.setString(2, central_No);
		cstmt.setDate(3, new java.sql.Date(record_Date.getTime()));
		cstmt.setString(4, upload_No);
		cstmt.setString(5, status);
		cstmt.registerOutParameter(6, Types.VARCHAR);
		
		cstmt.execute();
		
		int returnCode = cstmt.getInt(1);
		
		// 有錯誤釋出錯誤訊息   不往下繼續進行
		if (returnCode != 0) {
			String errorMessage = cstmt.getString(6);
//			System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
		
	}
	
	public static void main(String[] argv) {
		try {
			String[] serverInfo = new String[3];
			serverInfo[0] = "ETL_S1";
			serverInfo[1] = "test2";
			serverInfo[2] = "127.0.0.1:8083";
			Date date = new Date();
			executeETL(serverInfo, "tim18226", "600", date);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
