package DB;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Types;
import java.util.Date;

import Bean.ETL_Bean_LogData;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_P_EData_Filter {
	
	// 進行E系列程式寫入後資料過濾, 排除重複PK
	public static void E_Datas_Filter(String procedureName, 
			String batch_no, String exc_central_no, Date exc_record_date, String upload_no, String program_no) throws Exception {
		
		System.out.println("#######Extract - E_Datas_Filter - " + procedureName + " Start " + new Date());
		
		// 目前傳值未用到
		ETL_Bean_LogData logData = new ETL_Bean_LogData();
		logData.setBATCH_NO(batch_no);
		logData.setCENTRAL_NO(exc_central_no);
		logData.setRECORD_DATE(exc_record_date);
		logData.setUPLOAD_NO(upload_no);
		logData.setPROGRAM_NO(program_no);
		
		try {
			
			// TODO
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Extract." + procedureName + "(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
//	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            throw new Exception(procedureName + " => Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
//			ex.printStackTrace();
			throw new Exception(procedureName + "'s Error:" + ex.getMessage());
		}
		
		System.out.println("#######Extract - E_Datas_Filter - " + procedureName + " End " + new Date());
		
	}
	
	public static void main(String[] argv) {
		
		try {
			E_Datas_Filter("filter_Party_Temp_Temp", 
					null, null, null, null, null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

}
