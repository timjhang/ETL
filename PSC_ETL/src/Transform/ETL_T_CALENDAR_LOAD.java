package Transform;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Types;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_T_CALENDAR_LOAD {
	// 觸發DB2轉換Procedure, 轉換資料寫進CALENDAR_LOAD  // TODO
		public void trans_to_CALENDAR_LOAD(ETL_Bean_LogData logData) {
			
			System.out.println("#######Transform - ETL_T_CALENDAR_LOAD - Start"); // TODO
			
			try {
				
				// TODO
				String sql = "{call " + ETL_Profile.db2TableSchema + ".Transform.TempTo_CALENDAR_LOAD(?,?,?)}";
				
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
		            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				}
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
			System.out.println("#######Transform - ETL_T_CALENDAR_LOAD - End"); // TODO
			
		}
}
