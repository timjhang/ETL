package Transform;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.util.Date;

import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_T_TRANSACTION {

//	// 觸發DB2轉換function, 轉換資料寫進PARTY_PHONE_LOAD  // TODO
//	public void trans_to_TRANSACTION(
//			String batch_no, String exc_central_no, Date exc_record_date, String upload_no, String program_no) {
//		
//		System.out.println("#######Transform - ETL_T_TRANSACTION - Start"); // TODO
//		
//		try {
//			
//			// TODO
//			String sql = "call " + ETL_Profile.db2TableSchema + ".Transform.TempTo_TRANSACTION_LOAD(?,?,?); ";
//
//			
//			Connection con = ConnectionHelper.getDB2Connection();
//			CallableStatement cstmt = con.prepareCall(sql);
//			
//			ETL_Tool_CastObjUtil.castObjectArr(sql);
//			 person = con.createStruct("MYTYPE.PERSON_T", personAttributes);  
//			// TODO Start
//			cstmt.registerOutParameter(1, Types.INTEGER);
//			cstmt.setObject(2, person);  
//			cstmt.registerOutParameter(3, Types.VARCHAR);
//			// TODO End
//			
//			cstmt.execute();
//			
//			int returnCode = cstmt.getInt(1);
//			
//			if (returnCode != 0) {
//				String errorMessage = cstmt.getString(2);
//	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//			}
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//		
//		System.out.println("#######Transform - ETL_T_TRANSACTION - End"); // TODO
		
	}
	
	


