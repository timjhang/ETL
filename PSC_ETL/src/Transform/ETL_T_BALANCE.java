package Transform;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Types;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_T_BALANCE {

	// 觸發DB2轉換function, 轉換資料寫進BALANCE_LOAD
	public void trans_to_BALANCE_LOAD(ETL_Bean_LogData logData) {

		System.out.println("#######Transform - ETL_T_BALANCE - Start");

		try {

			String sql = "begin ? := " + ETL_Profile.db2TableSchema + ".Transform.TempTo_BALANCE_LOAD(?,?); end;";

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
		System.out.println("#######Transform - ETL_T_BALANCE - End");
	}

	public static void main(String[] argv) {

		ETL_T_BALANCE one = new ETL_T_BALANCE();
		ETL_Bean_LogData two = new ETL_Bean_LogData();
		two.setBATCH_NO("1");
		two.setCENTRAL_NO("2");
		two.setFILE_TYPE("3");
		two.setPROGRAM_NO("4");
		two.setUPLOAD_NO("5");
		two.setRECORD_DATE(new java.util.Date());

		one.trans_to_BALANCE_LOAD(two);
	}
}
