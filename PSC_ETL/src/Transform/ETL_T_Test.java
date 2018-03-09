package Transform;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;

import DB.ConnectionHelper;
import Profile.ETL_Profile;

public class ETL_T_Test {

	// 測試程式 - function執行起點
	public void execute_function() {
		
		System.out.println("#######Transform - ETL_T_Test - execute_function Start");
		
		try {
			
			String sql = "begin ? := " + ETL_Profile.db2TableSchema + ".Tim.SampleFunction(?,?); end;";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, "123");
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
		
		System.out.println("#######Transform - ETL_T_Test - execute_function End");
		
	}
	
	// 測試程式 - function執行起點
	public void execute_procedure() {
		
		System.out.println("#######Transform - ETL_T_Test - execute_procedure Start");
		
		String input = "Kevin";
		System.out.println("input = " + input);
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Tim.SampleProcedure(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.setString(1, input);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			String output = cstmt.getString(2);
			String errorMsg = cstmt.getString(3);
			
			System.out.println("output = " + output);
			System.out.println("errorMsg = " + errorMsg);
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("#######Transform - ETL_T_Test - execute_procedure End");
		
	}
	
	public static void main(String[] argv) {
		
		ETL_T_Test one = new ETL_T_Test();
//		one.execute_procedure();
		one.execute_function();
		
	}
	
}
