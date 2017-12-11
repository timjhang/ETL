package DB;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ETL_Q_ColumnCheckCodes {
	
	// 母確認Map(取得單一Extract程式下, 所有欄位檢核用Map)
	private Map<String, Map<String, String>> checkMaps = new HashMap<String, Map<String, String>>();
	
	// 取得檢核用代碼List
	public static List<CodeData> getCheckList(String columnName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		List<CodeData> resultList = new ArrayList<CodeData>();
		
		Connection con = ConnectionHelper.getDB2Connection();
		
		java.sql.Statement stmt = con.createStatement();
		String query = "SELECT \"Code\", \"Name\" FROM \"" + columnName + "\"";
        java.sql.ResultSet rs = stmt.executeQuery(query);
        
        while (rs.next()) {
        	CodeData data = new CodeData();
        	data.setCode(rs.getString(1));
        	data.setName(rs.getString(2));
        	
        	System.out.println(data.getCode() + " " + data.getName()); // test
        	resultList.add(data);
        }
        
        System.out.println("List Size = " + resultList.size()); // test
		
		return resultList;
	}
	
	// 取得檢核用代碼Map
	public static Map<String, String> getCheckMap(String columnName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		// Code & Name
		Map<String, String> resultMap = new HashMap<String, String>();
		
		Connection con = ConnectionHelper.getDB2Connection();
		
		java.sql.Statement stmt = con.createStatement();
		String query = "SELECT \"Code\", \"Name\" FROM \"" + columnName + "\"";
        java.sql.ResultSet rs = stmt.executeQuery(query);
        
        while (rs.next()) {
        	System.out.println(rs.getString(1) + " " + rs.getString(2)); // test
        	
        	String code = rs.getString(1);
        	code = (code == null)?"":code.trim();
        
        	String name = rs.getString(2);
        	name = (name == null)?"":name.trim();
        	
        	resultMap.put(code, name);
        }
        
        System.out.println("Map Size = " + resultMap.size()); // test
		
		return resultMap;
	}
	
	public Map<String, Map<String, String>> getCheckMaps(String[][] checkColumnArray) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		for (int i = 0 ; i < checkColumnArray.length; i++) {
			checkMaps.put(checkColumnArray[i][0], getCheckMap(checkColumnArray[i][1]));
		}
		
		return checkMaps;
	}

	public static void main(String[] argv) {
		
		try {
			
//			getCheckList("TimTest");
//			getCheckMap("TimTest");
			
			String[][] // checkColumnList = new String[2][];
			checkColumnList = {
					{"PARTY_PHONE_column_1", "TimTest"},
					{"PARTY_PHONE_column_2", "TimTest"}, 
					{"PARTY_PHONE_column_3", "TimTest"}};
			
			ETL_Q_ColumnCheckCodes one = new ETL_Q_ColumnCheckCodes();
			
			Map<String, Map<String, String>> maps = one.getCheckMaps(checkColumnList);
			System.out.println("size = " + maps.size());
			Map<String, String> map;
			map = maps.get("PARTY_PHONE_column_1");
			System.out.println("PARTY_PHONE_column_1 test");
			System.out.println("L exists is " + map.containsKey("L"));
			System.out.println("L2 exists is " + map.containsKey("L2"));
			map = maps.get("PARTY_PHONE_column_2");
			System.out.println("PARTY_PHONE_column_2 test");
			System.out.println("L exists is " + map.containsKey("L"));
			System.out.println("L2 exists is " + map.containsKey("L2"));
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

}
