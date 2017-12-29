import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

import DB.ConnectionHelper;
import Extract.ETL_E_PARTY;
import Extract.ETL_E_PARTY_PHONE;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;


public class Test {

	public static void main(String[] argv) {
		try {
//			test1();
//			test2();
//			test3();
//			test4();
//			test5();
//			test6();
//			connection();
			updateTest();
//			Date date = new Date().setTime(0);
//			date.setTime(0);
//			System.out.println(date);
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static void updateTest()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String insert_query = " UPDATE " + ETL_Profile.db2TableSchema + ".TIMTEST SET TVALUE = ? WHERE TKEY = ?";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(insert_query);

		pstmt.setString(1, "TEST");
		pstmt.setString(2, "123");

		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}

	}
	
	private static void columnTest() {
		String[][] array = {
				{"PARTY_PHONE_column_1", "TimTest"}, 
				{"PARTY_PHONE_column_2", "TimTest"}, 
				{"PARTY_PHONE_column_3", "TimTest"}};
		System.out.println(array.length);
		System.out.println(array[0].length);
		System.out.println(array[0][1]);
		System.out.println(array[1][0]);
	}
	
	private static void connection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection con = ConnectionHelper.getDB2Connection();
		ETL_E_PARTY_PHONE one = new ETL_E_PARTY_PHONE();
//		one.read_Party_Phone_File("C:/Users/10404003/Desktop/農經/171221/file/", "PARTY_PHONE", "ETL00001", "003");
		
//		ETL_E_PARTY two = new ETL_E_PARTY();
//		two.read_Party_File("C:/Users/10404003/Desktop/農經/171221/file/", "PARTY", "003");
	}
	
	private static void test1() throws Exception {
		ETL_Tool_ParseFileName one = new ETL_Tool_ParseFileName("018_FR_PARTY_PHONE_20171211.txt");
		System.out.println(one.getFileName());
		System.out.println(one.getCentral_No());
		System.out.println(one.getFile_Type());
		System.out.println(one.getRecord_Date());
		System.out.println(one.getFile_Name());
	}
	
	private static void test2() throws Exception {
		String input = "張";
//		System.out.println(input.getBytes().length);
		System.out.println(bytesToHexString(input.getBytes()));
	}
	
	// bytes轉16進位
	private static String bytesToHexString(byte[] src){  
	    StringBuilder stringBuilder = new StringBuilder("");  
	    if (src == null || src.length <= 0) {  
	        return null;  
	    }  
	    for (int i = 0; i < src.length; i++) {  
	        int v = src[i] & 0xFF;  
	        String hv = Integer.toHexString(v);
	        if (hv.length() < 2) {  
	            stringBuilder.append(0);  
	        }  
	        stringBuilder.append(hv.toUpperCase());
	    }  
	    return stringBuilder.toString();  
	}
	
	private static void test3() throws UnsupportedEncodingException {
		String babel = "一二三";
		System.out.println(babel);
		//Convert string to ByteBuffer:
		ByteBuffer babb = Charset.forName("UTF-8").encode(babel);
		try{
		    //Convert ByteBuffer to String
		    System.out.println(new String(babb.array(), "UTF-8"));
		}
		catch(Exception e){
		    e.printStackTrace();
		}
	}
	
	private static void test4() {
		System.out.println(ETL_Tool_FormatCheck.checkNum("0000123"));
	}
	
	private static void test5() {
		
		String temp = "00000123";
		System.out.println(Integer.valueOf(temp));
		
	}
	
	private static void test6() throws IOException {
		File file = new File("C:/Users/10404003/Desktop/農經/171221/file/600_CF_PARTY_20171206.TXT");
		FileInputStream fis = new FileInputStream(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
		
		String str = "";
		int count = 0;
		while (br.ready()) {
			str = br.readLine();
			count++;
		}
		System.out.println("count = " + count);
	}
	
}
