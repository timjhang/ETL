import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;

import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;


public class Test {

	public static void main(String[] argv) {
		try {
//			test1();
//			test2();
//			test3();
			test4();
		} catch (Exception ex) {
			ex.printStackTrace();
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
	
}
