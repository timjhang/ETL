import java.text.ParseException;

import Tool.ETL_Tool_ParseFileName;


public class Test {

	public static void main(String[] argv) {
		try {
			test1();
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
		ETL_Tool_ParseFileName one = new ETL_Tool_ParseFileName("018_FR_PARTY_20171211.txt");
		System.out.println(one.getFileName());
		System.out.println(one.getCentral_No());
		System.out.println(one.getFile_Type());
		System.out.println(one.getRecord_Date());
	}
	
}
