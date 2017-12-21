package Tool;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_Tool_StringX {

	/**
	 * 字串轉換為Util Date，格式預設為yyyyMMdd
	 * @param dateStr 轉換字串
	 * @return Util Date
	 * @throws Exception
	 */
	public static Date toUtilDate(String dateStr)throws Exception{
		return new SimpleDateFormat("yyyyMMdd").parse(dateStr);
	}
	
	/**
	 * 字串格式轉換為 :數字右靠左補0
	 * 
	 * @param numStr : 需更改格式的字串
	 * @param size : 要轉換的長度
	 * @return : 回傳轉換後的
	 * @throws Exception
	 */
	public static String formatNumber(String numStr, int size) throws Exception {
		if (numStr == null) {
			throw new Exception("字串不可為NULL");
		}

		if (numStr.length() > size) {
			throw new Exception("長度不合");
		}

		String zero = "";
		for (int i = 0; i < (size - numStr.length()); i++) {
			zero = zero + "0";
		}
		return zero + numStr;
	}

	/**
	 * 字串轉BigDecimal 預設格式"000000000000.00"
	 * 
	 * @param strDecimal : 進行轉換字串
	 * @return : 回傳轉換後的
	 * @throws Exception
	 */
	public static BigDecimal strToBigDecimal(String strDecimal) throws ParseException {
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		symbols.setDecimalSeparator('.');
		String pattern = "000000000000.00";
		DecimalFormat decimalFormat = new DecimalFormat(pattern, symbols);
		decimalFormat.setParseBigDecimal(true);
		return ((BigDecimal) decimalFormat.parse(strDecimal));
	}

	/**
	 * 字串轉BigDecimal 預設整數位為12位
	 * 
	 * @param strDecimal : 進行轉換字串
	 * @param newScale 希望擁有幾位的小數位
	 * @return 轉換後的BigDecimal
	 * @throws ParseException
	 */
	public static BigDecimal strToBigDecimal(String strDecimal, int newScale) throws ParseException {
		
		strDecimal = strDecimal.substring(0, strDecimal.length() - newScale) + "."
				+ strDecimal.substring(strDecimal.length() - newScale, strDecimal.length());

		return strToBigDecimal(strDecimal);

	}

	/**
	 * 字串轉BigDecimal
	 * @param strDecimal : 進行轉換字串
	 * @param pattern : 規定的格式
	 * @return :回傳轉換後的BigDecimal
	 * @throws ParseException
	 */
	public static BigDecimal strToBigDecimal(String strDecimal, String pattern) throws ParseException {
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		symbols.setDecimalSeparator('.');
		DecimalFormat decimalFormat = new DecimalFormat(pattern, symbols);
		decimalFormat.setParseBigDecimal(true);
		return ((BigDecimal) decimalFormat.parse(strDecimal));
	}

	/**
	 * BigDecimal 轉 字串 預設格式"#.#"
	 * 
	 * @param bigDecimal :要轉換的BigDecimal
	 * @return 回傳轉換後的bigDecimal
	 * @throws Exception
	 */
	public static String bigDecimalToStr(BigDecimal bigDecimal) throws Exception {
		if (bigDecimal == null) {
			throw new Exception("BigDecimal不可為NULL");
		}

		DecimalFormat df = new DecimalFormat("#.00");
		return df.format(bigDecimal);
	}

	/**
	 * 字串格式轉換為:文字左靠右補空白
	 * 
	 * @param str :字串
	 * @param size :全部長度
	 * @return :回傳轉換後的
	 * @throws Exception
	 */
	public static String FormatString(String str, int size) throws Exception {

		if (str == null) {
			throw new Exception("字串不可為NULL");
		}

		if (str.length() > size) {
			throw new Exception("長度不合");
		}

		byte[] byteArr = str.getBytes();
		byte[] formByteArr = new byte[size];

		for (int i = 0; i < size; i++) {
			formByteArr[i] = 32;
		}

		System.arraycopy(byteArr, 0, formByteArr, 0, byteArr.length);
		String formStr = new String(formByteArr);

		return formStr;
	}

	public static void main(String[] argv) throws Exception {

		System.out.println(strToBigDecimal("5000", 2));
		// BigDecimal BigDecimal = new BigDecimal("123.0001");
		// //System.out.println(bigDecimalToStr(BigDecimal));
		//
		// String aaa=bigDecimalToStr(BigDecimal,"#.000");
		// System.out.println("aaa:"+aaa);
	}

}
