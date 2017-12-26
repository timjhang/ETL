package Tool;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_Tool_FormatCheck {
	// ETL 檢核工具
	
	// 是否為空值檢核工具  (空值:true\非空值:false)
	public static boolean isEmpty(String input) {
		if (input == null || "".equals(input.trim())) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * 檢測字串是否符合Timestamp格式 yyyyMMddhhmmss
	 * @param dateStr 檢測字串
	 * @return true 成功 / false 失敗
	 */
	public static boolean checkTimestamp(String dateStr) {
		boolean isVaild = false;
		
		if (isEmpty(dateStr)) {
			return isVaild;
		}
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
			sdf.setLenient(false); // 過濾不合理日期
			sdf.parse(dateStr);
			isVaild = true;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex.getMessage());
			return isVaild;
		}
		
		return isVaild;
	}
	
	// 日期格式檢核工具  (通過檢核:true\檢核失敗:false)
	public static boolean checkDate(String dateStr) {
		
		if (isEmpty(dateStr)) {
			return false;
		}
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			sdf.setLenient(false); // 過濾不合理日期
			Date realDate = sdf.parse(dateStr);
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			return false;
		}
		
		return true;
	}
	

	// 日期格式檢核工具  (通過檢核:true\檢核失敗:false)  String pattern 格式 ex: "yyyy-MM-dd HH:mm:ss.SSSSSS"
	public static boolean checkDate(String inputString, String pattern)
	{ 
	    SimpleDateFormat format = new SimpleDateFormat(pattern);
	    try{
	       format.parse(inputString);
	       return true;
	    }catch(Exception e)
	    {
	        return false;
	    }
	}
	
	// 數字檢核工具  (通過檢核:true\檢核失敗:false)
	public static boolean checkNum(String numStr) {
		if (isEmpty(numStr)) {
			return false;
		}
		
		try {
			long realNum = Long.parseLong(numStr);
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			return false;
		}
		
		return true;
	}

}
