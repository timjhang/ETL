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
