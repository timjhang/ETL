package External;

public class ETL_Rerun {
	
	// 確認是否可執行rerun
	public static boolean checkRerunOK(String[] errorMsg) {
		
		String errorType = "";
		
		// test code Start
		if (errorMsg != null && errorMsg.length > 0) {
			errorType = errorMsg[0];
		}
		// test code End
		
		if (!"".equals(errorType)) {
			
			if ("E1".equals(errorType)) {
				errorMsg[0] = "E1";
				errorMsg[1] = "正在進行rerun作業, 執行失敗。";
				errorMsg[2] = "";
			} else if ("E2".equals(errorType)) {
				errorMsg[0] = "E2";
				errorMsg[1] = "正在進行每日ETL作業, 執行失敗。";
				errorMsg[2] = "";
			} else {
				errorMsg[0] = "EE";
				errorMsg[1] = "執行發生系統錯誤, 執行失敗。";
				errorMsg[2] = "runtime exception ...";
			}
			
			return false;
		}
		
		errorMsg[0] = "";
		errorMsg[1] = "";
		errorMsg[2] = "";

		return true;
	}
	
	// 執行rerun作業
	public static boolean executeRerun(String central_No, String[] rerunRecordDates, String personID, String[] errorMsg) {
		
		String errorType = "";
		
		// test code Start
		if (errorMsg != null && errorMsg.length > 0) {
			errorType = errorMsg[0];
		}
		// test code End
		
		if (!ETL_Rerun.checkRerunOK(errorMsg)) {
			return false;
		}
		
		errorMsg[0] = "";
		errorMsg[1] = "";
		errorMsg[2] = "";

		return true;
	}
	
//	public static void main(String[] argv) {
//		
//		/** 測試  是否可進行ETL Rerun **/
//		String[] errorMsg = new String[3]; // 必須
//		
//		errorMsg[0] = ""; // 測試用(正式使用不須)
//		
//		if (!ETL_Rerun.checkRerunOK(errorMsg)) {
//			// 無法正常執行, 印出錯誤訊息
//			System.out.println(errorMsg[0]); // 錯誤代碼
//			System.out.println(errorMsg[1]); // 錯誤中文說明(for IT)
//			System.out.println(errorMsg[2]); // Exception Content
//			System.out.println("確認失敗");
//		} else {
//			// 確認ok可以正常執行
//			System.out.println("確認OK!!");
//		}
//		
//		/** 測試  進行ETL Rerun **/
//		String central_No = "018";
//		String[] rerunRecordDates = {"20180221", "20180222", "20180223"};
//		String userID = "10508001";
//		String[] errorMsg2 = new String[3]; // 必須
//		errorMsg2[0] = "EE"; // 測試用(正式使用不須)
//		
//		if (!ETL_Rerun.executeRerun(central_No, rerunRecordDates, userID, errorMsg2)) {
//			// 無法正常執行, 印出錯誤訊息
//			System.out.println(errorMsg[0]); // 錯誤代碼
//			System.out.println(errorMsg[1]); // 錯誤中文說明(for IT)
//			System.out.println(errorMsg[2]); // Exception Content
//			System.out.println("執行失敗");
//		} else {
//			// 確認ok可以正常執行
//			System.out.println("執行OK!!");
//		}
//	}

}
