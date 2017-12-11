package DB;

import java.sql.Timestamp;

public class ETL_P_Log {
	
	// ETL_Log格式
	// 1	報送單位	CENTRAL_NO
	// 2	檔案日期	RECORD_DATE
	// 3	檔名業務別	FILE_TYPE
	// 4	檔案名稱	FILE_NAME
	// 5	上傳批號	UPLOAD_NO
	// 6	步驟	STEP_TYPE
	// 7	執行開始日期時間	START_DATETIME
	// 8	執行結束日期時間	END_DATETIME
	// 9	總筆數	TOTAL_CNT
	// 10	成功筆數	SUCCESS_CNT
	// 11	失敗筆數	FAILED_CNT
	// 12	來源檔案	SRC_FILE
	public static void write_ETL_Log(String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE, String FILE_NAME, String UPLOAD_NO, 
			String STEP_TYPE, java.util.Date START_DATETIME, java.util.Date END_DATETIME, int TOTAL_CNT, int SUCCESS_CNT,
			int FAILED_CNT, String SRC_FILE) {
		
		// RECORD_DATE 轉  java.sql.Date
		// START_DATETIME 轉  java.sql.Timestamp
		// END_DATETIME 轉   java.sql.Timestamp
	}
	
	// Error_Log格式
	// 1	報送單位	CENTRAL_NO
	// 2	檔案日期	RECORD_DATE
	// 3	檔名業務別	FILE_TYPE
	// 4	檔案名稱	FILE_NAME
	// 5	上傳批號	UPLOAD_NO
	// 6	步驟	STEP_TYPE
	// 7	行數	ROW_COUNT
	// 8	欄位中文名稱	FIELD_NAME
	// 9	錯誤描述	ERROR_DESCRIPTION
	// 10	來源檔案	SRC_FILE
	public static void write_Error_Log(String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE, String FILE_NAME, String UPLOAD_NO,
			String STEP_TYPE, String ROW_COUNT, String FIELD_NAME, String ERROR_DESCRIPTION, String SRC_FILE) {
		
		// RECORD_DATE 轉  java.sql.Date
	}

}
