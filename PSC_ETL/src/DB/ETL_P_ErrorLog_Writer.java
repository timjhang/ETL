package DB;

import java.util.ArrayList;
import java.util.List;

import Bean.ETL_P_ErrorLog_Data;
import Profile.ETL_Profile;

public class ETL_P_ErrorLog_Writer {
	// Error Log寫入工具
	
	// Error寫入域值
	private int stageLimit = ETL_Profile.ErrorLog_Stage; 
//	private int stageLimit = 10000; // for test
	
	// Error Log計數
	private int errorLogCount = 0;
	
	// Error Log儲存List
	private List<ETL_P_ErrorLog_Data> errorLogList = new ArrayList<ETL_P_ErrorLog_Data>();
	
	public void addErrLog(ETL_P_ErrorLog_Data errorLog) throws Exception {
		if (errorLog == null) {
			throw new Exception("無接收到ErrorLog實體!");
//			System.out.println("無接收到實際Log");
		}
		
		errorLogList.add(errorLog);
		errorLogCount++;
		
		if (errorLogCount == stageLimit) {
			insert_ErrorLog(errorLogList);
			errorLogCount = 0;
		}
	}
	
	// Error Log寫入
	private void insert_ErrorLog(List<ETL_P_ErrorLog_Data> errorLogList) {
		// 呼叫Ian Error Log寫入method
	}
	
}
