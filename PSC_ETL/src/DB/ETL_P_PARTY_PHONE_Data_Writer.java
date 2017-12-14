package DB;

import java.util.ArrayList;
import java.util.List;

import Bean.ETL_Bean_PARTY_PHONE_Data;
import Profile.ETL_Profile;

public class ETL_P_PARTY_PHONE_Data_Writer {
	// Data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;
	
	// Data Log計數
	private int dataCount = 0;
	
	// Data儲存List
	private List<ETL_Bean_PARTY_PHONE_Data> dataList = new ArrayList<ETL_Bean_PARTY_PHONE_Data>();
	
	public void addData(ETL_Bean_PARTY_PHONE_Data data) throws Exception {
		if (data == null) {
			throw new Exception("無接收到ETL_Bean_PARTY_PHONE_Data實體!");
//			System.out.println("無接收到實際Log");
		}
		
		dataList.add(data);
		dataCount++;
		
		// 若超過域值  先行寫入DB
		if (dataCount == stageLimit) {
			insert_Datas(dataList);
			dataCount = 0; // 計數歸0
			dataList.clear(); // 清空list
		}
	}
	
	// Data 寫入
	private void insert_Datas(List<ETL_Bean_PARTY_PHONE_Data> dataList) {
		
	}
	
}
