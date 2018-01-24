package Extract;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_PARTY_PARTY_REL_Data;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.ETL_Q_ColumnCheckCodes;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_E_PARTY_PARTY_REL {
	
	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;
	
	// 欄位檢核用陣列
	private String[][] checkMapArray =
		{
			{"c-2", "COMM_DOMAIN_ID"}, // 本會代號
			{"c-4", "PARTY_PARTY_REL_CHANGE_CODE"}, // 異動代號
			{"c-5", "PARTY_PARTY_REL_RELATION_TYPE_CODE"}, // 顧客關係種類
			{"c-11", "COMM_NATIONALITY_CODE"} // 關係人-國籍
		};
	
	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;
	
	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;
	
	// list data筆數
	private int dataCount = 0;
	
	// Data儲存List
	private List<ETL_Bean_PARTY_PARTY_REL_Data> dataList = new ArrayList<ETL_Bean_PARTY_PARTY_REL_Data>();
	
	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {
			
			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);
			
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_PARTY_PARTY_REL 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}
	};
	
	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_Party_Party_Rel_File(String filePath, String fileTypeName,
			String batch_no, String exc_central_no, Date exc_record_date, String upload_no, String program_no) {
		
		System.out.println("#######Extrace - ETL_E_PARTY_PARTY_REL - Start");
		
		try {
			
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(
					batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no, "S", "", "", new Date(), null);
			
			// 處理Party_Phone錯誤計數
			int detail_ErrorCount = 0;
			
			// 取得目標檔案File
			List<File> fileList = ETL_Tool_FileReader.getTargetFileList(filePath, fileTypeName);
			
			System.out.println("共有檔案 " + fileList.size() + " 個！");
			System.out.println("===============");
			for (int i = 0; i < fileList.size(); i++) {
				System.out.println(fileList.get(i).getName());
			}
			System.out.println("===============");
			
			// 進行檔案處理
			for (int i = 0 ; i < fileList.size(); i++) {
				// 取得檔案
				File parseFile = fileList.get(i);
				
				// 檔名
				String fileName = parseFile.getName();
				Date parseStartDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " Start " + parseStartDate);
				
				// 解析fileName物件
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName);
				// 業務別非預期, 不進行解析
				if (pfn.getFile_Type() == null) {
					System.out.println("##" + pfn.getFileName() + " 處理業務別非預期，不進行解析！");
					continue;
				}
				// 設定批次編號
				pfn.setBatch_no(batch_no);
				
//				System.out.println(parseFile.getAbsoluteFile()); // test
				FileInputStream fis = new FileInputStream(parseFile);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
				
				// rowCount == 處理行數
				int rowCount = 1; // 從1開始
				// 成功計數
				int successCount = 0;
				// 失敗計數
				int failureCount = 0;
				// 尾錄總數
				int iTotalCount = 0;
				
				// 嚴重錯誤訊息變數
				String fileFmtErrMsg = ""; 
				
				String lineStr = ""; // 行字串暫存區
				
				// ETL_字串處理Queue
				ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue();
				// ETL_Error Log寫入輔助工具
				ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();
				
				// 首錄檢查
				if (br.ready()) {
					lineStr = br.readLine();
					
					// 注入首錄字串
					strQueue.setTargetString(lineStr);
					
					// 檢查整行bytes數(1 + 7 + 8 + 187 = 203)
					if (strQueue.getTotalByteLength() != 203) {
						fileFmtErrMsg = "首錄位元數非預期203";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
					}
					
					// 區別瑪檢核(1)
					String typeCode = strQueue.popBytesString(1);
					if (!"1".equals(typeCode)) { // 首錄區別碼檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
						fileFmtErrMsg = "首錄區別碼有誤";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
					}
					
					// 報送單位檢核(7)   
					String central_no = strQueue.popBytesString(7);
					if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息 
						fileFmtErrMsg = "首錄報送單位代碼與檔名不符";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
					}
					
					// 檔案日期檢核(8)
					String record_date = strQueue.popBytesString(8);
					if (record_date == null || "".equals(record_date.trim())) {
						fileFmtErrMsg = "首錄檔案日期空值";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
					} else if (!record_date.equals(pfn.getRecord_Date_String())) {
						fileFmtErrMsg = "首錄檔案日期與檔名不符";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
					} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
						fileFmtErrMsg = "首錄檔案日期格式錯誤";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
					}
					
					// 保留欄位檢核(187)
					String keepColumn = strQueue.popBytesString(187);
					
					if (!"".equals(fileFmtErrMsg)) {
						failureCount++; // 錯誤計數 + 1
					} else {
						successCount++; // 成功計數 + 1
					}
					rowCount++; // 處理行數  + 1
				}
				
				// 逐行讀取檔案
				if ("".equals(fileFmtErrMsg)) // 沒有嚴重錯誤時進行
				while (br.ready()) {
					
					lineStr = br.readLine();
//					System.out.println(lineStr); // test
					strQueue.setTargetString(lineStr); // queue裝入新String
					
					// 生成一個Data
					ETL_Bean_PARTY_PARTY_REL_Data data = new ETL_Bean_PARTY_PARTY_REL_Data(pfn);
					// 寫入資料行數
					data.setRow_count(rowCount);
					
					// 區別碼(1)
					String typeCode = strQueue.popBytesString(1);
					if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
						break;
					}

					// 整行bytes數檢核(203)
					if (strQueue.getTotalByteLength() != 203) {
						data.setError_mark("Y");
						fileFmtErrMsg = "非預期203";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						
						// 資料bytes不正確, 為格式嚴重錯誤, 跳出迴圈不繼續執行
						break;
					}
					
					// 區別碼檢核 c-1*
					if (!"2".equals(typeCode)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "區別碼", "非預期"));
					}
					
					// 本會代號檢核(7) c-2*
					String domain_id = strQueue.popBytesString(7);
					data.setDomain_id(domain_id);
					if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "本會代號", "空值"));
					} else if (advancedCheck && !checkMaps.get("c-2").containsKey(domain_id)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "本會代號", "非預期"));
					}
					
					// 本會(行)客戶統編 c-*3(11)
					String party_number = strQueue.popBytesString(11);
					data.setParty_number(party_number);
					if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "本會(行)客戶統編", "空值"));
					}
					
					// 異動代號 c-*4(1)
					String change_code = strQueue.popBytesString(1);
					data.setChange_code(change_code);
					if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "異動代號", "空值"));
					} else if (advancedCheck && !checkMaps.get("c-4").containsKey(change_code)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "異動代號", "非預期"));
					}
					
					// 顧客關係種類 c-*5(2)
					String relation_type_code = strQueue.popBytesString(2);
					data.setRelation_type_code(relation_type_code);
					if (ETL_Tool_FormatCheck.isEmpty(relation_type_code)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "顧客關係種類", "空值"));
					} else if (advancedCheck && !checkMaps.get("c-5").containsKey(relation_type_code)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "顧客關係種類", "非預期"));
					}
					
					// 顧客關係描述 c-6(40)
					String relation_description = strQueue.popBytesString(40);
					data.setRelation_description(relation_description);
					
					// 關係人-統編  c-*7(11)
					String party_key_2 = strQueue.popBytesString(11);
					data.setParty_key_2(party_key_2);
					if (ETL_Tool_FormatCheck.isEmpty(party_key_2)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "關係人-統編", "空值"));
					}
					
					// 關係人-姓氏 c-8(40)
					String party_first_name_1 = strQueue.popBytesString(40);
					data.setParty_first_name_1(party_first_name_1);
					
					// 關係人-名字 c-9(80)
					String party_last_name_1 = strQueue.popBytesString(80);
					data.setParty_last_name_1(party_last_name_1);
					
					// 關係人-出生年月日 c-10(8)
					String date_of_birth = strQueue.popBytesString(8);
					data.setDate_of_birth(ETL_Tool_StringX.toUtilDate(date_of_birth));
					if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(date_of_birth) && !ETL_Tool_FormatCheck.checkDate(date_of_birth)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "關係人-出生年月日", "格式錯誤"));
					}
					
					// 關係人-國籍 c-11(2)
					String nationality_code = strQueue.popBytesString(2);
					data.setNationality_code(nationality_code);
					if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(nationality_code) && !checkMaps.get("c-11").containsKey(nationality_code)) {
						data.setError_mark("Y");
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "關係人-國籍", "非預期"));
					}
					
					// data list 加入一個檔案
					addData(data);
					
					if ("Y".equals(data.getError_mark())) {
						failureCount++;
					} else {
						successCount++;
					}
					rowCount++; // 處理行數 + 1
				}
				
				// Party_Party_Rel_Data寫入DB
				insert_Party_Party_Rel_Datas();
				
				// 尾錄檢查
				if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行
					
					// 整行bytes數檢核 (1 + 7 + 8 + 7 + 180 = 203)
					if (strQueue.getTotalByteLength() != 203) {
						fileFmtErrMsg = "尾錄位元數非預期203";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
					}
					
					// 區別碼檢核(1) 經"逐行讀取檔案"區塊, 若無嚴重錯誤應為3, 此處無檢核
					
					// 報送單位檢核(7)
					String central_no = strQueue.popBytesString(7);
					if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
						fileFmtErrMsg = "尾錄報送單位代碼與檔名不符";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
					}
					
					// 檔案日期檢核(8)
					String record_date = strQueue.popBytesString(8);
					if (record_date == null || "".equals(record_date.trim())) {
						fileFmtErrMsg = "尾錄檔案日期空值";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
					} else if (!record_date.equals(pfn.getRecord_Date_String())) {
						fileFmtErrMsg = "尾錄檔案日期與檔名不符";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
					} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
						fileFmtErrMsg = "尾錄檔案日期格式錯誤";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
					}
					
					// 總筆數檢核(7)
					String totalCount = strQueue.popBytesString(7);
					iTotalCount = ETL_Tool_StringX.toInt(totalCount);
					if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
						fileFmtErrMsg = "尾錄總筆數格式錯誤";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
					} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
						fileFmtErrMsg = "尾錄總筆數與統計不符";
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
					}
					
					// 保留欄檢核(180)
					String keepColumn = strQueue.popBytesString(180);
					
					if (!"".equals(fileFmtErrMsg)) {
						failureCount++;
					} else {
						successCount++;
					}
					
				}
				
				// 程式統計檢核 
				if (rowCount != (successCount + failureCount)) {
					fileFmtErrMsg = "總筆數 <> 成功比數 + 失敗筆數";
					errWriter.addErrLog(
							new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "程式檢核", fileFmtErrMsg));
				}
				// 多餘行數檢查
				if (br.ready()) {
					fileFmtErrMsg = "出現多餘行數";
					errWriter.addErrLog(
							new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案總行數", fileFmtErrMsg));
					rowCount++;
				}
				
				fis.close();
				Date parseEndDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " End " + parseEndDate);
				
				// Error_Log寫入DB
				errWriter.insert_Error_Log();
				
				// ETL_Log寫入DB
				ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), pfn.getRecord_Date(), pfn.getFile_Type(), pfn.getFile_Name(), upload_no,
						"E", parseStartDate, parseEndDate, iTotalCount, successCount, failureCount, pfn.getFileName());
				
				// 累加PARTY_PHONE處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;
			}
			
			// 執行結果
			String exe_result;
			// 執行結果說明
			String exe_result_description;
			
			if (detail_ErrorCount == 0) {
				exe_result = "Y";
				exe_result_description = "檔案轉入檢核無錯誤";
			} else {
				exe_result = "N";
				exe_result_description = "錯誤資料筆數: " + detail_ErrorCount;
			}
			
			// 處理後更新ETL_Detail_Log  
//			ETL_P_Log.update_ETL_Detail_Log(
//					batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
//					"E", exe_result, exe_result_description, new Date());
		
		} catch (Exception ex) {
			
			ex.printStackTrace();
		}
		
		System.out.println("#######Extrace - ETL_E_PARTY_PARTY_REL - End");
	}
	
	// List增加一個data
	private void addData(ETL_Bean_PARTY_PARTY_REL_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;
		
		if (dataCount == stageLimit) {
			insert_Party_Party_Rel_Datas();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}
	
	// 將PARTY_PHONE資料寫入資料庫
	private void insert_Party_Party_Rel_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_PARTY_PARTY_REL - insert_Party_Party_Rel_Datas 無寫入任何資料");
			return;
		}
		
		InsertAdapter insertAdapter = new InsertAdapter(); 
		insertAdapter.setSql("{call SP_INSERT_PARTY_PARTY_REL_TEMP(?)}"); // 呼叫PARTY_PHONE寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_PARTY_PARTY_REL"); // DB2 array type - PARTY_PHONE
		insertAdapter.setCreateStructTypeName("T_PARTY_PARTY_REL"); // DB2 type - PARTY_PHONE
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage);  // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);
		
		if (isSuccess) {
			System.out.println("insert_Party_Party_Rel_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_Party_Party_Rel_Datas 發生錯誤");
		}
	}
	
	public static void main(String[] argv) {
		ETL_E_PARTY_PARTY_REL one = new ETL_E_PARTY_PARTY_REL();
		String filePath = "C:/Users/10404003/Desktop/農經/2017/171228";
		String fileTypeName = "PARTY_PARTY_REL";
		one.read_Party_Party_Rel_File(filePath, fileTypeName,
				"ETL00001", "080", new Date(), "001", "ETL_E_PARTY_PARTY_REL");
	}
	
}
