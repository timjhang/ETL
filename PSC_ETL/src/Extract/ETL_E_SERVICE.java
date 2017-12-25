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
import Bean.ETL_Bean_SERVICE_TEMP_Data;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.ETL_Q_ColumnCheckCodes;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETF_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_E_SERVICE {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	// TODO
	private String[][] checkMapArray = { { "T_3", "COMM_FILE_TYPE" }, // 檔名業務別
			{ "T_4", "COMM_DOMAIN_ID" }, // 金融機構代號
			{ "T_9", "COMM_FILE_TYPE" } // 服務資料_服務類別
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	// TODO
	private List<ETL_Bean_SERVICE_TEMP_Data> dataList = new ArrayList<ETL_Bean_SERVICE_TEMP_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {
			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_SERVICE 抓取checkMaps資料有誤!"); // TODO
			ex.printStackTrace();
		}
	};

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_Service_File(String filePath, String fileTypeName, String upload_no) {
		System.out.println("#######Extrace - ETL_E_SERVICE - Start");

		try {
			// 取得目標檔案File
			List<File> fileList = ETF_Tool_FileReader.getTargetFileList(filePath, fileTypeName);
			System.out.println("共有檔案 " + fileList.size() + " 個！");
			System.out.println("===============");
			for (int i = 0; i < fileList.size(); i++) {
				System.out.println(fileList.get(i).getName());
			}
			System.out.println("===============");

			// 進行檔案處理
			for (int i = 0; i < fileList.size(); i++) {
				// 取得檔案
				File parseFile = fileList.get(i);

				// 檔名
				String fileName = parseFile.getName();
				Date parseStartDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " Start " + parseStartDate);

				// 解析fileName物件
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName);
		

				FileInputStream fis = new FileInputStream(parseFile);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis, "BIG5"));

				// rowCount == 處理行數
				int rowCount = 1; // 從1開始
				// 成功計數
				int successCount = 0;
				// 失敗計數
				int failureCount = 0;

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

					// 檢查整行bytes數(1 + 7 + 8 + 1418 = 1434)
					if (strQueue.getTotalByteLength() != 1434) {
						fileFmtErrMsg = "首錄位元數非預期1434";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"行數bytes檢查", fileFmtErrMsg));
					}

					// 區別瑪檢核(1)
					String typeCode = strQueue.popBytesString(1);
					if (!"1".equals(typeCode)) { // 首錄區別碼檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
						fileFmtErrMsg = "首錄區別碼有誤";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"區別碼", fileFmtErrMsg));
					}

					// 報送單位檢核(7)
					String central_no = strQueue.popBytesString(7);
					if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
						fileFmtErrMsg = "首錄報送單位代碼與檔名不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"報送單位", fileFmtErrMsg));
					}

					// 檔案日期檢核(8)
					String record_date = strQueue.popBytesString(8);
					if (record_date == null || "".equals(record_date.trim())) {
						fileFmtErrMsg = "首錄檔案日期空值";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!record_date.equals(pfn.getRecord_Date_String())) {
						fileFmtErrMsg = "首錄檔案日期與檔名不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
						fileFmtErrMsg = "首錄檔案日期格式錯誤";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					}

					// 保留欄位檢核(1418)
					String keepColumn = strQueue.popBytesString(1418);

					if (!"".equals(fileFmtErrMsg)) {
						failureCount++; // 錯誤計數 + 1
					} else {
						successCount++; // 成功計數 + 1
					}
					rowCount++; // 處理行數 + 1
				}

				if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行
					// 逐行讀取明細資料
					while (br.ready()) {
						lineStr = br.readLine();

						strQueue.setTargetString(lineStr); // queue裝入新String

						// 生成一個Data
						ETL_Bean_SERVICE_TEMP_Data data = new ETL_Bean_SERVICE_TEMP_Data(pfn);
						data.setRow_count(rowCount);

						// 區別碼(1)
						String typeCode = strQueue.popBytesString(1);
						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
							break;
						}
					
						// 整行bytes數檢核(1 + 7 + 11 + 16 + 8 + 6+10+10+20+75+400+400+400+20+50 = 1434) 
						if (strQueue.getTotalByteLength() != 1434) {
							data.setError_mark("Y");
							fileFmtErrMsg = "非預期1434";
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
							
							// 資料bytes不正確, 為格式嚴重錯誤, 跳出迴圈不繼續執行
							break;
						}

						// 區別碼檢核  *
						if (!"2".equals(typeCode)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "區別碼", "非預期"));
						}

						// 本會代號檢核(7) T_4*
						String domain_id = strQueue.popBytesString(7);
						data.setDomain_id(domain_id);
						if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "本會代號", "空值"));
						} else if (!checkMaps.get("T_4").containsKey(domain_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "本會代號", "非預期"));
						}
						
						//	客戶統編					X(11)	*	
						String party_number = strQueue.popBytesString(11);
						data.setParty_number(party_number);
						if(ETL_Tool_FormatCheck.isEmpty(party_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "客戶統編", "空值"));
						}
						
						
						//	服務編號					X(16)	*	
						String service_id = strQueue.popBytesString(16);
						data.setService_id(service_id);
						if(ETL_Tool_FormatCheck.isEmpty(service_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務編號", "空值"));
						}
						
						//X(08)	* 格式yyyyMMdd。需等於首錄中檔案日期。	
						String service_date = strQueue.popBytesString(8);
						if(ETL_Tool_FormatCheck.isEmpty(service_date)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務日期", "空值"));
						
						}else if(ETL_Tool_FormatCheck.checkDate(service_date)) {
							data.setService_date(ETL_Tool_StringX.toUtilDate(service_date));
							if(!service_date.equals(pfn.getRecord_Date_String())) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務日期", "不等於首錄中檔案日期"));
							}
						}else {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務日期", "日期格式錯誤"));

						}

						//	服務時間	待上傳				X(06)	*	
						String service_time = strQueue.popBytesString(6);
						if(ETL_Tool_FormatCheck.isEmpty(service_time)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務時間", "空值"));
						}else if(ETL_Tool_FormatCheck.checkDate(service_time, "HHmmss")) {
							data.setService_time(ETL_Tool_StringX.toTimestamp(service_time,  "HHmmss"));
						}else {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務時間", "時間格式錯誤"));
						}


						
						//	服務類別					X(10)	*T9	
						String service_type = strQueue.popBytesString(10);
						data.setService_type(service_type);
						if (ETL_Tool_FormatCheck.isEmpty(service_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務類別", "空值"));
						} else if (!checkMaps.get("T_9").containsKey(service_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "服務類別", "非預期"));
						}
						
						//	服務管道類別				X(10)		
						String channel_type	= strQueue.popBytesString(10);
						data.setChannel_type(channel_type);
						
						//	服務管道編號				X(20)		
						String channel_id = strQueue.popBytesString(20);
						data.setChannel_id(channel_id);
						
						//	服務參考編號				X(75)		
						String service_reference_number = strQueue.popBytesString(75);
						data.setService_reference_number(service_reference_number);
						
						
						//	資料變更前詳細內容		X(400)		
						String previous_value= strQueue.popBytesString(400);
						data.setPrevious_value(previous_value);
						
						//	資料變更後詳細內容		X(400)		
						String new_value = strQueue.popBytesString(400);
						data.setNew_value(new_value);
						
						//	服務詳細內容				X(400)		
						String service_description 		= strQueue.popBytesString(400);
						data.setService_description(service_description);
						
						//	服務執行分行				X(20)		
						String execution_branch_code = strQueue.popBytesString(20);
						data.setExecution_branch_code(execution_branch_code);

						//	執行行員代號				X(50)		
						String executer_id = strQueue.popBytesString(50);
						data.setExecuter_id(executer_id);
					
						// data list 加入一個檔案
						addData(data);

						if ("Y".equals(data.getError_mark())) {
							failureCount++;
						} else {
							successCount++;
						}
						rowCount++; // 處理行數 + 1
					}
					
					insert_SERVICE_TEMP_Datas();

					// 尾錄檢查
					if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 1411 = 1434)
						if (strQueue.getTotalByteLength() != 1434) { // TODO
							fileFmtErrMsg = "尾錄位元數非預期1434";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別碼檢核(1) 經"逐行讀取檔案"區塊, 若無嚴重錯誤應為3, 此處無檢核

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "尾錄報送單位代碼與檔名不符";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}

						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (record_date == null || "".equals(record_date.trim())) {
							fileFmtErrMsg = "尾錄檔案日期空值";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!record_date.equals(pfn.getRecord_Date_String())) {
							fileFmtErrMsg = "尾錄檔案日期與檔名不符";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "尾錄檔案日期格式錯誤";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 總筆數檢核(7)
						String totalCount = strQueue.popBytesString(7);
						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) == (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount - 2), "總筆數", fileFmtErrMsg));
						}

						// 保留欄檢核(1411)
						String keepColumn = strQueue.popBytesString(1411);

						if (!"".equals(fileFmtErrMsg)) {
							failureCount++;
						} else {
							successCount++;
						}

					}

					// 程式統計檢核
					if (rowCount != (successCount + failureCount)) {
						fileFmtErrMsg = "總筆數 <> 成功比數 + 失敗筆數";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"程式檢核", fileFmtErrMsg));
					}
					// 多餘行數檢查
					if (br.ready()) {
						fileFmtErrMsg = "出現多餘行數";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案總行數", fileFmtErrMsg));
						rowCount++;
					}

					fis.close();
					Date parseEndDate = new Date(); // 開始執行時間
					System.out.println("解析檔案： " + fileName + " End " + parseEndDate);

					// Error_Log寫入DB
					errWriter.insert_Error_Log();

					// ETL_Log寫入DB
					ETL_P_Log.write_ETL_Log(pfn.getCentral_No(), pfn.getRecord_Date(), pfn.getFile_Type(),
							pfn.getFile_Name(), upload_no, "E", parseStartDate, parseEndDate, rowCount, successCount,
							failureCount, pfn.getFileName());

				}

			}

		} catch (Exception ex) {

			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_SERVICE - End"); //

	}

	// List增加一個data
	// TODO
	private void addData(ETL_Bean_SERVICE_TEMP_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_SERVICE_TEMP_Datas();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}

	private void insert_SERVICE_TEMP_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_SERVICE - insert_SERVICE_TEMP_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_SERVICE_TEMP(?)}"); // 呼叫PARTY_PHONE寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_SERVICE_TEMP"); // DB2 array type 
		insertAdapter.setCreateStructTypeName("T_SERVICE_TEMP"); // DB2 type 
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_SERVICE_TEMP_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_SERVICE_TEMP_Datas 發生錯誤");
		}
	}

}
