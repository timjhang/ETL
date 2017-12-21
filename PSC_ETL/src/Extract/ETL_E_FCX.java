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
import Bean.ETL_Bean_FCX_TEMP_Data;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_Q_ColumnCheckCodes;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETF_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;

public class ETL_E_FCX {
	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	// TODO
	private String[][] checkMapArray = { { "T_3", "COMM_FILE_TYPE" }, // 檔名業務別
			{ "T_4", "COMM_DOMAIN_ID" }, // 金融機構代號
			{ "T_8", "COMM_NATIONALITY_CODE" }, // 國籍
			{ "T_12", "FCX_DIRECTION" }, // 外幣現鈔買賣資料_結構或結售
			{ "T_13", "COMM_CURRENCY_CODE" }, // 幣別
			{ "T_15", "COMM_NATIONALITY_CODE" }, // 外幣現鈔買賣資料_交易管道類別
			{ "T_16", "FCX_CHANNEL_TYPE" } };

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	// TODO
	private List<ETL_Bean_FCX_TEMP_Data> dataList = new ArrayList<ETL_Bean_FCX_TEMP_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_FCX 抓取checkMaps資料有誤!"); // TODO
			ex.printStackTrace();
		}
	};

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_Party_Phone_File(String filePath, String fileTypeName, String upload_no) {
		System.out.println("#######Extrace - ETL_E_FCX - Start"); // TODO
		try {
			// 取得目標檔案File
			List<File> fileList = ETF_Tool_FileReader.getTargetFileList_noFT(filePath, fileTypeName);

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

					// 檢查整行bytes數(1 + 7 + 8 + 235 = 251)
					if (strQueue.getTotalByteLength() != 251) {
						fileFmtErrMsg = "首錄位元數非預期251";
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

					// 保留欄位檢核(235)
					String keepColumn = strQueue.popBytesString(235);

					if (!"".equals(fileFmtErrMsg)) {
						failureCount++; // 錯誤計數 + 1
					} else {
						successCount++; // 成功計數 + 1
					}
					rowCount++; // 處理行數 + 1

				}

				// 逐行讀取檔案
				if ("".equals(fileFmtErrMsg)) // 沒有嚴重錯誤時進行
					while (br.ready()) {
						lineStr = br.readLine();

						strQueue.setTargetString(lineStr); // queue裝入新String

						// 生成一個Data
						ETL_Bean_FCX_TEMP_Data data = new ETL_Bean_FCX_TEMP_Data(pfn);

						// 區別碼(1)
						String typeCode = strQueue.popBytesString(1);
						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
							break;
						}

						// 整行bytes數檢核(1+7+11+80+8+2+20+8+14+1+3+12+2+10+20+50= 249) // TODO
						if (strQueue.getTotalByteLength() != 249) {
							data.setError_mark("Y");
							fileFmtErrMsg = "非預期249";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));

							// 資料bytes不正確, 為格式嚴重錯誤, 跳出迴圈不繼續執行
							break;
						}

						// 區別碼檢核 c-1*
						if (!"2".equals(typeCode)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", "非預期"));
						}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
						//FCX


						// 整行bytes數檢核(1+7+11+80+8+2+20+8+14+1+3+12+2+10+20+50 = 249) 
											if (strQueue.getTotalByteLength() != 249) {
												data.setError_mark("Y");
												fileFmtErrMsg = "非預期249";
												errWriter.addErrLog(
														new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
												
												// 資料bytes不正確, 為格式嚴重錯誤, 跳出迴圈不繼續執行
												break;
											}


							//區別碼					X(01)		*	String typecode 							= strQueue.popBytesString(1);
							//本會代號				X(07)  T_4  *	String domain_id 							= strQueue.popBytesString(7);
							//顧客統編				X(11)       *	String party_number							= strQueue.popBytesString(11);
							//顧客姓名				X(80)       	String nationality_code						= strQueue.popBytesString(80);
							//顧客生日				X(08)       	String date_of_birth 						= strQueue.popBytesString(8);
							//顧客國籍				X(02)  T_8     	String nationality_code 					= strQueue.popBytesString(2);
							//交易編號				X(20)       *	String transaction_id 						= strQueue.popBytesString(20);
							//交易日期				X(08)       *	String transaction_date 					= strQueue.popBytesString(8);
							//實際交易時間			X(14)       *	String transaction_time 					= strQueue.popBytesString(14);
							//結構或結售				X(01)  T_12 *	String direction 							= strQueue.popBytesString(1);
							//交易幣別				X(03)  T_13 *	String currency_code 						= strQueue.popBytesString(3);
							//交易金額				9(12)  V99  *	String amount 								= strQueue.popBytesString(12);
							//申報國別				X(02)  T_15 *	String ordering_customer_country 			= strQueue.popBytesString(2);
							//交易管道類別			X(10)  T_16     String channel_type 						= strQueue.popBytesString(10);
							//交易執行分行			X(20)       	String execution_branch_code 				= strQueue.popBytesString(20);
							//交易執行者代號			X(50)       	String executer_id 							= strQueue.popBytesString(50);
						
						
						
						
						
						
						
						
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

						addData(data);

						if ("Y".equals(data.getError_mark())) {
							failureCount++;
						} else {
							successCount++;
						}
						rowCount++; // 處理行數 + 1
					}

			}

		} catch (Exception ex) {

			ex.printStackTrace();
		}
		System.out.println("#######Extrace - ETL_E_FCX - End"); // TODO

	}

	// List增加一個data
	private void addData(ETL_Bean_FCX_TEMP_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_FCX_TEMP_Datas();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}

	private void insert_FCX_TEMP_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_FCX - insert_FCX_TEMP_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_FCX_TEMP(?)}"); // 呼叫PARTY_PHONE寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("T_FCX_TEMP"); // DB2 type - PARTY_PHONE
		insertAdapter.setCreateStructTypeName("A_FCX_TEMP"); // DB2 array type - PARTY_PHONE
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_FCX_TEMP_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_FCX_TEMP_Datas 發生錯誤");
		}
	}

}
