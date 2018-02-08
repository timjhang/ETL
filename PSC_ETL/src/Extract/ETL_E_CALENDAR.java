package Extract;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_CALENDAR_TEMP_Data;
import Bean.ETL_Bean_ErrorLog_Data;
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

public class ETL_E_CALENDAR {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "T_5", "CALENDAR_IS_BUSINESS_DAY" },
			{ "comm_file_type", "COMM_FILE_TYPE" } };

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	private List<ETL_Bean_CALENDAR_TEMP_Data> dataList = new ArrayList<ETL_Bean_CALENDAR_TEMP_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_CALENDAR 抓取checkMaps資料有誤!"); // TODO
			ex.printStackTrace();
		}
	};

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_CALENDAR_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception { // TODO V3 {

		System.out.println("#######Extrace - ETL_E_CALENDAR - Start");

		try {

			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理錯誤計數
			int detail_ErrorCount = 0;

			// 程式執行錯誤訊息
			String processErrMsg = "";

			// 取得目標檔案File.getTargetFileList(filePath, fileTypeName);
			List<File> fileList = ETL_Tool_FileReader.getTargetFileList_noFT(filePath, fileTypeName);
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
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName, true);

				// 報送單位非預期, 不進行解析
				if (exc_central_no == null || "".equals(exc_central_no.trim())) {
					System.out.println("## ETL_E_CALENDAR - read_CALENDAR_File - 控制程式無提供報送單位，不進行解析！"); // TODO V3
					processErrMsg = processErrMsg + "控制程式無提供報送單位，不進行解析！\n";
					continue;
				} else if (!exc_central_no.trim().equals(pfn.getCentral_No().trim())) {
					System.out.println("##" + pfn.getFileName() + " 處理報送單位非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理報送單位非預期，不進行解析！\n";
					continue;
				}

				// 業務別非預期, 不進行解析
				// if (pfn.getFile_Type() == null || "".equals(pfn.getFile_Type().trim())
				// || !checkMaps.get("comm_file_type").containsKey(pfn.getFile_Type().trim())) {
				//
				// System.out.println("##" + pfn.getFileName() + " 處理業務別非預期，不進行解析！");
				// processErrMsg = processErrMsg + pfn.getFileName() + " 處理業務別非預期，不進行解析！\n";
				// continue;
				// }

				// 資料日期非預期, 不進行解析
				if (exc_record_date == null) {
					System.out.println("## ETL_E_CALENDAR - read_CALENDAR_File - 控制程式無提供資料日期，不進行解析！"); // TODO V3
					processErrMsg = processErrMsg + "控制程式無提供資料日期，不進行解析！\n";
					continue;
				} else if (!exc_record_date.equals(pfn.getRecord_Date())) {
					System.out.println("## " + pfn.getFileName() + " 處理資料日期非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理資料日期非預期，不進行解析！\n";
					continue;
				}

				// 設定批次編號
				pfn.setBatch_no(batch_no);

				FileInputStream fis = new FileInputStream(parseFile);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis, "BIG5"));

				// rowCount == 處理行數
				int rowCount = 1; // 從1開始
				// 成功計數
				int successCount = 0;
				// 失敗計數
				int failureCount = 0;
				// 尾錄總數
				int iTotalCount = 0;

				try { // TODO V3

					// 開始前ETL_FILE_Log寫入DB // TODO V3
					ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date /* TODO V3 */,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseStartDate, null, 0, 0, 0,
							pfn.getFileName());

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

						// // 檢查整行bytes數(1 + 7 + 8 7= 23)
						if (strQueue.getTotalByteLength() != 23) {
							fileFmtErrMsg = "首錄位元數非預期23";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別瑪檢核(1)
						String typeCode = strQueue.popBytesString(1);
						if (!"1".equals(typeCode)) { // 首錄區別碼檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄報送單位代碼與檔名不符: " + central_no;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}

						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (record_date == null || "".equals(record_date.trim())) {
							fileFmtErrMsg = "首錄檔案日期空值: "+record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!record_date.equals(pfn.getRecord_Date_String())) {
							fileFmtErrMsg = "首錄檔案日期與檔名不符: "+record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "首錄檔案日期格式錯誤 :"+record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 保留欄位檢核(7)
						String keepColumn = strQueue.popBytesString(7);

						rowCount++; // 處理行數 + 1
					}

					// 逐行讀取檔案
					if ("".equals(fileFmtErrMsg)) {// 沒有嚴重錯誤時進行
						while (br.ready()) {
							lineStr = br.readLine();

							strQueue.setTargetString(lineStr); // queue裝入新String

							ETL_Bean_CALENDAR_TEMP_Data data = new ETL_Bean_CALENDAR_TEMP_Data(pfn, null, null, null);
							data.setRow_count(rowCount);

							// 區別碼(1)
							String typeCode = strQueue.popBytesString(1);
							if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
								break;
							}

							// 整行bytes數檢核(1 + 8 + 1 +13= 23)
							if (strQueue.getTotalByteLength() != 23) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "行數bytes檢查", "非預期23"));

								failureCount++;
								rowCount++; // 處理行數 ++

								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								continue;
							}

							// 區別碼檢核 *
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "區別碼", "非預期:"+typeCode));
							}

							// 日期檢核
							String calendar_day = strQueue.popBytesString(8);
							if (!ETL_Tool_FormatCheck.isEmpty(calendar_day)) {
								if (ETL_Tool_FormatCheck.checkDate(calendar_day)) {
									data.setCalendar_day(ETL_Tool_StringX.toUtilDate(calendar_day));
								} else if (advancedCheck) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "日期", "明細資料日期格式錯誤:"+calendar_day));

								}

							}

							String is_business_day = strQueue.popBytesString(1);
							data.setIs_business_day(is_business_day);

							// 是否為營業日檢核 V3 改成必填
							if (ETL_Tool_FormatCheck.isEmpty(is_business_day)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "是否為營業日", "空值"));
							} else if (!checkMaps.get("T_5").containsKey(is_business_day.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "是否為營業日", "非預期:"+is_business_day));
							}

							// 保留欄位檢核(13)
							String keepColumn = strQueue.popBytesString(13);

							// data list 加入一個檔案
							addData(data);

							if ("Y".equals(data.getError_mark())) {
								failureCount++;
							} else {
								successCount++;
							}

							rowCount++; // 處理行數 + 1

						}

					}
					// Calendar_Data寫入DB
					insert_Calendar_Data();

					// 尾錄檢查
					if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						// 整行bytes數檢核 (1 + 7 + 8 + 7 = 23)
						if (strQueue.getTotalByteLength() != 23) {
							fileFmtErrMsg = "尾錄位元數非預期23";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別碼檢核(1) 經"逐行讀取檔案"區塊, 若無嚴重錯誤應為3, 此處無檢核

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "尾錄報送單位代碼與檔名不符:"+central_no;
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
							fileFmtErrMsg = "尾錄檔案日期與檔名不符:"+record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "尾錄檔案日期格式錯誤  :"+record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 總筆數檢核(7)
						String totalCount = strQueue.popBytesString(7);
						iTotalCount = ETL_Tool_StringX.toInt(totalCount);
						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤:"+totalCount;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符:"+totalCount+"!="+(rowCount - 2);
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}

						if ((rowCount - 2) != (successCount + failureCount)) { // TODO V3
							fileFmtErrMsg = "總筆數 <> 成功比數 + 失敗筆數";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "程式檢核", fileFmtErrMsg));
						}

						// 多餘行數檢查
						if (br.ready()) {
							fileFmtErrMsg = "出現多餘行數";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案總行數", fileFmtErrMsg));
							rowCount++;
						}

					}

					fis.close();
					Date parseEndDate = new Date(); // 開始執行時間
					System.out.println("解析檔案： " + fileName + " End " + parseEndDate);

					// Error_Log寫入DB
					errWriter.insert_Error_Log();

					// 執行結果
					String file_exe_result;
					// 執行結果說明
					String file_exe_result_description;

					if (!"".equals(fileFmtErrMsg)) {
						file_exe_result = "S";
						file_exe_result_description = "解析檔案出現嚴重錯誤";
						processErrMsg = processErrMsg + pfn.getFileName() + "解析檔案出現嚴重錯誤\n";
					} else if (failureCount == 0) {
						file_exe_result = "Y";
						file_exe_result_description = "執行結果無錯誤資料";
					} else {
						file_exe_result = "D";
						file_exe_result_description = "錯誤資料筆數: " + detail_ErrorCount;
					}

					// 處理後更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log_NO_FILE_TYPE(pfn.getBatch_no(), pfn.getCentral_No(),
							exc_record_date /* TODO V3 */, pfn.getFile_Name(), upload_no, "E", parseEndDate,
							iTotalCount, successCount, failureCount, file_exe_result, file_exe_result_description);

				} catch (Exception ex) {
					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log_NO_FILE_TYPE(pfn.getBatch_no(), pfn.getCentral_No(),
							exc_record_date, pfn.getFile_Name(), upload_no, "E", new Date(), iTotalCount, successCount,
							failureCount, "S", ex.getMessage());

					processErrMsg = processErrMsg + ex.getMessage() + "\n";

					ex.printStackTrace();
				}

				// 累加處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;

			}

			// 執行結果
			String detail_exe_result;
			// 執行結果說明
			String detail_exe_result_description;

			if (!"".equals(processErrMsg)) {
				detail_exe_result = "S";
				detail_exe_result_description = processErrMsg;
			} else if (detail_ErrorCount == 0) {
				detail_exe_result = "Y";
				detail_exe_result_description = "檔案轉入檢核無錯誤";
			} else {
				detail_exe_result = "N";
				detail_exe_result_description = "錯誤資料筆數: " + detail_ErrorCount;
			}

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", detail_exe_result, detail_exe_result_description, new Date());

		} catch (Exception ex) {
			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}
		System.out.println("#######Extrace - ETL_E_CALENDAR - End"); // TODO

	}

	// List增加一個data
	// TODO
	private void addData(ETL_Bean_CALENDAR_TEMP_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Calendar_Data();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}

	private void insert_Calendar_Data() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_CALENDAR - insert_Calendar_Data 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_CALENDAR_TEMP(?)}"); // 呼叫寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_CALENDAR_TEMP"); // DB2 type -
		insertAdapter.setCreateStructTypeName("T_CALENDAR_TEMP"); // DB2 array type -
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_CALENDAR_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_CALENDAR_Datas 發生錯誤");
		}

	}
}
