package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_PARTY_Data;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_EData_Filter;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.ETL_Q_ColumnCheckCodes;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FileByteUtil;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_E_PARTY {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { 
			{ "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "c-2", "COMM_DOMAIN_ID" }, // 本會代號
			{ "c-4", "PARTY_CHANGE_CODE" }, // 異動代號
			{ "c-4-2", "PARTY_CHANGE_CODE_2" }, // 異動代號
			{ "c-5", "PARTY_MY_CUSTOMER_FLAG" }, // 是否為本行客戶
			{ "c-6", "COMM_DOMAIN_ID" }, // 歸屬本/分會代號
			{ "c-7", "PARTY_ENTITY_TYPE" }, // 顧客類型
			{ "c-8", "PARTY_ENTITY_SUB_TYPE" }, // 客戶子類型
			{ "c-13", "COMM_NATIONALITY_CODE" }, // 國籍
			{ "c-18", "PARTY_GENDER" }, // 性別
			{ "c-20", "COMM_OCCUPATION_CODE" }, // 職業/行業
			{ "c-21", "PARTY_MARITAL_STATUS_CODE" }, // 婚姻狀況
			{ "c-24", "PARTY_EMPLOYEE_FLAG" }, // 行內員工註記
			{ "c-26", "PARTY_MULTIPLE_NATIONALITY_FLAG" }, // 是否具多重國籍(自然人)
			{ "c-27", "COMM_NATIONALITY_CODE" }, // 第二國籍
			{ "c-29", "PARTY_REGISTERED_SERVICE_ATM" }, // 金融卡約定服務
			{ "c-30", "PARTY_REGISTERED_SERVICE_TELEPHONE" }, // 電話約定服務
			{ "c-31", "PARTY_REGISTERED_SERVICE_FAX" }, // 傳真約定服務
			{ "c-32", "PARTY_REGISTERED_SERVICE_INTERNET" }, // 網銀約定服務
			{ "c-33", "PARTY_REGISTERED_SERVICE_MOBILE" }, // 行動銀行約定服務
			{ "c-34", "PARTY_BEARER_STOCK_FLAG" } // 是否得發行無記名股票 (法人)
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;
	
	// insert errorLog fail Count
	private int oneFileErrorCount = 0;

	// Data儲存List
	private List<ETL_Bean_PARTY_Data> dataList = new ArrayList<ETL_Bean_PARTY_Data>();

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	// filePath 讀檔路徑
	// fileTypeName 讀檔業務別
	// batch_no 批次編號
	// exc_central_no 批次執行_報送單位
	// exc_record_date 批次執行_檔案日期
	// upload_no 上傳批號
	// program_no 程式代號
	public void read_Party_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {
		
		// 取得所有檢核用子map, 置入母map內
		try {
			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(exc_record_date, exc_central_no, checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_PARTY 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_PARTY - Start");

		try {
			// 批次不重複執行
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_PARTY - 不重複執行\n" + inforMation);
				System.out.println("#######Extrace - ETL_E_PARTY - End");

				return;
			}

			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理Party_Phone錯誤計數
			int detail_ErrorCount = 0;

			// 程式執行錯誤訊息
			String processErrMsg = "";

			// 取得目標檔案File
			List<File> fileList = ETL_Tool_FileReader.getTargetFileList(filePath, fileTypeName);

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

				ETL_Tool_FileByteUtil fileByteUtil = new ETL_Tool_FileByteUtil(parseFile.getAbsolutePath(),
						ETL_E_PARTY.class);

				// 檔名
				String fileName = parseFile.getName();
				// 讀檔檔名英文字轉大寫比較
				if (!ETL_Tool_FormatCheck.isEmpty(fileName)) {
					fileName = fileName.toUpperCase();
				}
				Date parseStartDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " Start " + parseStartDate);

				// 解析fileName物件
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName);
				// 設定批次編號
				pfn.setBatch_no(batch_no);
				// 設定上傳批號
				pfn.setUpload_no(upload_no);

				// 報送單位非預期, 不進行解析
				if (exc_central_no == null || "".equals(exc_central_no.trim())) {
					System.out.println("## ETL_E_PARTY - read_Party_File - 控制程式無提供報送單位，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供報送單位，不進行解析！\n";
					continue;
				} else if (!exc_central_no.trim().equals(pfn.getCentral_No().trim())) {
					System.out.println("##" + pfn.getFileName() + " 處理報送單位非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理報送單位非預期，不進行解析！\n";
					continue;
				}

				// 業務別非預期, 不進行解析
				if (pfn.getFile_Type() == null || "".equals(pfn.getFile_Type().trim())
						|| !checkMaps.get("comm_file_type").containsKey(pfn.getFile_Type().trim())) {

					System.out.println("##" + pfn.getFileName() + " 處理業務別非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理業務別非預期，不進行解析！\n";
					continue;
				}

				// 資料日期非預期, 不進行解析
				if (exc_record_date == null) {
					System.out.println("## ETL_E_PARTY - read_Party_File - 控制程式無提供資料日期，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供資料日期，不進行解析！\n";
					continue;
				} else if (!exc_record_date.equals(pfn.getRecord_Date())) {
					System.out.println("## " + pfn.getFileName() + " 處理資料日期非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理資料日期非預期，不進行解析！\n";
					continue;
				}

				// rowCount == 處理行數
				int rowCount = 1; // 從1開始
				// 成功計數
				int successCount = 0;
				// 失敗計數
				int failureCount = 0;
				// 紀錄是否第一次
				boolean isFirstTime = false;

				try {

					// 開始前ETL_FILE_Log寫入DB
					ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseStartDate, null, 0, 0, 0,
							pfn.getFileName());

					// 嚴重錯誤訊息變數
					String fileFmtErrMsg = "";

					// ETL_字串處理Queue
					ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue(exc_central_no);
					// ETL_Error Log寫入輔助工具
					ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();
					
					int isFileOK = fileByteUtil.isFileOK(pfn, upload_no, parseFile.getAbsolutePath());
					boolean isFileFormatOK = isFileOK != 0 ? true : false;
					fileFmtErrMsg = isFileFormatOK ? "":"區別碼錯誤";

					// 首錄檢查
					if (isFileFormatOK) {

						// 注入指定範圍筆數資料到QUEUE
						strQueue.setBytesList(fileByteUtil.getFilesBytes());

						// strQueue工具注入第一筆資料
						strQueue.setTargetString();

						// 檢查整行bytes數(1 + 7 + 8 + 602 = 618)
						if (strQueue.getTotalByteLength() != 618) {
							fileFmtErrMsg = "首錄位元數非預期618:" + strQueue.getTotalByteLength();
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別瑪檢核(1)
						String typeCode = strQueue.popBytesString(1);
						if (!"1".equals(typeCode)) { // 首錄區別碼檢查, 嚴重錯誤,
														// 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查,
																		// 嚴重錯誤,
																		// 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄報送單位代碼與檔名不符:" + central_no;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}

						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (record_date == null || "".equals(record_date.trim())) {
							fileFmtErrMsg = "首錄檔案日期空值";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!record_date.equals(pfn.getRecord_Date_String())) {
							fileFmtErrMsg = "首錄檔案日期與檔名不符:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "首錄檔案日期格式錯誤:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 保留欄位檢核(602)
						String keepColumn = strQueue.popBytesString(602);

						rowCount++; // 處理行數 + 1
					}

					// 實際處理明細錄筆數
					int grandTotal = 0;

					// 逐行讀取檔案
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行
						
						if (rowCount == 2) {
							isFirstTime = true;
						}
						System.out.println("資料總筆數:" + isFileOK);
						
						// 以實際處理明細錄筆數為依據，只運行明細錄次數
						while (grandTotal < (isFileOK - 2)) {

							strQueue.setTargetString();

							// 生成一個Data
							ETL_Bean_PARTY_Data data = new ETL_Bean_PARTY_Data(pfn);
							// 寫入資料行數
							data.setRow_count(rowCount);

							// 整行bytes數檢核(618)
							if (strQueue.getTotalByteLength() != 618) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
												"行數bytes檢查", "非預期618:" + strQueue.getTotalByteLength()));

								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								failureCount++;
								rowCount++;
								grandTotal++;
								continue;
							}

							// 區別碼檢核 c-1*
							String typeCode = strQueue.popBytesString(1);
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "區別碼", "非預期:" + typeCode));
							}

							// 本會代號檢核(7) c-2*
							String domain_id = strQueue.popBytesString(7);
							data.setDomain_id(domain_id);
							if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "空值"));
							} else if (!checkMaps.get("c-2").containsKey(domain_id)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "非預期:" + domain_id));
							}

							// 客戶統編檢核(11) c-3*
							String party_number = strQueue.popBytesString(11);
							data.setParty_number(party_number);
							if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶統編", "空值"));
							}

							// 異動代號檢核(1) c-4*
							String change_code = strQueue.popBytesString(1);
							data.setChange_code(change_code);
							// 檢核內容至分出本行/非本行客戶後進行

							// 是否為本行客戶 c-*5(1)
							String my_customer_flag = strQueue.popBytesString(1);
							data.setMy_customer_flag(my_customer_flag);
							if (ETL_Tool_FormatCheck.isEmpty(my_customer_flag)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "是否為本行客戶", "空值"));
								// 無法區分是否為本行客戶, 則無法判定檢核方式, 跳過後續檢核不執行 // TODO
								// temp
								// failureCount++;
								// rowCount++;
								// continue;
							} else if (!checkMaps.get("c-5").containsKey(my_customer_flag)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "是否為本行客戶", "非預期:" + my_customer_flag));
								// 無法區分是否為本行客戶, 則無法判定檢核方式, 跳過後續檢核不執行 // TODO
								// temp
								// failureCount++;
								// rowCount++;
								// continue;
							}

							// 由是否為本行/非本行客戶, 區分兩種完全不同檢核方式
							// 本行客戶
							if ("Y".equals(my_customer_flag)) {
								// System.out.println("本行客戶");

								// 異動代號檢核*
								if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "異動代號", "空值"));
								} else if (!checkMaps.get("c-4").containsKey(change_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "異動代號", "非預期:" + change_code));
								}

								// 歸屬本/分會代號 c-*6(7)
								String branch_code = strQueue.popBytesString(7);
								// System.out.println("歸屬本/分會代號 c-*6(7):" +
								// branch_code.getBytes().length);
								data.setBranch_code(branch_code);
								if (ETL_Tool_FormatCheck.isEmpty(branch_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "歸屬本/分會代號", "空值"));
								} else if (!checkMaps.get("c-6").containsKey(branch_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "歸屬本/分會代號", "非預期:" + branch_code));
								}

								// 顧客類型 c-*7(3)
								String entity_type = strQueue.popBytesString(3);
								// System.out.println("顧客類型 c-*7(3):" +
								// entity_type.getBytes().length);
								data.setEntity_type(entity_type);
								if (ETL_Tool_FormatCheck.isEmpty(entity_type)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "顧客類型", "空值"));
								} else if (!checkMaps.get("c-7").containsKey(entity_type)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "顧客類型", "非預期:" + entity_type));
								}

								// 客戶子類型 c-8(1)
								String entity_sub_type = strQueue.popBytesString(1);
								// System.out.println("客戶子類型 c-8(1):" +
								// entity_sub_type.getBytes().length);
								data.setEntity_sub_type(entity_sub_type);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(entity_sub_type)
										&& !checkMaps.get("c-8").containsKey(entity_sub_type)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "客戶子類型", "非預期:" + entity_sub_type));
								}

								// 中文名字 c-9(40)
								String party_first_name_1 = strQueue.popBytesString(40);
								// System.out.println("中文名字 c-9(40):" +
								// party_first_name_1.getBytes().length);
								data.setParty_first_name_1(party_first_name_1);

								// 中文姓氏 c-*10(80)
								String party_last_name_1 = strQueue.popBytesDiffString(80);
								// System.out.println("中文姓氏 c-*10(80):" +
								// party_last_name_1.getBytes().length);
								data.setParty_last_name_1(party_last_name_1);
								if (ETL_Tool_FormatCheck.isEmpty(party_last_name_1)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "中文姓氏", "空值"));
								}

								// 出生年月日/創立日期 c-11(8)
								String date_of_birth = strQueue.popBytesString(8);
								// System.out.println("出生年月日/創立日期 c-11(8):" +
								// date_of_birth.getBytes().length);
								data.setDate_of_birth(ETL_Tool_StringX.toUtilDate(date_of_birth));
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(date_of_birth)
										&& !ETL_Tool_FormatCheck.checkDate(date_of_birth)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "出生年月日/創立日期", "日期格式不正確:" + date_of_birth));
								}

								// 亡故日期 c-12(8)
								String deceased_date = strQueue.popBytesString(8);
								// System.out.println("亡故日期 c-12(8):" +
								// deceased_date.getBytes().length);
								data.setDeceased_date(ETL_Tool_StringX.toUtilDate(deceased_date));
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(deceased_date)
										&& !ETL_Tool_FormatCheck.checkDate(deceased_date)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "亡故日期", "日期格式不正確:" + deceased_date));
								}

								// 國籍 c-*13(2)
								String nationality_code = strQueue.popBytesString(2);
								// System.out.println("國籍 c-*13(2):" +
								// nationality_code.getBytes().length);
								data.setNationality_code(nationality_code);
								if (ETL_Tool_FormatCheck.isEmpty(nationality_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "國籍", "空值"));
								} else if (!checkMaps.get("c-13").containsKey(nationality_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "國籍", "非預期:" + nationality_code));
								} else if ("　".equals(nationality_code)) { // 國籍代碼防全形空白錯誤  2018.03.28
									data.setNationality_code("  ");
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "國籍", "全形空白錯誤"));
								}

								// 英文名字 c-14(40)
								String party_first_name_2 = strQueue.popBytesString(40);
								// System.out.println("英文名字 c-14(40):" +
								// party_first_name_2.getBytes().length);
								data.setParty_first_name_2(party_first_name_2);

								// 英文姓氏 c-15(80)
								String party_last_name_2 = strQueue.popBytesString(80);
								// System.out.println("英文姓氏 c-15(80):" +
								// party_last_name_2.getBytes().length);
								data.setParty_last_name_2(party_last_name_2);

								// 顧客開戶日期 c-16(8)
								String open_date = strQueue.popBytesString(8);
								// System.out.println("顧客開戶日期 c-16(8):" +
								// open_date.getBytes().length);
								data.setOpen_date(ETL_Tool_StringX.toUtilDate(open_date));
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(open_date)
										&& !ETL_Tool_FormatCheck.checkDate(open_date)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "顧客開戶日期", "日期格式不正確:" + open_date));
								}

								// 顧客結清日期 c-17(8)
								String close_date = strQueue.popBytesString(8);
								// System.out.println("顧客結清日期 c-17(8):" +
								// close_date.getBytes().length);
								data.setClose_date(ETL_Tool_StringX.toUtilDate(close_date));
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(close_date)
										&& !ETL_Tool_FormatCheck.checkDate(close_date)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "顧客結清日期", "日期格式不正確:" + close_date));
								}

								// 性別 c-18(1)
								String gender = strQueue.popBytesString(1);
								// System.out.println("性別 c-18(1):" +
								// gender.getBytes().length);
								data.setGender(gender);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(gender)
										&& !checkMaps.get("c-18").containsKey(gender)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "性別", "非預期:" + gender));
								}

								// 年收入(法人) c-19(10)
								String annual_income = strQueue.popBytesString(10);
								// System.out.println("年收入(法人) c-19(10):" +
								// annual_income.getBytes().length);
								data.setAnnual_income(ETL_Tool_StringX.toLong(annual_income));
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(annual_income)
										&& !ETL_Tool_FormatCheck.checkNum(annual_income)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "年收入(法人)", "非數字:" + annual_income));
								}

								// 職業/行業 c-*20(6)
								String occupation_code = strQueue.popBytesString(6);
								// System.out.println("職業/行業 c-*20(6):" +
								// occupation_code.getBytes().length);
								data.setOccupation_code(occupation_code);
								if (ETL_Tool_FormatCheck.isEmpty(occupation_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "職業/行業", "空值"));
								} else if (!checkMaps.get("c-20").containsKey(occupation_code.trim())) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "職業/行業", "非預期:" + occupation_code));
								}

								// 婚姻狀況 c-21(1)
								String marital_status_code = strQueue.popBytesString(1);
								// System.out.println("婚姻狀況 c-21(1):" +
								// marital_status_code.getBytes().length);
								data.setMarital_status_code(marital_status_code);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(marital_status_code)
										&& !checkMaps.get("c-21").containsKey(marital_status_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "婚姻狀況", "非預期:" + marital_status_code));
								}

								// 服務機構 c-22(30)
								String employer_name = strQueue.popBytesString(30);
								// System.out.println("服務機構 c-22(30):" +
								// employer_name.getBytes().length);
								data.setEmployer_name(employer_name);

								// 服務機構統編 c-23(8)
								String employer = strQueue.popBytesString(8);
								// System.out.println("服務機構統編 c-23(8):" +
								// employer.getBytes().length);
								data.setEmployer(employer);

								// 行內員工註記 c-24(1)
								String employee_flag = strQueue.popBytesString(1);
								// System.out.println("行內員工註記 c-24(1):" +
								// employee_flag.getBytes().length);
								data.setEmployee_flag(employee_flag);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(employee_flag)
										&& !checkMaps.get("c-24").containsKey(employee_flag)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "行內員工註記", "非預期:" + employee_flag));
								}

								// 出生地 c-25(18)
								String place_of_birth = strQueue.popBytesString(18);
								// System.out.println("出生地 c-25(18):" +
								// place_of_birth.getBytes().length);
								data.setPlace_of_birth(place_of_birth);

								// 是否具多重國籍(自然人) c-26(1)
								String multiple_nationality_flag = strQueue.popBytesString(1);
								// System.out
								// .println("是否具多重國籍(自然人) c-26(1):" +
								// multiple_nationality_flag.getBytes().length);
								data.setMultiple_nationality_flag(multiple_nationality_flag);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(multiple_nationality_flag)
										&& !checkMaps.get("c-26").containsKey(multiple_nationality_flag)) {
									data.setError_mark("Y");
									errWriter.addErrLog(
											new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
													"是否具多重國籍(自然人)", "非預期:" + multiple_nationality_flag));
								}

								// 第二國籍 c-27(2)
								String nationality_code_2 = strQueue.popBytesString(2);
								// System.out.println("第二國籍 c-27(2):" +
								// nationality_code_2.getBytes().length);
								data.setNationality_code_2(nationality_code_2);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(nationality_code_2)
										&& !checkMaps.get("c-27").containsKey(nationality_code_2)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "第二國籍", "非預期:" + nationality_code_2));
								} else if ("　".equals(nationality_code_2)) { // 國籍代碼防全形空白錯誤  2018.03.28
									data.setNationality_code_2("  ");
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "第二國籍", "全形空白錯誤"));
								}

								// 顧客電子郵件 c-28(80)
								String email_address = strQueue.popBytesString(80);
								// System.out.println("顧客電子郵件 c-28(80):" +
								// email_address.getBytes().length);
								data.setEmail_address(email_address);

								// 金融卡約定服務 c-29(1)
								String registered_service_atm = strQueue.popBytesString(1);
								// System.out.println("金融卡約定服務 c-29(1):" +
								// registered_service_atm.getBytes().length);
								data.setRegistered_service_atm(registered_service_atm);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_atm)
										&& !checkMaps.get("c-29").containsKey(registered_service_atm)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "金融卡約定服務", "非預期:" + registered_service_atm));
								}

								// 電話約定服務 c-30(1)
								String registered_service_telephone = strQueue.popBytesString(1);
								// System.out.println("電話約定服務 c-30(1):" +
								// registered_service_telephone.getBytes().length);
								data.setRegistered_service_telephone(registered_service_telephone);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_telephone)
										&& !checkMaps.get("c-30").containsKey(registered_service_telephone)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "電話約定服務", "非預期:" + registered_service_telephone));
								}

								// 傳真約定服務 c-31(1)
								String registered_service_fax = strQueue.popBytesString(1);
								// System.out.println("傳真約定服務 c-31(1):" +
								// registered_service_fax.getBytes().length);
								data.setRegistered_service_fax(registered_service_fax);
								// if
								// (ETL_Profile.Foreign_Currency.equals(pfn.getFile_Type()))
								// // 只有外幣才有此欄位
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_fax)
										&& !checkMaps.get("c-31").containsKey(registered_service_fax)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "傳真約定服務", "非預期:" + registered_service_fax));
								}

								// 網銀約定服務 c-32(1)
								String registered_service_internet = strQueue.popBytesString(1);
								// System.out.println("網銀約定服務 c-32(1):" +
								// registered_service_internet.getBytes().length);
								data.setRegistered_service_internet(registered_service_internet);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_internet)
										&& !checkMaps.get("c-32").containsKey(registered_service_internet)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "網銀約定服務", "非預期:" + registered_service_internet));
								}

								// 行動銀行約定服務 c-33(1)
								String registered_service_mobile = strQueue.popBytesString(1);
								// System.out.println("行動銀行約定服務 c-33(1):" +
								// registered_service_mobile.getBytes().length);
								data.setRegistered_service_mobile(registered_service_mobile);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_mobile)
										&& !checkMaps.get("c-33").containsKey(registered_service_mobile)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "行動銀行約定服務", "非預期:" + registered_service_mobile));
								}

								// 是否得發行無記名股票 (法人) c-34(1)
								String bearer_stock_flag = strQueue.popBytesString(1);
								// System.out.println("是否得發行無記名股票 (法人) c-34(1):"
								// + bearer_stock_flag.getBytes().length);
								data.setBearer_stock_flag(bearer_stock_flag);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(bearer_stock_flag)
										&& !checkMaps.get("c-34").containsKey(bearer_stock_flag)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "是否得發行無記名股票 (法人)", "非預期:" + bearer_stock_flag));
								}

								// 無記名股票 (法人)資訊說明 c-35(40)
								String bearer_stock_description = strQueue.popBytesString(40);
								// System.out.println(
								// "無記名股票 (法人)資訊說明 c-35(40):" +
								// bearer_stock_description.getBytes().length);
								data.setBearer_stock_description(bearer_stock_description);

								// 外國人士居留或交易目的 c-36(80)
								String foreign_transaction_purpose = strQueue.popBytesString(80);
								// System.out.println(
								// "外國人士居留或交易目的 c-36(80):" +
								// foreign_transaction_purpose.getBytes().length);
								data.setForeign_transaction_purpose(foreign_transaction_purpose);

								// 顧客AUM餘額 c-37(14)
								String total_asset = strQueue.popBytesString(14);
								// System.out.println("顧客AUM餘額 c-37(14):" +
								// total_asset.getBytes().length);
								data.setTotal_asset(ETL_Tool_StringX.strToBigDecimal(total_asset, 2));

								// 信託客戶AUM餘額 c-38(14)
								String trust_total_asset = strQueue.popBytesString(14);
								// System.out.println("信託客戶AUM餘額 c-38(14):" +
								// trust_total_asset.getBytes().length);
								data.setTrust_total_asset(ETL_Tool_StringX.strToBigDecimal(trust_total_asset, 2));

							}

							// 非本行客戶
							if ("N".equals(my_customer_flag)) {

								// 異動代號檢核
								if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "異動代號", "空值"));
								} else if (advancedCheck && !checkMaps.get("c-4-2").containsKey(change_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "異動代號", "非預期:" + change_code));
								}

								// 歸屬本/分會代號 c-6(7)
								String branch_code = strQueue.popBytesString(7);
								data.setBranch_code(branch_code);

								// 顧客類型 c-7(3)
								String entity_type = strQueue.popBytesString(3);
								data.setEntity_type(entity_type);

								// 客戶子類型 c-8(1)
								String entity_sub_type = strQueue.popBytesString(1);
								data.setEntity_sub_type(entity_sub_type);

								// 中文名字 c-9(40)
								String party_first_name_1 = strQueue.popBytesDiffString(40);
								data.setParty_first_name_1(party_first_name_1);

								// 中文姓氏 c-*10(80)
								String party_last_name_1 = strQueue.popBytesDiffString(80);
								data.setParty_last_name_1(party_last_name_1);
								if (ETL_Tool_FormatCheck.isEmpty(party_last_name_1)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "中文姓氏", "空值"));
								}

								// 出生年月日/創立日期 c-11(8)
								String date_of_birth = strQueue.popBytesString(8);
								data.setDate_of_birth(ETL_Tool_StringX.toUtilDate(date_of_birth));
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(date_of_birth)
										&& !ETL_Tool_FormatCheck.checkDate(date_of_birth)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "出生年月日/創立日期", "日期格式不正確:" + date_of_birth));
								}

								// 亡故日期 c-12(8)
								String deceased_date = strQueue.popBytesString(8);
								data.setDeceased_date(ETL_Tool_StringX.toUtilDate(deceased_date));

								// 國籍 c-13(2)
								String nationality_code = strQueue.popBytesString(2);
								data.setNationality_code(nationality_code);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(nationality_code)
										&& !checkMaps.get("c-13").containsKey(nationality_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "國籍", "非預期:" + nationality_code));
								} else if ("　".equals(nationality_code)) { // 國籍代碼防全形空白錯誤  2018.03.28
									data.setNationality_code("  ");
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "國籍", "全形空白錯誤"));
								}

								// 英文名字 c-14(40)
								String party_first_name_2 = strQueue.popBytesString(40);
								data.setParty_first_name_2(party_first_name_2);

								// 英文姓氏 c-15(80)
								String party_last_name_2 = strQueue.popBytesString(80);
								data.setParty_last_name_2(party_last_name_2);

								// 顧客開戶日期 c-16(8)
								String open_date = strQueue.popBytesString(8);
								data.setOpen_date(ETL_Tool_StringX.toUtilDate(open_date));

								// 顧客結清日期 c-17(8)
								String close_date = strQueue.popBytesString(8);
								data.setClose_date(ETL_Tool_StringX.toUtilDate(close_date));

								// 性別 c-18(1)
								String gender = strQueue.popBytesString(1);
								data.setGender(gender);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(gender)
										&& !checkMaps.get("c-18").containsKey(gender)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "性別", "非預期:" + gender));
								}

								// 年收入(法人) c-19(10)
								String annual_income = strQueue.popBytesString(10);
								data.setAnnual_income(ETL_Tool_StringX.toLong(annual_income));

								// 職業/行業 c-20(6)
								String occupation_code = strQueue.popBytesString(6);
								data.setOccupation_code(occupation_code);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(occupation_code)
										&& !checkMaps.get("c-20").containsKey(occupation_code.trim())) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "職業/行業", "非預期:" + occupation_code));
								}

								// 婚姻狀況 c-21(1)
								String marital_status_code = strQueue.popBytesString(1);
								data.setMarital_status_code(marital_status_code);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(marital_status_code)
										&& !checkMaps.get("c-21").containsKey(marital_status_code)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "婚姻狀況", "非預期:" + marital_status_code));
								}

								// 服務機構 c-22(30)
								String employer_name = strQueue.popBytesString(30);
								data.setEmployer_name(employer_name);

								// 服務機構統編 c-23(8)
								String employer = strQueue.popBytesString(8);
								data.setEmployer(employer);

								// 行內員工註記 c-24(1)
								String employee_flag = strQueue.popBytesString(1);
								data.setEmployee_flag(employee_flag);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(employee_flag)
										&& !checkMaps.get("c-24").containsKey(employee_flag)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "行內員工註記", "非預期:" + employee_flag));
								}

								// 出生地 c-25(18)
								String place_of_birth = strQueue.popBytesString(18);
								data.setPlace_of_birth(place_of_birth);

								// 是否具多重國籍(自然人) c-26(1)
								String multiple_nationality_flag = strQueue.popBytesString(1);
								data.setMultiple_nationality_flag(multiple_nationality_flag);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(multiple_nationality_flag)
										&& !checkMaps.get("c-26").containsKey(multiple_nationality_flag)) {
									data.setError_mark("Y");
									errWriter.addErrLog(
											new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
													"是否具多重國籍(自然人)", "非預期:" + multiple_nationality_flag));
								}

								// 第二國籍 c-27(2)
								String nationality_code_2 = strQueue.popBytesString(2);
								data.setNationality_code_2(nationality_code_2);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(nationality_code_2)
										&& !checkMaps.get("c-27").containsKey(nationality_code_2)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "第二國籍", "非預期:" + nationality_code_2));
								} else if ("　".equals(nationality_code_2)) { // 國籍代碼防全形空白錯誤  2018.03.28
									data.setNationality_code_2("  ");
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "第二國籍", "全形空白錯誤"));
								}

								// 顧客電子郵件 c-28(80)
								String email_address = strQueue.popBytesString(80);
								data.setEmail_address(email_address);

								// 金融卡約定服務 c-29(1)
								String registered_service_atm = strQueue.popBytesString(1);
								data.setRegistered_service_atm(registered_service_atm);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_atm)
										&& !checkMaps.get("c-29").containsKey(registered_service_atm)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "金融卡約定服務", "非預期:" + registered_service_atm));
								}

								// 電話約定服務 c-30(1)
								String registered_service_telephone = strQueue.popBytesString(1);
								data.setRegistered_service_telephone(registered_service_telephone);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_telephone)
										&& !checkMaps.get("c-30").containsKey(registered_service_telephone)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "電話約定服務", "非預期:" + registered_service_telephone));
								}

								// 傳真約定服務 c-31(1)
								String registered_service_fax = strQueue.popBytesString(1);
								data.setRegistered_service_fax(registered_service_fax);
								// if
								// (ETL_Profile.Foreign_Currency.equals(pfn.getFile_Type()))
								// // 只有外幣才有此欄位
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_fax)
										&& !checkMaps.get("c-31").containsKey(registered_service_fax)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "傳真約定服務", "非預期:" + registered_service_fax));
								}

								// 網銀約定服務 c-32(1)
								String registered_service_internet = strQueue.popBytesString(1);
								data.setRegistered_service_internet(registered_service_internet);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_internet)
										&& !checkMaps.get("c-32").containsKey(registered_service_internet)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "網銀約定服務", "非預期:" + registered_service_internet));
								}

								// 行動銀行約定服務 c-33(1)
								String registered_service_mobile = strQueue.popBytesString(1);
								data.setRegistered_service_mobile(registered_service_mobile);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(registered_service_mobile)
										&& !checkMaps.get("c-33").containsKey(registered_service_mobile)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "行動銀行約定服務", "非預期:" + registered_service_mobile));
								}

								// 是否得發行無記名股票 (法人) c-34(1)
								String bearer_stock_flag = strQueue.popBytesString(1);
								data.setBearer_stock_flag(bearer_stock_flag);
								if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(bearer_stock_flag)
										&& !checkMaps.get("c-34").containsKey(bearer_stock_flag)) {
									data.setError_mark("Y");
									errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "是否得發行無記名股票 (法人)", "非預期:" + bearer_stock_flag));
								}

								// 無記名股票 (法人)資訊說明 c-35(40)
								String bearer_stock_description = strQueue.popBytesString(40);
								data.setBearer_stock_description(bearer_stock_description);

								// 外國人士居留或交易目的 c-36(80)
								String foreign_transaction_purpose = strQueue.popBytesString(80);
								data.setForeign_transaction_purpose(foreign_transaction_purpose);

								// 顧客AUM餘額 c-37(14)
								String total_asset = strQueue.popBytesString(14);
								data.setTotal_asset(ETL_Tool_StringX.strToBigDecimal(total_asset, 2));

								// 信託客戶AUM餘額 c-38(14)
								String trust_total_asset = strQueue.popBytesString(14);
								data.setTrust_total_asset(ETL_Tool_StringX.strToBigDecimal(trust_total_asset, 2));

							}

							// 無法分辨本行/非本行客戶, 依然寫入欄位資料, 不進行任何檢核
							if (!"N".equals(my_customer_flag) && !"Y".equals(my_customer_flag)) {

								// 歸屬本/分會代號 c-6(7)
								String branch_code = strQueue.popBytesString(7);
								data.setBranch_code(branch_code);

								// 顧客類型 c-7(3)
								String entity_type = strQueue.popBytesString(3);
								data.setEntity_type(entity_type);

								// 客戶子類型 c-8(1)
								String entity_sub_type = strQueue.popBytesString(1);
								data.setEntity_sub_type(entity_sub_type);

								// 中文名字 c-9(40)
								String party_first_name_1 = strQueue.popBytesDiffString(40);
								data.setParty_first_name_1(party_first_name_1);

								// 中文姓氏 c-*10(80)
								String party_last_name_1 = strQueue.popBytesDiffString(80);
								data.setParty_last_name_1(party_last_name_1);

								// 出生年月日/創立日期 c-11(8)
								String date_of_birth = strQueue.popBytesString(8);
								data.setDate_of_birth(ETL_Tool_StringX.toUtilDate(date_of_birth));

								// 亡故日期 c-12(8)
								String deceased_date = strQueue.popBytesString(8);
								data.setDeceased_date(ETL_Tool_StringX.toUtilDate(deceased_date));

								// 國籍 c-13(2)
								String nationality_code = strQueue.popBytesString(2);
								data.setNationality_code(nationality_code);

								// 英文名字 c-14(40)
								String party_first_name_2 = strQueue.popBytesString(40);
								data.setParty_first_name_2(party_first_name_2);

								// 英文姓氏 c-15(80)
								String party_last_name_2 = strQueue.popBytesString(80);
								data.setParty_last_name_2(party_last_name_2);

								// 顧客開戶日期 c-16(8)
								String open_date = strQueue.popBytesString(8);
								data.setOpen_date(ETL_Tool_StringX.toUtilDate(open_date));

								// 顧客結清日期 c-17(8)
								String close_date = strQueue.popBytesString(8);
								data.setClose_date(ETL_Tool_StringX.toUtilDate(close_date));

								// 性別 c-18(1)
								String gender = strQueue.popBytesString(1);
								data.setGender(gender);

								// 年收入(法人) c-19(10)
								String annual_income = strQueue.popBytesString(10);
								data.setAnnual_income(ETL_Tool_StringX.toLong(annual_income));

								// 職業/行業 c-20(6)
								String occupation_code = strQueue.popBytesString(6);
								data.setOccupation_code(occupation_code);

								// 婚姻狀況 c-21(1)
								String marital_status_code = strQueue.popBytesString(1);
								data.setMarital_status_code(marital_status_code);

								// 服務機構 c-22(30)
								String employer_name = strQueue.popBytesString(30);
								data.setEmployer_name(employer_name);

								// 服務機構統編 c-23(8)
								String employer = strQueue.popBytesString(8);
								data.setEmployer(employer);

								// 行內員工註記 c-24(1)
								String employee_flag = strQueue.popBytesString(1);
								data.setEmployee_flag(employee_flag);

								// 出生地 c-25(18)
								String place_of_birth = strQueue.popBytesString(18);
								data.setPlace_of_birth(place_of_birth);

								// 是否具多重國籍(自然人) c-26(1)
								String multiple_nationality_flag = strQueue.popBytesString(1);
								data.setMultiple_nationality_flag(multiple_nationality_flag);

								// 第二國籍 c-27(2)
								String nationality_code_2 = strQueue.popBytesString(2);
								data.setNationality_code_2(nationality_code_2);

								// 顧客電子郵件 c-28(80)
								String email_address = strQueue.popBytesString(80);
								data.setEmail_address(email_address);

								// 金融卡約定服務 c-29(1)
								String registered_service_atm = strQueue.popBytesString(1);
								data.setRegistered_service_atm(registered_service_atm);

								// 電話約定服務 c-30(1)
								String registered_service_telephone = strQueue.popBytesString(1);
								data.setRegistered_service_telephone(registered_service_telephone);

								// 傳真約定服務 c-31(1)
								String registered_service_fax = strQueue.popBytesString(1);
								data.setRegistered_service_fax(registered_service_fax);

								// 網銀約定服務 c-32(1)
								String registered_service_internet = strQueue.popBytesString(1);
								data.setRegistered_service_internet(registered_service_internet);

								// 行動銀行約定服務 c-33(1)
								String registered_service_mobile = strQueue.popBytesString(1);
								data.setRegistered_service_mobile(registered_service_mobile);

								// 是否得發行無記名股票 (法人) c-34(1)
								String bearer_stock_flag = strQueue.popBytesString(1);
								data.setBearer_stock_flag(bearer_stock_flag);

								// 無記名股票 (法人)資訊說明 c-35(40)
								String bearer_stock_description = strQueue.popBytesString(40);
								data.setBearer_stock_description(bearer_stock_description);

								// 外國人士居留或交易目的 c-36(80)
								String foreign_transaction_purpose = strQueue.popBytesString(80);
								data.setForeign_transaction_purpose(foreign_transaction_purpose);

								// 顧客AUM餘額 c-37(14)
								String total_asset = strQueue.popBytesString(14);
								data.setTotal_asset(ETL_Tool_StringX.strToBigDecimal(total_asset, 2));

								// 信託客戶AUM餘額 c-38(14)
								String trust_total_asset = strQueue.popBytesString(14);
								data.setTrust_total_asset(ETL_Tool_StringX.strToBigDecimal(trust_total_asset, 2));
							}

							// data list 加入一個檔案
							addData(data);

							if ("Y".equals(data.getError_mark())) {
								failureCount++;
							} else {
								successCount++;
							}
							
							// 實際處理明細錄筆數累加
							grandTotal += 1;

							rowCount++; // 處理行數 + 1
							
							/*
							 * 第一個條件是 初次處理，且資料總筆數比制定範圍大時 會進入條件
							 * 第二個條件是非初次處理，且個別資料來源已處理的筆數，可以被制定範圍整除時進入
							 */
							if ((isFirstTime && (isFileOK >= ETL_Profile.ETL_E_Stage)
									&& grandTotal == (ETL_Profile.ETL_E_Stage - 1))
									|| (!isFirstTime && (strQueue.getBytesListIndex() % ETL_Profile.ETL_E_Stage == 0))) {

								// 記錄非初次
								isFirstTime = false;
								// 注入指定範圍筆數資料到QUEUE
								strQueue.setBytesList(fileByteUtil.getFilesBytes());
								// 初始化使用筆數
								strQueue.setBytesListIndex(0);
							}
						}
					}

					// Party_Data寫入DB
					insert_Party_Datas();
					
					// 修正筆數, 考慮寫入資料庫時寫入失敗的狀況
					successCount = successCount - this.oneFileErrorCount;
					failureCount = failureCount + this.oneFileErrorCount;
					// 單一檔案寫入DB error個數重計
					this.oneFileErrorCount = 0;

					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						strQueue.setTargetString();

						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 595 = 618)
						if (strQueue.getTotalByteLength() != 618) {
							fileFmtErrMsg = "尾錄位元數非預期618:" + strQueue.getTotalByteLength();
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別碼檢核(1)
						String typeCode = strQueue.popBytesString(1);
						if (!"3".equals(typeCode)) {
							fileFmtErrMsg = "尾錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "尾錄報送單位代碼與檔名不符:" + central_no;
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
							fileFmtErrMsg = "尾錄檔案日期與檔名不符:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "尾錄檔案日期格式錯誤:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 總筆數檢核(7)
						String totalCount = strQueue.popBytesString(7);
						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤:" + totalCount;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符:" + totalCount + " != " + (rowCount - 2);
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}

						// 保留欄檢核(595)
						String keepColumn = strQueue.popBytesString(595);

						// 程式統計檢核
						if ((rowCount - 2) != (successCount + failureCount)) {
							fileFmtErrMsg = "總筆數 <> 成功比數 + 失敗筆數";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "程式檢核", fileFmtErrMsg));
						}

					}

					Date parseEndDate = new Date(); // 開始執行時間
					System.out.println("解析檔案： " + fileName + " End " + parseEndDate);

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
						file_exe_result_description = "錯誤資料筆數: " + failureCount;
					}

					// Error_Log寫入DB
					errWriter.insert_Error_Log();

					// 處理後更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseEndDate,
							(successCount + failureCount),
							successCount, failureCount, file_exe_result, file_exe_result_description);

				} catch (Exception ex) {
					// 發生錯誤時, 資料List & 計數 reset
					this.dataCount = 0; 
					this.dataList.clear();
					
					// 寫入Error_Log
					ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
							"E", "0", "ETL_E_PARTY程式處理", ex.getMessage(), null);

					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", new Date(), 0, 0, 0, "S",
							ex.getMessage());
					processErrMsg = processErrMsg + ex.getMessage() + "\n";

					ex.printStackTrace();
				}

				// 累加PARTY_PHONE處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;
			}
			
			// 過濾軌跡資料
			try {
				
				ETL_P_EData_Filter.E_Datas_Filter("filter_Party_Temp_Temp",
						batch_no, exc_central_no, exc_record_date, upload_no, program_no);
				
			} catch (Exception ex) {
				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
						"E", "0", "ETL_E_PARTY程式處理", ex.getMessage(), null);
				processErrMsg = processErrMsg + ex.getMessage() + "\n";
				
				ex.printStackTrace();
			}

			// 執行結果
			String detail_exe_result;
			// 執行結果說明
			String detail_exe_result_description;

			if (fileList.size() == 0) {
				detail_exe_result = "S";
				detail_exe_result_description = "缺檔案類型：" + fileTypeName + " 檔案";

				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
						"0", "ETL_E_PARTY程式處理", detail_exe_result_description, null);

			} else if (!"".equals(processErrMsg)) {
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
			// 寫入Error_Log
			ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
					"0", "ETL_E_PARTY程式處理", ex.getMessage(), null);

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_PARTY - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_PARTY_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Party_Datas();
		}
	}

	// 將PARTY_PHONE資料寫入資料庫
	private void insert_Party_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_PARTY - insert_Party_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call ADMINISTRATOR.SP_INSERT_PARTY_TEMP(?,?)}"); // 呼叫PARTY_PHONE寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_PARTY"); // DB2 array type - PARTY
		insertAdapter.setCreateStructTypeName("T_PARTY"); // DB2 type - PARTY
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.dataList, insertAdapter);
		int errorCount = insertAdapter.getErrorCount();

		if (isSuccess) {
			System.out.println("insert_Party_Datas 寫入 " + this.dataList.size() + "(-" + errorCount + ")筆資料!");
			this.oneFileErrorCount = this.oneFileErrorCount + errorCount;
		} else {
			throw new Exception("insert_Party_Datas 發生錯誤");
		}

		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}

	public static void main(String[] argv) throws Exception {

		// 讀取測試資料，並運行程式
		ETL_E_PARTY one = new ETL_E_PARTY();
		String filePath = "C:\\Users\\10404003\\Desktop\\農金\\2018\\180309";
		String fileTypeName = "PARTY";

		long time1, time2;
		time1 = System.currentTimeMillis();

		one.read_Party_File(filePath, fileTypeName, "E8888888", "605",
				new SimpleDateFormat("yyyyMMdd").parse("20180227"), "999", "ETL_E_PARTY");

		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");

	}
}
