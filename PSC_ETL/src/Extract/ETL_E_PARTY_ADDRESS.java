package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_PARTY_ADDRESS_Data;
import DB.ETL_P_Data_Writer;
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

public class ETL_E_PARTY_ADDRESS {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "change_code", "PARTY_ADDRESS_CHANGE_CODE" }, // 異動代號
			{ "address_type", "PARTY_ADDRESS_ADDRESS_TYPE" }, // 地址類別
			{ "country", "COMM_NATIONALITY_CODE" }// 地址國別
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	private List<ETL_Bean_PARTY_ADDRESS_Data> dataList = new ArrayList<ETL_Bean_PARTY_ADDRESS_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_PARTY_ADDRESS 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}
	};

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
	public void read_Party_Address_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		System.out.println("#######Extrace - ETL_E_PARTY_ADDRESS - Start");

		try {
			// 批次不重複執行
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_PARTY_ADDRESS - 不重複執行\n" + inforMation);
				System.out.println("#######Extrace - ETL_E_PARTY_ADDRESS - End");

				return;
			}
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理PARTY_ADDRESS錯誤計數
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

				// TODO V5 START
				ETL_Tool_FileByteUtil fileByteUtil = new ETL_Tool_FileByteUtil(parseFile.getAbsolutePath(),
						ETL_E_PARTY_ADDRESS.class);// TODO
				// TODO V5 END

				// 檔名
				String fileName = parseFile.getName();
				// TODO V5 START
				// 讀檔檔名英文字轉大寫比較
				if (!ETL_Tool_FormatCheck.isEmpty(fileName))
					fileName = fileName.toUpperCase();
				// TODO V5 END
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
					System.out.println("## ETL_E_PARTY_ADDRESS - read_Party_Address_File - 控制程式無提供報送單位，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供報送單位，不進行解析！\n";
					continue;
				} else if (!exc_central_no.trim().equals(pfn.getCentral_No().trim())) {
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
					System.out.println("## ETL_E_PARTY_ADDRESS - read_Party_Address_File - 控制程式無提供資料日期，不進行解析！");
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
				// TODO V5 START
				// 尾錄總數
				// int iTotalCount = 0;

				// 紀錄是否第一次
				boolean isFirstTime = false;
				// TODO V5 END

				try {
					// 開始前ETL_FILE_Log寫入DB
					ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseStartDate, null, 0, 0, 0,
							pfn.getFileName());

					// 嚴重錯誤訊息變數(讀檔)
					String fileFmtErrMsg = "";

					String lineStr = ""; // 行字串暫存區

					// ETL_字串處理Queue
					ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue(exc_central_no);
					// ETL_Error Log寫入輔助工具
					ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();
					// TODO V5 START
					// 讀檔並將結果注入ETL_字串處理Queue
					// strQueue.setBytesList(ETL_Tool_FileByteUtil.getFilesBytes(parseFile.getAbsolutePath()));
					// 首、明細、尾錄, 基本組成檢查
					// boolean isFileFormatOK =
					// ETL_Tool_FileFormat.checkBytesList(strQueue.getBytesList());
					int isFileOK = fileByteUtil.isFileOK(parseFile.getAbsolutePath());
					boolean isFileFormatOK = isFileOK != 0 ? true : false;
					// TODO V5 END

					// 首錄檢查
					if (isFileFormatOK) {

						// TODO V5 START
						// 注入指定範圍筆數資料到QUEUE
						strQueue.setBytesList(fileByteUtil.getFilesBytes());
						// TODO V5 END

						// strQueue工具注入第一筆資料
						strQueue.setTargetString();

						// 檢查整行bytes數(1 + 7 + 8 + 121 = 137)
						if (strQueue.getTotalByteLength() != 137) {
							fileFmtErrMsg = "首錄位元數非預期137:" + strQueue.getTotalByteLength();
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

						/*
						 * 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
						 */
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) {
							fileFmtErrMsg = "首錄報送單位代碼與檔名不符:" + central_no;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}

						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (ETL_Tool_FormatCheck.isEmpty(record_date)) {
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

						// 保留欄檢核(121)
						String reserve_field = strQueue.popBytesString(121);

						rowCount++; // 處理行數 + 1
					}

					// TODO V5 START
					// 實際處理明細錄筆數
					int grandTotal = 0;
					// TODO V5 END

					// TODO V5 START
					// 明細錄檢查- 逐行讀取檔案
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行 TODO
						if (rowCount == 2)
							isFirstTime = true;
						// System.out.println("資料總筆數:" + isFileOK);
						// while (strQueue.setTargetString() <
						// strQueue.getByteListSize()) {
						// 以實際處理明細錄筆數為依據，只運行明細錄次數
						while (grandTotal < (isFileOK - 2)) {

							strQueue.setTargetString();

							// TODO V5 END

							// 生成一個Data
							ETL_Bean_PARTY_ADDRESS_Data data = new ETL_Bean_PARTY_ADDRESS_Data(pfn);
							data.setRow_count(rowCount);

							// 整行bytes數檢核(1 + 7 + 11 + 1 + 3 + 2 + 12 + 100
							// =137)
							if (strQueue.getTotalByteLength() != 137) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
												"行數bytes檢查", "非預期137:" + strQueue.getTotalByteLength()));

								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								failureCount++;
								rowCount++;
								continue;
							}

							// 區別碼檢核 R X(01)*
							String typeCode = strQueue.popBytesString(1);
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "區別碼", "非預期:" + typeCode));
							}

							// 本會代號檢核 R X(07)*
							String domain_id = strQueue.popBytesString(7);
							data.setDomain_id(domain_id);

							if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "空值"));
							} else if (!checkMaps.get("domain_id").containsKey(domain_id.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "非預期:" + domain_id));
							}

							// 客戶統編檢核 R X(11)*
							String party_number = strQueue.popBytesString(11);
							data.setParty_number(party_number);

							if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶統編", "空值"));
							}

							// 異動代號檢核 R X(01)*
							String change_code = strQueue.popBytesString(1);
							data.setChange_code(change_code);

							if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "異動代號", "空值"));
							} else if (!checkMaps.get("change_code").containsKey(change_code.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "異動代號", "非預期:" + change_code));
							}

							// 地址類別檢核 R X(03)*
							String address_type = strQueue.popBytesString(3);
							data.setAddress_type(address_type);

							if (ETL_Tool_FormatCheck.isEmpty(address_type)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "地址類別", "空值"));
							} else if (!checkMaps.get("address_type").containsKey(address_type.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "地址類別", "非預期:" + address_type));
							}

							// 地址國別檢核 O X(02)
							String country = strQueue.popBytesString(2);
							data.setCountry(country);

							if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(country)
									&& !checkMaps.get("country").containsKey(country.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "地址國別", "非預期:" + country));
							}

							// 郵遞區號檢核 O X(12)
							String po_box = strQueue.popBytesString(12);
							data.setPo_box(po_box);

							// 地址檢核 R X(100)*
							String address_line_1 = strQueue.popBytesString(100);
							data.setAddress_line_1(address_line_1);

							if (ETL_Tool_FormatCheck.isEmpty(address_line_1)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "地址", "空值"));
							}

							// data list 加入一個檔案
							addData(data);

							if ("Y".equals(data.getError_mark())) {
								failureCount++;
							} else {
								successCount++;
							}
							// TODO V5 START
							// 實際處理明細錄筆數累加
							grandTotal += 1;

//							System.out.println("實際處理列數:" + rowCount + " / 實際處理明細錄筆數:" + grandTotal + " / 目前處理資料第"
//									+ strQueue.getBytesListIndex() + "筆");

							rowCount++; // 處理行數 + 1

							/*
							 * 第一個條件是 初次處理，且資料總筆數比制定範圍大時 會進入條件
							 * 第二個條件是非初次處理，且個別資料來源已處理的筆數，可以被制定範圍整除時進入
							 */
							if ((isFirstTime && (isFileOK >= ETL_Profile.ETL_E_Stage)
									&& grandTotal == (ETL_Profile.ETL_E_Stage - 1))
									|| (!isFirstTime && (strQueue.getBytesListIndex() % ETL_Profile.ETL_E_Stage == 0))

							) {

//								System.out.println("=======================================");
//
//								if (isFirstTime)
//									System.out.println("第一次處理，資料來源須扣除首錄筆數");
								//記錄非初次
								isFirstTime = false;

//								System.out
//										.println("累積處理資料已達到限制處理筆數範圍:" + ETL_Profile.ETL_E_Stage + "筆，再度切割資料來源進入QUEUE");

								// 注入指定範圍筆數資料到QUEUE
								strQueue.setBytesList(fileByteUtil.getFilesBytes());
								// 初始化使用筆數
								strQueue.setBytesListIndex(0);

//								System.out.println("初始化提取處理資料，目前處理資料為:" + strQueue.getBytesListIndex());
//								System.out.println("=======================================");
							}
							// TODO V5 END
						}
					}

					// Party_Address_Data寫入DB
					insert_Party_Address_Datas();

					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行
						
						//TODO V5 START
						strQueue.setTargetString();
						//TODO V5 END
						
						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 114 = 137)
						if (strQueue.getTotalByteLength() != 137) {
							fileFmtErrMsg = "尾錄位元數非預期137:" + strQueue.getTotalByteLength();
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

						/*
						 * 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
						 */
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) {
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
						// iTotalCount = ETL_Tool_StringX.toInt(totalCount); TODO V5
						
						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤:" + totalCount;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符:" + totalCount + "!=" + (rowCount - 2);
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}

						// 保留欄檢核(111)
						String reserve_field = strQueue.popBytesString(111);

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

					if (!isFileFormatOK) {
						file_exe_result = "S";
						file_exe_result_description = "解析檔案出現嚴重錯誤-區別碼錯誤";
						processErrMsg = processErrMsg + pfn.getFileName() + "解析檔案出現嚴重錯誤-區別碼錯誤\n";

						// 寫入Error Log
						errWriter.addErrLog(
								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", "0", "區別碼", "解析檔案出現嚴重錯誤-區別碼錯誤"));

					} else if (!"".equals(fileFmtErrMsg)) {
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
//					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
//							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseEndDate, iTotalCount,
//							successCount, failureCount, file_exe_result, file_exe_result_description);
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseEndDate,
							(successCount + failureCount), // TODO V5
							successCount, failureCount, file_exe_result, file_exe_result_description);
				} catch (Exception ex) {
					// 寫入Error_Log
					ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
							"E", "0", "ETL_E_PARTY_ADDRESS程式處理", ex.getMessage(), null);

					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", new Date(), 0, 0, 0, "S",
							ex.getMessage());
					processErrMsg = processErrMsg + ex.getMessage() + "\n";

					ex.printStackTrace();
				}
				// 累加PARTY_ADDRESS處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;
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
						"0", "ETL_E_PARTY_ADDRESS程式處理", detail_exe_result_description, null);

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
					"0", "ETL_E_PARTY_ADDRESS程式處理", ex.getMessage(), null);

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}
		System.out.println("#######Extrace - ETL_E_PARTY_ADDRESS - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_PARTY_ADDRESS_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Party_Address_Datas();
		}
	}

	// 將PARTY_ADDRESS資料寫入資料庫
	private void insert_Party_Address_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_PARTY_ADDRESS - insert_Party_Address_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫PARTY_ADDRESS寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_PARTY_ADDRESS_TEMP(?)}");
		// DB2 type - PARTY_ADDRESS
		insertAdapter.setCreateStructTypeName("T_PARTY_ADDRESS");
		// DB2 array type - PARTY_ADDRESS
		insertAdapter.setCreateArrayTypesName("A_PARTY_ADDRESS");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_Party_Address_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_Party_Address_Datas 發生錯誤");
		}
		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}

	public static void main(String[] argv) throws Exception {
		// 讀取測試資料，並列出明細錄欄位
		// Charset charset = Charset.forName("Big5");
		// List<String> lines = Files.readAllLines(
		// Paths.get("D:\\PSC\\Projects\\AgriBank\\單元測試報告\\018_CF_PARTY_ADDRESS_20180116.TXT"),
		// charset);
		//
		// if (lines.size() > 2) {
		//
		// lines.remove(0);
		// lines.remove(lines.size() - 1);
		//
		// System.out.println(
		// "============================================================================================");
		// for (String line : lines) {
		// byte[] tmp = line.getBytes(charset);
		// System.out.println("第" + (lines.indexOf(line) + 1) + "行");
		// System.out.println("位元組長度: " + tmp.length);
		// System.out.println("區別碼X(01): " + new String(Arrays.copyOfRange(tmp,
		// 0, 1), "Big5"));
		// System.out.println("本會代號X(07): " + new String(Arrays.copyOfRange(tmp,
		// 1, 8), "Big5"));
		// System.out.println("客戶統編X(11): " + new String(Arrays.copyOfRange(tmp,
		// 8, 19), "Big5"));
		// System.out.println("異動代號X(01): " + new String(Arrays.copyOfRange(tmp,
		// 19, 20), "Big5"));
		// System.out.println("地址類別X(03): " + new String(Arrays.copyOfRange(tmp,
		// 20, 23), "Big5"));
		// System.out.println("地址國別X(02): " + new String(Arrays.copyOfRange(tmp,
		// 23, 25), "Big5"));
		// System.out.println("郵遞區號X(12): " + new String(Arrays.copyOfRange(tmp,
		// 25, 37), "Big5"));
		// System.out.println("地址X(100): " + new String(Arrays.copyOfRange(tmp,
		// 37, 137), "Big5"));
		// System.out.println(
		// "============================================================================================");
		// }
		// }

		// 讀取測試資料，並運行程式
		ETL_E_PARTY_ADDRESS one = new ETL_E_PARTY_ADDRESS();
		String filePath = "D:\\PSC\\Projects\\AgriBank\\UNIT_TEST";
		String fileTypeName = "PARTY_ADDRESS";
		one.read_Party_Address_File(filePath, fileTypeName, "ETL00001", "018",
				new SimpleDateFormat("yyyyMMdd").parse("20180116"), "001", "ETL_E_PARTY_ADDRESS");
	}
}
