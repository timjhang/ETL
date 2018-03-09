package Extract;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_TRANSACTION_Data;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.ETL_Q_ColumnCheckCodes;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FileByteUtil;
import Tool.ETL_Tool_FileFormat;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_E_TRANSACTION {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "currency_code", "COMM_CURRENCY_CODE" }, // 交易幣別
			{ "amt_sign", "TRANSACTION_AMT_SIGN" }, // 交易金額正負號
			{ "direction", "TRANSACTION_DIRECTION" }, // 存提區分
			{ "transaction_type", "COMM_TRANSACTION_TYPE" }, // 交易類別
			{ "channel_type", "TRANSACTION_CHANNEL_TYPE" }, // 交易管道
			{ "ec_flag", "TRANSACTION_EC_FLAG" } // 更正記號
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	private List<ETL_Bean_TRANSACTION_Data> dataList = new ArrayList<ETL_Bean_TRANSACTION_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_TRANSACTION 抓取checkMaps資料有誤!");
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
	public void read_Transaction_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		System.out.println("#######Extrace - ETL_E_TRANSACTION - Start");

		try {
			// 批次不重複執行
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_TRANSACTION - 不重複執行\n" + inforMation);
				System.out.println("#######Extrace - ETL_E_TRANSACTION - End");

				return;
			}
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理TRANSACTION錯誤計數
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
						ETL_E_TRANSACTION.class);//TODO
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
					System.out.println("## ETL_E_TRANSACTION - read_Transaction_File - 控制程式無提供報送單位，不進行解析！");
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
					System.out.println("## ETL_E_TRANSACTION - read_Transaction_File - 控制程式無提供資料日期，不進行解析！");
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
					//TODO V5 START
					// 讀檔並將結果注入ETL_字串處理Queue
					// strQueue.setBytesList(ETL_Tool_FileByteUtil.getFilesBytes(parseFile.getAbsolutePath()));
					// 首、明細、尾錄, 基本組成檢查
					//boolean isFileFormatOK = ETL_Tool_FileFormat.checkBytesList(strQueue.getBytesList());
					// TODO V6 START
					//int isFileOK = fileByteUtil.isFileOK(parseFile.getAbsolutePath());
					int isFileOK = fileByteUtil.isFileOK(pfn, upload_no, parseFile.getAbsolutePath());
					// TODO V6 END
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

						// 檢查整行bytes數(1 + 7 + 8 + 468 = 484)
						if (strQueue.getTotalByteLength() != 484) {
							fileFmtErrMsg = "首錄位元數非預期484:" + strQueue.getTotalByteLength();
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

						// 保留欄檢核(468)
						String reserve_field = strQueue.popBytesString(468);

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
						//System.out.println("資料總筆數:" + isFileOK);
						// while (strQueue.setTargetString() < strQueue.getByteListSize()) {
						//以實際處理明細錄筆數為依據，只運行明細錄次數
						while (grandTotal < (isFileOK - 2)) {

							strQueue.setTargetString();

							// TODO V5 END

							// 生成一個Data
							ETL_Bean_TRANSACTION_Data data = new ETL_Bean_TRANSACTION_Data(pfn);
							data.setRow_count(rowCount);

							/*
							 * 整行bytes數檢核(01+ 07+ 11+ 30+ 20+ 08+ 14+ 03+ 01+
							 * 14+ 01+ 04+ 03+ 10+ 10+ 05+ 80+ 07+ 20+ 80+ 80+
							 * 08+ 50+ 14+ 01+ 02 = 484)
							 */
							if (strQueue.getTotalByteLength() != 484) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
												"行數bytes檢查", "非預期484:" + strQueue.getTotalByteLength()));

								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								failureCount++;
								rowCount++;
								grandTotal++; //TODO V6
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

							// 帳號 R X(30)*
							String account_id = strQueue.popBytesString(30);
							data.setAccount_id(account_id);

							if (ETL_Tool_FormatCheck.isEmpty(account_id)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "帳號", "空值"));
							}

							// 主機交易序號 R X(20)*
							String transaction_id = strQueue.popBytesString(20);
							data.setTransaction_id(transaction_id);

							if (ETL_Tool_FormatCheck.isEmpty(transaction_id)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "主機交易序號", "空值"));
							}

							// 作帳日 R X(08)*
							String transaction_date = strQueue.popBytesString(8);
							data.setTransaction_date(ETL_Tool_StringX.toUtilDate(transaction_date));

							if (ETL_Tool_FormatCheck.isEmpty(transaction_date)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "作帳日", "空值"));
							} else if (!ETL_Tool_FormatCheck.checkDate(transaction_date)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "作帳日", "格式錯誤:" + transaction_date));
							}
							// 實際交易時間 R X(14)*
							String transaction_time = strQueue.popBytesString(14);
							data.setTransaction_time(ETL_Tool_StringX.toTimestamp(transaction_time));

							if (ETL_Tool_FormatCheck.isEmpty(transaction_time)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "實際交易時間", "空值"));
							} else if (!ETL_Tool_FormatCheck.checkTimestamp(transaction_time)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "實際交易時間", "格式錯誤:" + transaction_time));
							}

							// 交易幣別 R X(03)*
							String currency_code = strQueue.popBytesString(3);
							data.setCurrency_code(currency_code);

							if (ETL_Tool_FormatCheck.isEmpty(currency_code)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易幣別", "空值"));
							} else if (!checkMaps.get("currency_code").containsKey(currency_code.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易幣別", "非預期:" + currency_code));
							}

							// 交易金額正負號 R X(01)*
							String amt_sign = strQueue.popBytesString(1);
							data.setAmt_sign(amt_sign);

							if (ETL_Tool_FormatCheck.isEmpty(amt_sign)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易金額正負號", "空值"));
							} else if (!checkMaps.get("amt_sign").containsKey(amt_sign.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易金額正負號", "非預期:" + amt_sign));
							}

							// 交易金額 R 9(12)V99*
							String amount = strQueue.popBytesString(14);
							data.setAmount(ETL_Tool_StringX.strToBigDecimal(amount, 2));

							if (ETL_Tool_FormatCheck.isEmpty(amount)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易金額", "空值"));
							} else if (!ETL_Tool_FormatCheck.checkNum(amount)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易金額", "格式錯誤:" + amount));
							}

							// 存提區分 R X(01)*
							String direction = strQueue.popBytesString(1);
							data.setDirection(direction);

							if (ETL_Tool_FormatCheck.isEmpty(direction)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "存提區分", "空值"));
							} else if (!checkMaps.get("direction").containsKey(direction.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "存提區分", "非預期:" + direction));
							}

							// 交易類別 R X(04)*
							String transaction_type = strQueue.popBytesString(4);
							data.setTransaction_type(transaction_type);

							if (ETL_Tool_FormatCheck.isEmpty(transaction_type)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易類別", "空值"));
							} else if (!checkMaps.get("transaction_type").containsKey(transaction_type.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易類別", "非預期:" + transaction_type));
							}

							// 交易管道 R X(03)*
							String channel_type = strQueue.popBytesString(3);
							data.setChannel_type(channel_type);

							if (ETL_Tool_FormatCheck.isEmpty(channel_type)) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易管道", "空值"));
							} else if (!checkMaps.get("channel_type").containsKey(channel_type.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易管道", "非預期:" + channel_type));
							}

							// 交易代號 R X(10)
							String transaction_purpose = strQueue.popBytesString(10);
							data.setTransaction_purpose(transaction_purpose);

							// 原交易代號 O X(10)
							String transaction_reference_number = strQueue.popBytesString(10);
							data.setTransaction_reference_number(transaction_reference_number);

							// 交易摘要 O X(5)
							String transaction_summary = strQueue.popBytesString(5);
							data.setTransaction_summary(transaction_summary);

							// 備註 O X(80)
							String transaction_description = strQueue.popBytesString(80);
							data.setTransaction_description(transaction_description);

							// 操作行 O X(7)
							String execution_branch_code = strQueue.popBytesString(7);
							data.setExecution_branch_code(execution_branch_code);

							if (!specialRequired(channel_type, execution_branch_code, "C02")) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "操作行", "當交易管道為C02才提供登入行"));
							}

							// 操作櫃員代號或姓名 O X(20)
							String executer_id = strQueue.popBytesDiffString(20);
							data.setExecuter_id(executer_id);
							// System.out.println("操作櫃員代號或姓名:" + executer_id);

							// 匯款人姓名 O X(80)
							String ordering_customer_party_name = strQueue.popBytesDiffString(80);
							data.setOrdering_customer_party_name(ordering_customer_party_name);
							// System.out.println("匯款人姓名:" +
							// ordering_customer_party_name);

							// 受款人姓名 O X(80)
							String beneficiary_customer_party_name = strQueue.popBytesDiffString(80);
							data.setBeneficiary_customer_party_name(beneficiary_customer_party_name);
							// System.out.println("受款人姓名:" +
							// beneficiary_customer_party_name);
							if (!specialRequired(transaction_type, beneficiary_customer_party_name, "DWR")) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "受款人姓名", "當交易類型為國內跨行匯款(DWR) 時需有值"));
							}

							// 受款人銀行 O X(08)
							String beneficiary_customer_bank_bic = strQueue.popBytesString(8);
							data.setBeneficiary_customer_bank_bic(beneficiary_customer_bank_bic);

							if (!specialRequired(transaction_type, beneficiary_customer_bank_bic, "DWR")) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "受款人銀行", "當交易類型為國內跨行匯款(DWR) 時需有值"));
							}

							// 受款人帳號 O X(50)
							String beneficiary_customer_account_number = strQueue.popBytesString(50);
							data.setBeneficiary_customer_account_number(beneficiary_customer_account_number);

							if (!specialRequired(transaction_type, beneficiary_customer_account_number, "DWR")) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "受款人帳號", "當交易類型為國內跨行匯款(DWR) 時需有值"));
							}

							// 還款本金 O 9(12)V99
							String repayment_principal = strQueue.popBytesString(14);
							data.setRepayment_principal(ETL_Tool_StringX.strToBigDecimal(repayment_principal, 2));

							if (!specialRequired(transaction_type, repayment_principal, "LNP")) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "還款本金", "當交易類型為還款(LNP) 時需有值"));
							}

							// 更正記號 O X(01)
							String ec_flag = strQueue.popBytesString(1);
							data.setEc_flag(ec_flag);

							if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(ec_flag)
									&& !checkMaps.get("ec_flag").containsKey(ec_flag.trim())) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "更正記號", "非預期"));
							}

							// 申報國別 R X(02)
							String ordering_customer_country = strQueue.popBytesString(2);
							data.setOrdering_customer_country(ordering_customer_country);

							if ((!specialRequired(transaction_type, ordering_customer_country, "FCSH"))
									&& (!specialRequired(transaction_type, ordering_customer_country, "FCX"))) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "申報國別", "當交易類型為 外幣現鈔時，需提供此欄位資料"));
							}

							// TODO Transaction新規格，因目前客戶提供資料還有問題，暫時不更版 START
							//代理人ID O X(11)
//							String surrogate_id = strQueue.popBytesString(11);
//							data.setSurrogate_id(surrogate_id);

							//特金信託申購/贖回單位數 O 9(12)V99
//							String fund_number_unit = strQueue.popBytesString(14);
//							data.setRepayment_principal(ETL_Tool_StringX.strToBigDecimal(fund_number_unit, 2));
							// TODO Transaction新規格，因目前客戶提供資料還有問題，暫時不更版 END

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
					
					// Transaction_Data寫入DB
					insert_Transaction_Datas();

					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) {// 沒有嚴重錯誤時進行

						//TODO V5 START
						strQueue.setTargetString();
						//TODO V5 END
						
						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 461 = 484)
						if (strQueue.getTotalByteLength() != 484) {
							fileFmtErrMsg = "尾錄位元數非預期484:" + strQueue.getTotalByteLength();
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

						// 保留欄檢核(461)
						String reserve_field = strQueue.popBytesString(461);

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

					//TODO V6 START
//					if (!isFileFormatOK) {
//						file_exe_result = "S";
//						file_exe_result_description = "解析檔案出現嚴重錯誤-區別碼錯誤";
//						processErrMsg = processErrMsg + pfn.getFileName() + "解析檔案出現嚴重錯誤-區別碼錯誤\n";
//
//						// 寫入Error Log
//						errWriter.addErrLog(
//								new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", "0", "區別碼", "解析檔案出現嚴重錯誤-區別碼錯誤"));
//
//					} else
					//TODO V6 END
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
							"E", "0", "ETL_E_TRANSACTION程式處理", ex.getMessage(), null);

					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", new Date(), 0, 0, 0, "S",
							ex.getMessage());
					processErrMsg = processErrMsg + ex.getMessage() + "\n";

					ex.printStackTrace();
				}
				// 累加TRANSACTION處理錯誤筆數
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
						"0", "ETL_E_TRANSACTION程式處理", detail_exe_result_description, null);

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
					"0", "ETL_E_TRANSACTION程式處理", ex.getMessage(), null);

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}
		System.out.println("#######Extrace - ETL_E_TRANSACTION - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_TRANSACTION_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Transaction_Datas();
		}
	}

	// 將TRANSACTION資料寫入資料庫
	private void insert_Transaction_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_TRANSACTION - insert_Transaction_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫TRANSACTION寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_TRANSACTION_TEMP(?)}");
		// DB2 type - TRANSACTION
		insertAdapter.setCreateStructTypeName("T_TRANSACTION");
		// DB2 array type - TRANSACTION
		insertAdapter.setCreateArrayTypesName("A_TRANSACTION");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_Transaction_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_Transaction_Datas 發生錯誤");
		}
		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}

	/**
	 * 檢測在特定欄位等於特定值時，會觸發另外一個欄位為必填時的規則
	 * 
	 * @param source
	 *            特定欄位
	 * @param now
	 *            被觸發的欄位
	 * @param compareVal
	 *            特定值
	 * @return true 符合規則 / false 不符合規則
	 */
	private static boolean specialRequired(String source, String now, String compareVal) {
		return (!ETL_Tool_FormatCheck.isEmpty(source) && compareVal.equals(source.trim())
				&& ETL_Tool_FormatCheck.isEmpty(now)) ? false : true;
	}

	public static void main(String[] argv) throws Exception {

		// 讀取測試資料，並列出明細錄欄位
		// Charset charset = Charset.forName("Big5");
		// List<String> lines = Files.readAllLines(
		// Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\600_R_TRANSACTION_20171206.TXT"),
		// charset);
		// // 難字轉換工具
		// ETL_Tool_Big5_To_UTF8 wordsXTool = new
		// ETL_Tool_Big5_To_UTF8(ETL_Profile.DifficultWords_Lists_Path);
		// // 難字轉換map
		// Map<String, Map<String, String>> difficultWordMaps=
		// wordsXTool.getDifficultWordMaps("600");

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
		// System.out.println("帳號X(30): " + new String(Arrays.copyOfRange(tmp,
		// 19, 49), "Big5"));
		// System.out.println("主機交易序號X(20): " + new
		// String(Arrays.copyOfRange(tmp, 49, 69), "Big5"));
		// System.out.println("作帳日X(08): " + new String(Arrays.copyOfRange(tmp,
		// 69, 77), "Big5"));
		// System.out.println("實際交易時間X(14): " + new
		// String(Arrays.copyOfRange(tmp, 77, 91), "Big5"));
		// System.out.println("交易幣別X(03): " + new String(Arrays.copyOfRange(tmp,
		// 91, 94), "Big5"));
		// System.out.println("交易金額正負號X(01): " + new
		// String(Arrays.copyOfRange(tmp, 94, 95), "Big5"));
		// System.out.println("交易金額9(12)V99 : " + new
		// String(Arrays.copyOfRange(tmp, 95, 109), "Big5"));
		// System.out.println("存提區分X(01): " + new String(Arrays.copyOfRange(tmp,
		// 109, 110), "Big5"));
		// System.out.println("交易類別X(04): " + new String(Arrays.copyOfRange(tmp,
		// 110, 114), "Big5"));
		// System.out.println("交易管道X(03): " + new String(Arrays.copyOfRange(tmp,
		// 114, 117), "Big5"));
		// System.out.println("交易代號X(10): " + new String(Arrays.copyOfRange(tmp,
		// 117, 127), "Big5"));
		// System.out.println("原交易代號X(10): " + new
		// String(Arrays.copyOfRange(tmp, 127, 137), "Big5"));
		// System.out.println("交易摘要X(05): " + new String(Arrays.copyOfRange(tmp,
		// 137, 142), "Big5"));
		// System.out.println("備註X(80): " + new String(Arrays.copyOfRange(tmp,
		// 142, 222), "Big5"));
		// System.out.println("操作行X(07): " + new String(Arrays.copyOfRange(tmp,
		// 222, 229), "Big5"));

		// StringBuffer stringBuffer = new StringBuffer();
		// for(byte b:Arrays.copyOfRange(tmp, 249, 329)){
		// stringBuffer.append("[").append(b).append("]");
		// }
		// System.out.println(stringBuffer.toString());

		// System.out.println("操作櫃員代號或姓名X(20): "
		// +wordsXTool.format(Arrays.copyOfRange(tmp, 229, 249),
		// difficultWordMaps));
		// System.out.println("匯款人姓名X(80): "
		// +wordsXTool.format(Arrays.copyOfRange(tmp, 249, 329),
		// difficultWordMaps));
		// System.out.println("受款人姓名X(80): "
		// +wordsXTool.format(Arrays.copyOfRange(tmp, 329, 409),
		// difficultWordMaps));

		// System.out.println("操作櫃員代號或姓名X(20): " + new
		// String(Arrays.copyOfRange(tmp, 229, 249), "Big5"));
		// System.out.println("匯款人姓名X(80): " + new
		// String(Arrays.copyOfRange(tmp, 249, 329), "Big5"));
		// System.out.println("受款人姓名X(80): " + new
		// String(Arrays.copyOfRange(tmp, 329, 409), "Big5"));

		// System.out.println("受款人銀行X(08): " + new
		// String(Arrays.copyOfRange(tmp, 409, 417), "Big5"));
		// System.out.println("受款人帳號 X(50): " + new
		// String(Arrays.copyOfRange(tmp, 417, 467), "Big5"));
		// System.out.println("還款本金9(12)V99: " + new
		// String(Arrays.copyOfRange(tmp, 467, 481), "Big5"));
		// System.out.println("更正記號X(01): " + new String(Arrays.copyOfRange(tmp,
		// 481, 482), "Big5"));
		// System.out.println("申報國別X(02): " + new String(Arrays.copyOfRange(tmp,
		// 482, 484), "Big5"));
		// System.out.println(
		// "============================================================================================");
		// }
		// }

		// 讀取測試資料，並運行程式
		ETL_E_TRANSACTION one = new ETL_E_TRANSACTION();
		String filePath = "D:\\PSC\\Projects\\AgriBank\\UNIT_TEST";
		String fileTypeName = "TRANSACTION";

		long time1, time2;
		time1 = System.currentTimeMillis();

//		byte[]  file_018= Files.readAllBytes(Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\018_L_TRANSACTION_20180131_______ - 複製.TXT"));
//		byte[] file_600 = Files.readAllBytes(Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\600_R_TRANSACTION_20180221.TXT"));
//		byte[] file_928_old = Files.readAllBytes(Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\928_K_TRANSACTION_20180105.TXT"));
//		System.out.println("file_600: "+file_600.length);
//		System.out.println("file_018: "+file_018.length);
//		System.out.println("file_928_old: "+file_928_old.length);
		one.read_Transaction_File(filePath, fileTypeName, "E9999999", "928",
				new SimpleDateFormat("yyyyMMdd").parse("20180105"), "001", "ETL_E_TRANSACTION");

		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");

	}

}
