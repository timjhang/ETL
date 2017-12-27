package Extract;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
import Tool.ETF_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_E_TRANSACTION {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { 
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
	public void read_Transaction_File(String filePath, String fileTypeName, String upload_no) {

		System.out.println("#######Extrace - ETL_E_TRANSACTION - Start");

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

				// System.out.println(parseFile.getAbsoluteFile()); // test
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

					// 檢查整行bytes數(1 + 7 + 8 + 468 = 484)
					if (strQueue.getTotalByteLength() != 484) {
						fileFmtErrMsg = "首錄位元數非預期484";
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

					/*
					 * 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
					 */
					String central_no = strQueue.popBytesString(7);
					if (!central_no.equals(pfn.getCentral_No())) {
						fileFmtErrMsg = "首錄報送單位代碼與檔名不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"報送單位", fileFmtErrMsg));
					}

					// 檔案日期檢核(8)
					String record_date = strQueue.popBytesString(8);
					if (ETL_Tool_FormatCheck.isEmpty(record_date)) {
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

					// 保留欄檢核(468)
					String reserve_field = strQueue.popBytesString(468);

					if (!"".equals(fileFmtErrMsg)) {
						failureCount++; // 錯誤計數 + 1
					} else {
						successCount++; // 成功計數 + 1
					}
					rowCount++; // 處理行數 + 1
				}

				// 明細錄檢查- 逐行讀取檔案
				if ("".equals(fileFmtErrMsg)) // 沒有嚴重錯誤時進行
					while (br.ready()) {

						lineStr = br.readLine();
						// System.out.println(lineStr); // test
						strQueue.setTargetString(lineStr); // queue裝入新String

						// 生成一個Data
						ETL_Bean_TRANSACTION_Data data = new ETL_Bean_TRANSACTION_Data(pfn);
						data.setRow_count(rowCount);

						// 區別碼(1)
						String typeCode = strQueue.popBytesString(1);
						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
							break;
						}

						/*
						 * 整行bytes數檢核(01+ 07+ 11+ 30+ 20+ 08+ 14+ 03+ 01+ 14+
						 * 01+ 04+ 03+ 10+ 10+ 05+ 80+ 07+ 20+ 80+ 80+ 08+ 50+
						 * 14+ 01+ 02 = 484)
						 */
						if (strQueue.getTotalByteLength() != 484) {
							data.setError_mark("Y");
							fileFmtErrMsg = "明細錄位元數非預期484";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));

							// 資料bytes不正確, 為格式嚴重錯誤, 跳出迴圈不繼續執行
							break;
						}

						// 區別碼檢核 R X(01)*
						if (ETL_Tool_FormatCheck.isEmpty(typeCode)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", "空值"));
						} else if (!"2".equals(typeCode)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", "非預期"));
						}

						// 本會代號檢核 R X(07)*
						String domain_id = strQueue.popBytesString(7);
						data.setDomain_id(domain_id);

						if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "本會代號", "空值"));
						} else if (!checkMaps.get("domain_id").containsKey(domain_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "本會代號", "非預期"));
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
									String.valueOf(rowCount), "作帳日", "格式錯誤"));
						}
						// 實際交易時間 R X(14)*
						String transaction_time = strQueue.popBytesString(14);
						data.setTransaction_time(ETL_Tool_StringX.toTimestamp(transaction_time));

//						if (ETL_Tool_FormatCheck.isEmpty(transaction_time)) {
//							data.setError_mark("Y");
//							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
//									String.valueOf(rowCount), "實際交易時間", "空值"));
//						} else if (!ETL_Tool_FormatCheck.checkTimestamp(transaction_time)) {
//							data.setError_mark("Y");
//							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
//									String.valueOf(rowCount), "實際交易時間", "格式錯誤"));
//						}

						// 交易幣別 R X(03)*
						String currency_code = strQueue.popBytesString(3);
						data.setCurrency_code(currency_code);

						if (ETL_Tool_FormatCheck.isEmpty(currency_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易幣別", "空值"));
						} else if (!checkMaps.get("currency_code").containsKey(currency_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易幣別", "非預期"));
						}

						// 交易金額正負號 R X(01)*
						String amt_sign = strQueue.popBytesString(1);
						data.setAmt_sign(amt_sign);

						if (ETL_Tool_FormatCheck.isEmpty(amt_sign)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易金額正負號", "空值"));
						} else if (!checkMaps.get("amt_sign").containsKey(amt_sign)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易金額正負號", "非預期"));
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
									String.valueOf(rowCount), "交易金額", "格式錯誤"));
						}

						// 存提區分 R X(01)*
						String direction = strQueue.popBytesString(1);
						data.setDirection(direction);

						if (ETL_Tool_FormatCheck.isEmpty(direction)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "存提區分", "空值"));
						} else if (!checkMaps.get("direction").containsKey(direction)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "存提區分", "非預期"));
						}

						// 交易類別 R X(04)*
						String transaction_type = strQueue.popBytesString(4);
						data.setTransaction_type(transaction_type);

						if (ETL_Tool_FormatCheck.isEmpty(transaction_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易類別", "空值"));
						} else if (!checkMaps.get("transaction_type").containsKey(transaction_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易類別", "非預期"));
						}

						// 交易管道 R X(03)*
						String channel_type = strQueue.popBytesString(3);
						data.setChannel_type(channel_type);

						if (ETL_Tool_FormatCheck.isEmpty(channel_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易管道", "空值"));
						} else if (!checkMaps.get("channel_type").containsKey(channel_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易管道", "非預期"));
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
									String.valueOf(rowCount), "操作行", "當交易管道為C02需提供登入行"));
						}

						// 操作櫃員代號或姓名 O X(20)
						String execution_id = strQueue.popBytesString(20);
						data.setExecution_id(execution_id);

						// 匯款人姓名 O X(80)
						String ordering_customer_party_name = strQueue.popBytesString(80);
						data.setOrdering_customer_party_name(ordering_customer_party_name);

						// 受款人姓名 O X(80)
						String beneficiary_customer_party_name = strQueue.popBytesString(80);
						data.setBeneficiary_customer_party_name(beneficiary_customer_party_name);

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
								&& !checkMaps.get("ec_flag").containsKey(ec_flag)) {
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

						// data list 加入一個檔案
						addData(data);

						if ("Y".equals(data.getError_mark())) {
							failureCount++;
						} else {
							successCount++;
						}
						rowCount++; // 處理行數 + 1
					}

				// Transaction_Data寫入DB
				insert_Transaction_Datas();

				// 尾錄檢查
				if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

					// 整行bytes數檢核 (1 + 7 + 8 + 7 + 461 = 484)
					if (strQueue.getTotalByteLength() != 484) {
						fileFmtErrMsg = "尾錄位元數非預期484";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"行數bytes檢查", fileFmtErrMsg));
					}

					// 區別碼檢核(1) 經"逐行讀取檔案"區塊, 若無嚴重錯誤應為3, 此處無檢核

					/*
					 * 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
					 */
					String central_no = strQueue.popBytesString(7);
					if (!central_no.equals(pfn.getCentral_No())) {
						fileFmtErrMsg = "尾錄報送單位代碼與檔名不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"報送單位", fileFmtErrMsg));
					}

					// 檔案日期檢核(8)
					String record_date = strQueue.popBytesString(8);
					if (record_date == null || "".equals(record_date.trim())) {
						fileFmtErrMsg = "尾錄檔案日期空值";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!record_date.equals(pfn.getRecord_Date_String())) {
						fileFmtErrMsg = "尾錄檔案日期與檔名不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
						fileFmtErrMsg = "尾錄檔案日期格式錯誤";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					}

					// 總筆數檢核(7)
					String totalCount = strQueue.popBytesString(7);
					if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
						fileFmtErrMsg = "尾錄總筆數格式錯誤";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"總筆數", fileFmtErrMsg));
					} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
						fileFmtErrMsg = "尾錄總筆數與統計不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
								String.valueOf(rowCount - 2), "總筆數", fileFmtErrMsg));
					}

					// 保留欄檢核(461)
					String reserve_field = strQueue.popBytesString(461);

					if (!"".equals(fileFmtErrMsg)) {
						failureCount++;
					} else {
						successCount++;
					}

				}

				// 程式統計檢核
				if (rowCount != (successCount + failureCount)) {
					fileFmtErrMsg = "總筆數 <> 成功筆數 + 失敗筆數";
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

		} catch (Exception ex) {

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
			this.dataCount = 0;
			this.dataList.clear();
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

	public static void main(String[] argv) throws IOException {

		//讀取測試資料，並列出明細錄欄位
	    Charset charset = Charset.forName("Big5");
		List<String> lines = Files.readAllLines(Paths.get("D:\\PSC\\Projects\\全國農業金庫洗錢防制系統案\\UNIT_TEST\\TRANSACTION.txt"), charset);
		System.out.println("============================================================================================");
		for (String line : lines) {
			byte[] tmp = line.getBytes(charset);
			System.out.println("第"+ ( lines.indexOf(line) + 1 ) + "行");
			System.out.println("位元組長度: "+ tmp.length);
			System.out.println("區別碼X(01): " + new String(Arrays.copyOfRange(tmp, 0, 1), "Big5"));
			System.out.println("本會代號X(07): " + new String(Arrays.copyOfRange(tmp, 1, 8), "Big5"));
			System.out.println("客戶統編X(11): " + new String(Arrays.copyOfRange(tmp, 8, 19), "Big5"));
			System.out.println("帳號X(30): " + new String(Arrays.copyOfRange(tmp, 19, 49), "Big5"));
			System.out.println("主機交易序號X(20): " + new String(Arrays.copyOfRange(tmp, 49, 69), "Big5"));
			System.out.println("作帳日X(08): " + new String(Arrays.copyOfRange(tmp, 69, 77), "Big5"));
			System.out.println("實際交易時間X(14): " + new String(Arrays.copyOfRange(tmp, 77, 91), "Big5"));
			System.out.println("交易幣別X(03): " + new String(Arrays.copyOfRange(tmp, 91, 94), "Big5"));
			System.out.println("交易金額正負號X(01): " + new String(Arrays.copyOfRange(tmp, 94, 95), "Big5"));
			System.out.println("交易金額9(12)V99 : " + new String(Arrays.copyOfRange(tmp, 95, 109), "Big5"));
			System.out.println("存提區分X(01): " + new String(Arrays.copyOfRange(tmp, 109, 110), "Big5"));
			System.out.println("交易類別X(04): " + new String(Arrays.copyOfRange(tmp, 110, 114), "Big5"));
			System.out.println("交易管道X(03): " + new String(Arrays.copyOfRange(tmp, 114, 117), "Big5"));
			System.out.println("交易代號X(10): " + new String(Arrays.copyOfRange(tmp, 117, 127), "Big5"));
			System.out.println("原交易代號X(10): " + new String(Arrays.copyOfRange(tmp, 127, 137), "Big5"));
			System.out.println("交易摘要X(05): " + new String(Arrays.copyOfRange(tmp, 137, 142), "Big5"));
			System.out.println("備註X(80): " + new String(Arrays.copyOfRange(tmp, 142, 222), "Big5"));
			System.out.println("操作行X(07): " + new String(Arrays.copyOfRange(tmp, 222, 229), "Big5"));
			System.out.println("操作櫃員代號或姓名X(20): " + new String(Arrays.copyOfRange(tmp, 229, 249), "Big5"));
			System.out.println("匯款人姓名X(80): " + new String(Arrays.copyOfRange(tmp, 249, 329), "Big5"));
			System.out.println("受款人姓名X(80): " + new String(Arrays.copyOfRange(tmp, 329, 409), "Big5"));
			System.out.println("受款人銀行X(08): " + new String(Arrays.copyOfRange(tmp, 409, 417), "Big5"));
			System.out.println("受款人帳號 X(50): " + new String(Arrays.copyOfRange(tmp, 417, 467), "Big5"));
			System.out.println("還款本金9(12)V99: " + new String(Arrays.copyOfRange(tmp, 467, 481), "Big5"));
			System.out.println("更正記號X(01): " + new String(Arrays.copyOfRange(tmp, 481, 482), "Big5"));
			System.out.println("申報國別X(02): " + new String(Arrays.copyOfRange(tmp, 482, 484), "Big5"));
			System.out.println("============================================================================================");
		}

		//讀取測試資料，並運行程式
		ETL_E_TRANSACTION one = new ETL_E_TRANSACTION();
		String filePath = "D:\\PSC\\Projects\\全國農業金庫洗錢防制系統案\\UNIT_TEST";
		String fileTypeName = "TRANSACTION";
		one.read_Transaction_File(filePath, fileTypeName, "001");
		System.out.println(ETL_Tool_FormatCheck.checkTimestamp("20171206140406"));
	}

}
