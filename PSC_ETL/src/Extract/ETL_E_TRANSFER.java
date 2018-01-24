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
import Bean.ETL_Bean_TRANSFER_TEMP_Data;
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

public class ETL_E_TRANSFER {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "T_3", "COMM_FILE_TYPE" }, // 檔名業務別
			{ "T_4", "COMM_DOMAIN_ID" }, // 金融機構代號
			{ "T_9", "TRANSFER_DIRECTION" }, // 顧客境外交易資料_匯入或匯出
			{ "T_10", "COMM_CURRENCY_CODE" },// 幣別
			{"BIC" , "COMM_NATIONALITY_CODE"}//國籍代碼檢驗
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	private List<ETL_Bean_TRANSFER_TEMP_Data> dataList = new ArrayList<ETL_Bean_TRANSFER_TEMP_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_TRANSFER 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}
	};

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_Transfer_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) {
		System.out.println("#######Extrace - ETL_E_TRANSFER - Start");

		try {
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理錯誤計數
			int detail_ErrorCount = 0;

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

				// 業務別非預期, 不進行解析
				if (pfn.getFile_Type() == null) {
					System.out.println("##" + pfn.getFileName() + " 處理業務別非預期，不進行解析！");
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

					// 檢查整行bytes數(1 + 7 + 8 + 707 = 723)
					if (strQueue.getTotalByteLength() != 723) {
						fileFmtErrMsg = "首錄位元數非預期723";
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

					// 保留欄位檢核(707)
					String keepColumn = strQueue.popBytesString(707);

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
						ETL_Bean_TRANSFER_TEMP_Data data = new ETL_Bean_TRANSFER_TEMP_Data(pfn);
						data.setRow_count(rowCount);

						// 區別碼(1)
						String typeCode = strQueue.popBytesString(1);
						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
							break;
						}

						// 整行bytes數檢核(1 + 7 + 11 + 50 + 8 + 14 + 1 + 3 + 14 + 50 + 50 + 80 + 100 + 20 +
						// 50 + 80 + 50 + 20 + 4 + 10 + 10 + 20 + 20 + 50 = 723)
						if (strQueue.getTotalByteLength() != 723) {
							data.setError_mark("Y");
							fileFmtErrMsg = "非預期723";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));

							// 資料bytes不正確, 為格式嚴重錯誤, 跳出迴圈不繼續執行
							break;
						}

						// 區別碼 X(01) *
						if (!"2".equals(typeCode)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", "非預期"));
						}
						// 本會代號 X(07) T_4 *
						String domain_id = strQueue.popBytesString(7);
						data.setDomain_id(domain_id);
						if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "本會代號", "空值"));
						} else if (!checkMaps.get("T_4").containsKey(domain_id.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "本會代號", "非預期"));
						}

						// 客戶統編 X(11) *
						String party_number = strQueue.popBytesString(11);
						data.setParty_number(party_number);
						if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "客戶統編", "空值"));
						}

						// 匯款編號(交易編號) X(50) *
						String transfer_id = strQueue.popBytesString(50);
						data.setTransfer_id(transfer_id);
						if (ETL_Tool_FormatCheck.isEmpty(transfer_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯款編號", "空值"));
						}

						// 匯款日期 X(08) *
						String transfer_date = strQueue.popBytesString(8);
						if (ETL_Tool_FormatCheck.isEmpty(transfer_date)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯率日期", "空值"));
						} else if (ETL_Tool_FormatCheck.checkDate(transfer_date)) {
							data.setTransfer_date(ETL_Tool_StringX.toUtilDate(transfer_date));
						} else {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯率日期", "日期格式錯誤"));
						}

						// 實際匯款時間 X(14) *
						String transfer_time = strQueue.popBytesString(14);
						if (ETL_Tool_FormatCheck.isEmpty(transfer_time)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "實際匯款時間", "空值"));

						} else if (ETL_Tool_FormatCheck.checkDate(transfer_time, "yyyyMMddHHmmss")) {
							data.setTransfer_time(ETL_Tool_StringX.toTimestamp(transfer_time));
						} else {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "實際匯款時間", "時間格式錯誤"));
						}

						// 匯入或匯出 X(01) T_9 *
						String direction = strQueue.popBytesString(1);
						data.setDirection(direction);
						if (ETL_Tool_FormatCheck.isEmpty(direction)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯入或匯出", "空值"));
						} else if (!checkMaps.get("T_9").containsKey(direction.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯入或匯出", "非預期"));
						}

						// 交易幣別 X(03) T_10 *
						String instructed_currency_code = strQueue.popBytesString(3);
						data.setInstructed_currency_code(instructed_currency_code);
						if (ETL_Tool_FormatCheck.isEmpty(instructed_currency_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易幣別", "空值"));
						} else if (!checkMaps.get("T_10").containsKey(instructed_currency_code.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易幣別", "非預期"));
						}

						// 交易金額 9(12)V99 *
						String instructed_amount = strQueue.popBytesString(14);
						if (ETL_Tool_FormatCheck.isEmpty(instructed_amount)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易金額", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkNum(instructed_amount)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易金額", "非數字"));
						} else {
							data.setInstructed_amount(ETL_Tool_StringX.strToBigDecimal(instructed_amount, 2));
						}

						// 匯款人銀行帳戶編號 X(50) *
						String ordering_customer_account_number = strQueue.popBytesString(50);
						data.setOrdering_customer_account_number(ordering_customer_account_number);
						if (ETL_Tool_FormatCheck.isEmpty(ordering_customer_account_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯款人銀行帳戶編號", "空值"));
						}

						// 匯款人顧客編號 X(50)
						String ordering_customer_party_id = strQueue.popBytesString(50);
						data.setOrdering_customer_party_id(ordering_customer_party_id);

						// 匯款人顧客姓名 X(80) *
						String ordering_customer_party_name = strQueue.popBytesString(80);
						data.setOrdering_customer_party_name(ordering_customer_party_name);
						if (ETL_Tool_FormatCheck.isEmpty(ordering_customer_party_name)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯款人顧客姓名", "空值"));
						}

						// 匯款人顧客地址 X(100)
						String ordering_customer_address_line = strQueue.popBytesString(100);
						data.setOrdering_customer_address_line(ordering_customer_address_line);

						// 匯款銀行BIC 編碼 X(20) *
						String ordering_bank_bic = strQueue.popBytesString(20);
						data.setOrdering_bank_bic(ordering_bank_bic);
						if (ETL_Tool_FormatCheck.isEmpty(ordering_bank_bic)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯款銀行BIC 編碼", "空值"));
						}else if(ordering_bank_bic.length()<6) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "匯款銀行BIC 編碼", "長度小於6"));
						}else if(!checkMaps.get("BIC").containsKey(getCOMM_NATIONALITY_CODE(ordering_bank_bic.trim()))) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯款銀行BIC 編碼", "非預期"));
						}
						

						// 受款人銀行帳戶編號 X(50) *
						String beneficiary_customer_account_number = strQueue.popBytesString(50);
						data.setBeneficiary_customer_account_number(beneficiary_customer_account_number);
						if (ETL_Tool_FormatCheck.isEmpty(beneficiary_customer_account_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "受款人銀行帳戶編號", "空值"));
						}

						// 受款人姓名 X(80) *
						String beneficiary_customer_party_name = strQueue.popBytesString(80);
						data.setBeneficiary_customer_party_name(beneficiary_customer_party_name);
						if (ETL_Tool_FormatCheck.isEmpty(beneficiary_customer_party_name)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "受款人姓名", "空值"));
						}

						// 受款銀行顧客編號 X(50)
						String beneficiary_customer_party_id = strQueue.popBytesString(50);
						data.setBeneficiary_customer_party_id(beneficiary_customer_party_id);

						// 受款銀行BIC 編碼 X(20) *
						String beneficiary_bank_bic = strQueue.popBytesString(20);
						data.setBeneficiary_bank_bic(beneficiary_bank_bic);
						if (ETL_Tool_FormatCheck.isEmpty(beneficiary_bank_bic)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "受款銀行BIC 編碼", "空值"));
						}else if(beneficiary_bank_bic.length()<6) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(beneficiary_bank_bic), "受款銀行BIC 編碼", "長度小於6"));
						}else if(!checkMaps.get("BIC").containsKey(getCOMM_NATIONALITY_CODE(beneficiary_bank_bic.trim()))) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "受款銀行BIC 編碼", "非預期"));
						}

						// 交易類別 X(04) *
						String transaction_type = strQueue.popBytesString(4);
						data.setTransaction_type(transaction_type);
						if (ETL_Tool_FormatCheck.isEmpty(transaction_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "交易類別", "空值"));
						}

						// 匯款系統 X(10)
						String transaction_system = strQueue.popBytesString(10);
						data.setTransaction_system(transaction_system);

						// 匯款管道類別 X(10)
						String channel_type = strQueue.popBytesString(10);
						data.setChannel_type(channel_type);

						// 匯款管道編號 X(20)
						String channel_id = strQueue.popBytesString(20);
						data.setChannel_id(channel_id);

						// 匯款執行分行 X(20)
						String execution_branch_code = strQueue.popBytesString(20);
						data.setExecution_branch_code(execution_branch_code);

						// 匯款執行者代號 X(50)
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

					insert_TRANSFER_TEMP_Datas();

					// 尾錄檢查
					if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 700 = 723)
						if (strQueue.getTotalByteLength() != 723) {
							fileFmtErrMsg = "尾錄位元數非預期723";
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
						iTotalCount = ETL_Tool_StringX.toInt(totalCount);
						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}

						// 保留欄檢核(700)
						String keepColumn = strQueue.popBytesString(700);

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

					// ETL_FILE_Log寫入DB
					ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), pfn.getRecord_Date(),
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseStartDate, parseEndDate,
							iTotalCount, successCount, failureCount, pfn.getFileName());

					// 累加處理錯誤筆數
					detail_ErrorCount = detail_ErrorCount + failureCount;

				}

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
			ETL_P_Log.update_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "E",
					exe_result, exe_result_description, new Date());

		} catch (Exception ex) {

			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_TRANSFER - End"); //

	}
	
	private static String getCOMM_NATIONALITY_CODE(String str){
			if(str==null || str.length()<6) {
				return null;
			}
			
			return str.substring(4, 6);
	}

	// List增加一個data
	private void addData(ETL_Bean_TRANSFER_TEMP_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_TRANSFER_TEMP_Datas();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}

	private void insert_TRANSFER_TEMP_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_TRANSFER - insert_TRANSFER_TEMP_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_TRANSFER_TEMP(?)}"); // 呼叫寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_TRANSFER_TEMP"); // DB2 array type
		insertAdapter.setCreateStructTypeName("T_TRANSFER_TEMP"); // DB2 type
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_TRANSFER_TEMP_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_TRANSFER_TEMP_Datas 發生錯誤");
		}
	}

}