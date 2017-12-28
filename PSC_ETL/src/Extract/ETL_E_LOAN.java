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
import Bean.ETL_Bean_LOAN_Data;
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

public class ETL_E_LOAN {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { 
			{ "domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "change_code", "LOAN_CHANGE_CODE" }, // 異動代號
			{ "loan_category_code", "LOAN_LOAN_CATEGORY_CODE" }, // 放款種類
			{ "loan_type_code", "LOAN_LOAN_TYPE_CODE" }, // 利率類別
			{ "loan_currency_code", "COMM_CURRENCY_CODE" }, // 幣別
			{ "loan_status_code", "LOAN_LOAN_STATUS_CODE" },// 放款狀態
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	private List<ETL_Bean_LOAN_Data> dataList = new ArrayList<ETL_Bean_LOAN_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_LOAN 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}
	};

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_Loan_File(String filePath, String fileTypeName, String upload_no) {

		System.out.println("#######Extrace - ETL_E_LOAN - Start");

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

					// 檢查整行bytes數(1 + 7 + 8 + 198 = 214)
					if (strQueue.getTotalByteLength() != 214) {
						fileFmtErrMsg = "首錄位元數非預期214";
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

					// 保留欄檢核(198)
					String reserve_field = strQueue.popBytesString(198);

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
						ETL_Bean_LOAN_Data data = new ETL_Bean_LOAN_Data(pfn);
						data.setRow_count(rowCount);

						// 區別碼(1)
						String typeCode = strQueue.popBytesString(1);
						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
							break;
						}

						/*
						 * 整行bytes數檢核(01+07+11+01+30+08+20+20+02+01+03+14+03+14+
						 * 14+04+07+14+40 = 214)
						 */
						if (strQueue.getTotalByteLength() != 214) {
							data.setError_mark("Y");
							fileFmtErrMsg = "明細錄位元數非預期214";
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
						} else if (!checkMaps.get("domain_id").containsKey(domain_id.trim())) {
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
									String.valueOf(rowCount), "異動代號", "非預期"));
						}

						// 放款帳號 R X(30)*
						String loan_number = strQueue.popBytesString(30);
						data.setLoan_number(loan_number);

						if (ETL_Tool_FormatCheck.isEmpty(loan_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "放款帳號", "空值"));
						}

						// 初貸日 R X(08)*
						String loan_date = strQueue.popBytesString(8);
						data.setLoan_date(ETL_Tool_StringX.toUtilDate(loan_date));

						if (ETL_Tool_FormatCheck.isEmpty(loan_date)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "初貸日", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkDate(loan_date)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "初貸日", "格式錯誤"));
						}

						// 批覆書編號 R X(20)*
						String loan_master_number = strQueue.popBytesString(20);
						data.setLoan_master_number(loan_master_number);

						if (ETL_Tool_FormatCheck.isEmpty(loan_master_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "批覆書編號", "空值"));
						}

						// 額度編號 R X(20)*
						String loan_detail_number = strQueue.popBytesString(20);
						data.setLoan_detail_number(loan_detail_number);

						if (ETL_Tool_FormatCheck.isEmpty(loan_detail_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "額度編號", "空值"));
							// 系統若無額度設計，請放批覆書編號
							data.setLoan_detail_number(loan_master_number);
						}

						// 放款種類 R X(02)*
						String loan_category_code = strQueue.popBytesString(2);
						data.setLoan_category_code(loan_category_code);

						if (ETL_Tool_FormatCheck.isEmpty(loan_category_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "放款種類", "空值"));
						} else if (!checkMaps.get("loan_category_code").containsKey(loan_category_code.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "放款種類", "非預期"));
						}

						// 利率類別 R X(01)*
						String loan_type_code = strQueue.popBytesString(1);
						data.setLoan_type_code(loan_type_code);

						if (ETL_Tool_FormatCheck.isEmpty(loan_type_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "利率類別", "空值"));
						} else if (!checkMaps.get("loan_type_code").containsKey(loan_type_code.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "利率類別", "非預期"));
						}

						// 幣別 R X(03)*
						String loan_currency_code = strQueue.popBytesString(3);
						data.setLoan_currency_code(loan_currency_code);

						if (ETL_Tool_FormatCheck.isEmpty(loan_currency_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "幣別", "空值"));
						} else if (!checkMaps.get("loan_currency_code").containsKey(loan_currency_code.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "幣別", "非預期"));
						}

						// 貸放金額 R 9(12)V99*
						String loan_amount = strQueue.popBytesString(14);
						data.setLoan_amount(ETL_Tool_StringX.strToBigDecimal(loan_amount, 2));

						if (ETL_Tool_FormatCheck.isEmpty(loan_amount)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "貸放金額", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkNum(loan_amount)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "貸放金額", "格式錯誤"));
						}

						// 放款狀態 R X(03)*
						String loan_status_code = strQueue.popBytesString(3);
						data.setLoan_status_code(loan_status_code);

						if (ETL_Tool_FormatCheck.isEmpty(loan_status_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "放款狀態", "空值"));
						} else if (!checkMaps.get("loan_status_code").containsKey(loan_status_code.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "放款狀態", "非預期"));
						}

						// 本金餘額 R 9(12)V99*
						String outstanding_loan_balance = strQueue.popBytesString(14);
						data.setOutstanding_loan_balance(ETL_Tool_StringX.strToBigDecimal(outstanding_loan_balance, 2));

						if (ETL_Tool_FormatCheck.isEmpty(outstanding_loan_balance)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "本金餘額", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkNum(outstanding_loan_balance)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "本金餘額", "格式錯誤"));
						}

						// 已還本金 R 9(12)V99*
						String total_repayments_value = strQueue.popBytesString(14);
						data.setTotal_repayments_value(ETL_Tool_StringX.strToBigDecimal(total_repayments_value, 2));

						if (ETL_Tool_FormatCheck.isEmpty(total_repayments_value)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "已還本金", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkNum(total_repayments_value)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "已還本金", "格式錯誤"));
						}

						// 違繳天數 R 9(4)*
						String delinquency_days = strQueue.popBytesString(4);
						data.setDelinquency_days(ETL_Tool_StringX.toInt(delinquency_days));

						if (ETL_Tool_FormatCheck.isEmpty(delinquency_days)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "違繳天數", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkNum(delinquency_days)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "違繳天數", "格式錯誤"));
						}

						// 帳戶行 R X(07)*
						String execution_branch_code = strQueue.popBytesString(7);
						data.setExecution_branch_code(execution_branch_code);

						if (ETL_Tool_FormatCheck.isEmpty(execution_branch_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "帳戶行", "空值"));
						}

						// 到期一次還本金額 O 9(12)V99
						String last_payment_value = strQueue.popBytesString(14);
						data.setLast_payment_value(ETL_Tool_StringX.strToBigDecimal(last_payment_value, 2));

						if (advancedCheck && !ETL_Tool_FormatCheck.checkNum(last_payment_value)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "到期一次還本金額", "格式錯誤"));
						}

						// 收回原因 O X(40)
						String cls = strQueue.popBytesString(40);
						data.setCls(cls);

						// data list 加入一個檔案
						addData(data);

						if ("Y".equals(data.getError_mark())) {
							failureCount++;
						} else {
							successCount++;
						}
						rowCount++; // 處理行數 + 1
					}

				// Loan_Data寫入DB 
				insert_Loan_Datas();

				// 尾錄檢查
				if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

					// 整行bytes數檢核 (1 + 7 + 8 + 7 + 191 = 214)
					if (strQueue.getTotalByteLength() != 214) {
						fileFmtErrMsg = "尾錄位元數非預期214";
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

					// 保留欄檢核(191)
					String reserve_field = strQueue.popBytesString(191);

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

		System.out.println("#######Extrace - ETL_E_LOAN - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_LOAN_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Loan_Datas();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}

	// 將LOAN資料寫入資料庫
	private void insert_Loan_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_LOAN - insert_Loan_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫LOAN寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_LOAN_TEMP(?)}");
		// DB2 type - LOAN
		insertAdapter.setCreateStructTypeName("T_LOAN");
		// DB2 array type - LOAN
		insertAdapter.setCreateArrayTypesName("A_LOAN");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_Loan_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_Loan_Datas 發生錯誤");
		}
	}

	public static void main(String[] argv) throws IOException {
		
		//讀取測試資料，並只列出明細錄欄位
	    Charset charset = Charset.forName("Big5");
		List<String> lines = Files.readAllLines(Paths.get("D:\\PSC\\Projects\\全國農業金庫洗錢防制系統案\\UNIT_TEST\\LOAN.txt"), charset);
		
		if ( lines.size() > 2 ){
			
			lines.remove(0);
			lines.remove(lines.size()-1);
			
			System.out.println("============================================================================================");
			for (String line : lines) {
				byte[] tmp = line.getBytes(charset);
				System.out.println("第"+ ( lines.indexOf(line) + 1 ) + "行");
				System.out.println("位元組長度: "+ tmp.length);
				System.out.println("區別碼X(01): " + new String(Arrays.copyOfRange(tmp, 0, 1), "Big5"));
				System.out.println("本會代號X(07): " + new String(Arrays.copyOfRange(tmp, 1, 8), "Big5"));
				System.out.println("客戶統編X(11): " + new String(Arrays.copyOfRange(tmp, 8, 19), "Big5"));
				System.out.println("異動代號X(01): " + new String(Arrays.copyOfRange(tmp, 19, 20), "Big5"));
				System.out.println("放款帳號X(30): " + new String(Arrays.copyOfRange(tmp, 20, 50), "Big5"));
				System.out.println("初貸日X(08): " + new String(Arrays.copyOfRange(tmp, 50, 58), "Big5"));
				System.out.println("批覆書編號X(20): " + new String(Arrays.copyOfRange(tmp, 58, 78), "Big5"));
				System.out.println("額度編號X(20): " + new String(Arrays.copyOfRange(tmp, 78, 98), "Big5"));
				System.out.println("放款種類X(02): " + new String(Arrays.copyOfRange(tmp, 98, 100), "Big5"));
				System.out.println("利率類別X(01): " + new String(Arrays.copyOfRange(tmp, 100, 101), "Big5"));
				System.out.println("幣別X(03): " + new String(Arrays.copyOfRange(tmp, 101, 104), "Big5"));
				System.out.println("貸放金額9(12)V99): " + new String(Arrays.copyOfRange(tmp, 104, 118), "Big5"));
				System.out.println("放款狀態X(03): " + new String(Arrays.copyOfRange(tmp, 118, 121), "Big5"));
				System.out.println("本金餘額9(12)V99): " + new String(Arrays.copyOfRange(tmp, 121, 135), "Big5"));
				System.out.println("已還本金9(12)V99): " + new String(Arrays.copyOfRange(tmp, 135, 149), "Big5"));
				System.out.println("違繳天數9(04): " + new String(Arrays.copyOfRange(tmp, 149, 153), "Big5"));
				System.out.println("帳戶行X(07): " + new String(Arrays.copyOfRange(tmp, 153, 160), "Big5"));
				System.out.println("到期一次還本金額9(12)V99: " + new String(Arrays.copyOfRange(tmp, 160, 174), "Big5"));
				System.out.println("收回原因X(40): " + new String(Arrays.copyOfRange(tmp, 174, 214), "Big5"));
				System.out.println("============================================================================================");
			}
		}
		
		//讀取測試資料，並運行程式
		ETL_E_LOAN one = new ETL_E_LOAN();
		String filePath = "D:\\PSC\\Projects\\全國農業金庫洗錢防制系統案\\UNIT_TEST";
		String fileTypeName = "LOAN";
		one.read_Loan_File(filePath, fileTypeName, "001");
	}

}
