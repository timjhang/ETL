package Extract;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ACCOUNT_Data;
import Bean.ETL_Bean_ErrorLog_Data;
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

public class ETL_E_ACCOUNT {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	// TODO
	private String[][] checkMapArray = { { "domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "change_code", "ACCOUNT_CHANGE_CODE" }, // 異動代號
			{ "account_type_code", "ACCOUNT_ACCOUNT_TYPE_CODE" }, // 帳戶類別
			{ "property_code", "ACCOUNT_PROPERTY_CODE" }, // 連結服務
			{ "currency_code", "COMM_CURRENCY_CODE" }, // 幣別
			{ "status_code", "ACCOUNT_STATUS_CODE" }, // 帳戶狀態
			{ "account_opening_channel", "ACCOUNT_ACCOUNT_OPENING_CHANNEL" }, // 開戶管道
			{ "balance_acct_currency_sign", "ACCOUNT_BALANCE_ACCT_CURRENCY_SIGN" }, // 帳戶餘額正負號
			{ "balance_last_month_avg_sign", "ACCOUNT_BALANCE_LAST_MONTH_AVG_SIGN" }, // 過去一個月平均餘額正負號
			{ "caution_note", "ACCOUNT_CAUTION_NOTE" }// 警示註記
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	// TODO
	private List<ETL_Bean_ACCOUNT_Data> dataList = new ArrayList<ETL_Bean_ACCOUNT_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_ACCOUNT 抓取checkMaps資料有誤!"); // TODO
			ex.printStackTrace();
		}
	};

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	// TODO
	public void read_Account_File(String filePath, String fileTypeName, String upload_no) {

		System.out.println("#######Extrace - ETL_E_ACCOUNT - Start"); // TODO

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

					// 檢查整行bytes數(1 + 7 + 8 + 97 = 113)
					if (strQueue.getTotalByteLength() != 115) {// TODO
						fileFmtErrMsg = "首錄位元數非預期115";// TODO
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
					if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查,
																	// 嚴重錯誤,
																	// 不進行迴圈並記錄錯誤訊息
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

					// 保留欄檢核(97)
					String reserve_field = strQueue.popBytesString(97);

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
						ETL_Bean_ACCOUNT_Data data = new ETL_Bean_ACCOUNT_Data(pfn, null, null, null, null, null, null,
								null, null, null, null, null, null, null, null, null, null, null);

						// 區別碼(1)
						String typeCode = strQueue.popBytesString(1);
						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
							break;
						}

						// 整行bytes數檢核(1 + 7 + 11 + 1 + 30 + 7 + 2 + 1 + 3 + 1 +
						// 1 + 8 + 8 + 1 + 12 + 1 + 12 + 2 = 109)
						// TODO
						if (strQueue.getTotalByteLength() != 111) {
							data.setError_mark("Y");
							fileFmtErrMsg = "非預期111";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));

							// 資料bytes不正確, 為格式嚴重錯誤, 跳出迴圈不繼續執行
							break;
						}

						/*
						 * 區別碼 R X(01) * 本會代號 R X(07) * 客戶統編 R X(11) * 異動代號 R
						 * X(01) * 帳號 R X(30) * 帳戶行 R X(07) * 帳戶類別 R X(02) *
						 * 連結服務 R X(01) * 幣別 R X(03) * 帳戶狀態 R X(01) * 開戶管道 R
						 * X(01) * 開戶日期 R X(08) * 結清(銷戶)日期 O X(08) * 帳戶餘額正負號 R
						 * X(01) * 帳戶餘額 R 9(12)V99 * 過去一個月平均餘額正負號 R X(01)
						 * 過去一個月平均餘額 R 9(12)V99 警示註記 O X(02)
						 */

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

						// 異動代號檢核 R X(01)*
						String change_code = strQueue.popBytesString(1);
						data.setChange_code(change_code);
						if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "異動代號", "空值"));
						} else if (!checkMaps.get("change_code").containsKey(change_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "異動代號", "非預期"));
						}

						// 帳號 R X(30)*
						String account_id = strQueue.popBytesString(30);
						data.setAccount_id(account_id);
						if (ETL_Tool_FormatCheck.isEmpty(account_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "帳號", "空值"));
						}

						// 帳戶行 R X(07)*
						String branch_code = strQueue.popBytesString(7);
						data.setBranch_code(branch_code);
						if (ETL_Tool_FormatCheck.isEmpty(branch_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "帳戶行", "空值"));
						}

						// 帳戶類別 R X(02)*
						String account_type_code = strQueue.popBytesString(2);
						data.setAccount_type_code(account_type_code);
						if (ETL_Tool_FormatCheck.isEmpty(account_type_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "帳戶類別", "空值"));
						} else if (!checkMaps.get("account_type_code").containsKey(account_type_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "帳戶類別", "非預期"));
						}

						// 連結服務 R X(01)*
						String property_code = strQueue.popBytesString(1);
						data.setProperty_code(property_code);
						if (ETL_Tool_FormatCheck.isEmpty(property_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "連結服務", "空值"));
						} else if (!checkMaps.get("property_code").containsKey(property_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "連結服務", "非預期"));
						}

						// 幣別 R X(03)*
						String currency_code = strQueue.popBytesString(3);
						data.setCurrency_code(currency_code);
						if (ETL_Tool_FormatCheck.isEmpty(currency_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "幣別", "空值"));
						} else if (!checkMaps.get("currency_code").containsKey(currency_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "幣別", "非預期"));
						}

						// 帳戶狀態 R X(01)*
						String status_code = strQueue.popBytesString(1);
						data.setStatus_code(status_code);
						if (ETL_Tool_FormatCheck.isEmpty(status_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "帳戶狀態", "空值"));
						} else if (!checkMaps.get("status_code").containsKey(status_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "帳戶狀態", "非預期"));
						}

						// 開戶管道 R X(01)*
						String account_opening_channel = strQueue.popBytesString(1);
						data.setAccount_opening_channel(account_opening_channel);
						if (ETL_Tool_FormatCheck.isEmpty(account_opening_channel)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "開戶管道", "空值"));
						} else if (!checkMaps.get("account_opening_channel").containsKey(account_opening_channel)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "開戶管道", "非預期"));
						}

						// 開戶日期 R X(08)*
						String account_open_date = strQueue.popBytesString(8);
						if (ETL_Tool_FormatCheck.isEmpty(account_open_date)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkDate(account_open_date)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", "格式錯誤"));
						} else {
							data.setAccount_open_date(ETL_Tool_StringX.toUtilDate(account_open_date));
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

				// Account_Data寫入DB
				insert_Account_Datas();// TODO

				// 尾錄檢查
				if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

					// 整行bytes數檢核 (1 + 7 + 8 + 7 + 114 = 137)
					if (strQueue.getTotalByteLength() != 137) { // TODO
						fileFmtErrMsg = "尾錄位元數非預期134";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"行數bytes檢查", fileFmtErrMsg));
					}

					// 區別碼檢核(1) 經"逐行讀取檔案"區塊, 若無嚴重錯誤應為3, 此處無檢核

					// 報送單位檢核(7)
					String central_no = strQueue.popBytesString(7);
					if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查,
																	// 嚴重錯誤,
																	// 不進行迴圈並記錄錯誤訊息
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
					} else if (Integer.valueOf(totalCount) == (rowCount - 2)) {
						fileFmtErrMsg = "尾錄總筆數與統計不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
								String.valueOf(rowCount - 2), "總筆數", fileFmtErrMsg));
					}

					// 保留欄檢核(111)
					String reserve_field = strQueue.popBytesString(111);
					// if (ETL_Tool_FormatCheck.isEmpty(reserve_field)) {
					// fileFmtErrMsg = "尾錄保留欄空值";
					// errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn,
					// upload_no, "E", String.valueOf(rowCount),
					// "保留欄", fileFmtErrMsg));
					// }

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

		System.out.println("#######Extrace - ETL_E_ACCOUNT - End"); // TODO
	}

	// List增加一個data
	// TODO
	private void addData(ETL_Bean_ACCOUNT_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Account_Datas();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}

	// 將ACCOUNT資料寫入資料庫
	// TODO
	private void insert_Account_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_ACCOUNT - insert_Account_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫ACCOUNT寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_ACCOUNT_TEMP(?)}");
		// DB2 type - ACCOUNT
		insertAdapter.setCreateArrayTypesName("T_ACCOUNT");
		// DB2 array type - ACCOUNT
		insertAdapter.setCreateStructTypeName("A_ACCOUNT");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_Account_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_Account_Datas 發生錯誤");
		}
	}

	public static void main(String[] argv) {
		ETL_E_ACCOUNT one = new ETL_E_ACCOUNT();
		String filePath = "D:/aaa";
		String fileTypeName = "ACCOUNT";
		one.read_Account_File(filePath, fileTypeName, "001");
	}

}