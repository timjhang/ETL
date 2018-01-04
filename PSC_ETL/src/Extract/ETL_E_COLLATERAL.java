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

import Bean.ETL_Bean_COLLATERAL_Data;
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

public class ETL_E_COLLATERAL {

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { 
			{ "domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "change_code", "COLLATERAL_CHANGE_CODE" }, // 異動代號
			{ "collateral_type", "COMM_COLLATERAL_TYPE" }, // 擔保品類別
			{ "relation_type_code", "COMM_RELATION_TYPE_CODE" } // 與主債務人關係
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// Data儲存List
	private List<ETL_Bean_COLLATERAL_Data> dataList = new ArrayList<ETL_Bean_COLLATERAL_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_COLLATERAL 抓取checkMaps資料有誤!");
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
	public void read_Collateral_File(String filePath, String fileTypeName,
			String batch_no, String exc_central_no, Date exc_record_date, String upload_no, String program_no) {

		System.out.println("#######Extrace - ETL_E_COLLATERAL - Start");

		try {
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(
					batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no, "S", "", "", new Date(), null);
			
			// 處理Party_Phone錯誤計數
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
				
				// System.out.println(parseFile.getAbsoluteFile()); // test
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

					// 檢查整行bytes數(1 + 7 + 8 + 187 = 203)
					if (strQueue.getTotalByteLength() != 203) {
						fileFmtErrMsg = "首錄位元數非預期203";
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

					// 保留欄檢核(187)
					String reserve_field = strQueue.popBytesString(187);

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
						ETL_Bean_COLLATERAL_Data data = new ETL_Bean_COLLATERAL_Data(pfn);
						data.setRow_count(rowCount);

						// 區別碼(1)
						String typeCode = strQueue.popBytesString(1);
						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
							break;
						}

						/*
						 * 整行bytes數檢核(01+07+20+01+20+20+02+14+14+40+11+40+02+11
						 * = 203)
						 */
						if (strQueue.getTotalByteLength() != 203) {
							data.setError_mark("Y");
							fileFmtErrMsg = "明細錄位元數非預期203";
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

						// 擔保品編號 R X(20)*
						String collateral_id = strQueue.popBytesString(20);
						data.setCollateral_id(collateral_id);

						if (ETL_Tool_FormatCheck.isEmpty(collateral_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "擔保品編號", "空值"));
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

						// 批覆書編號/申請書編號 R X(20)*
						String loan_master_number = strQueue.popBytesString(20);
						data.setLoan_master_number(loan_master_number);

						if (ETL_Tool_FormatCheck.isEmpty(loan_master_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "批覆書編號/申請書編號", "空值"));
						}

						// 額度編號 R X(20)*
						String loan_detail_number = strQueue.popBytesString(20);
						data.setLoan_detail_number(loan_detail_number);

						if (ETL_Tool_FormatCheck.isEmpty(loan_detail_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "額度編號", "空值"));
						}

						// 擔保品類別 R X(02)*
						String collateral_type = strQueue.popBytesString(2);
						data.setCollateral_desc(collateral_type);

						if (ETL_Tool_FormatCheck.isEmpty(collateral_type)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "擔保品類別", "空值"));
						} else if (!checkMaps.get("collateral_type").containsKey(collateral_type.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "擔保品類別", "非預期"));
						}

						// 鑑價金額 R 9(12)V99*
						String collateral_value = strQueue.popBytesString(14);
						data.setCollateral_value(ETL_Tool_StringX.strToBigDecimal(collateral_value, 2));

						if (ETL_Tool_FormatCheck.isEmpty(collateral_value)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "鑑價金額 ", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkNum(collateral_value)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "鑑價金額 ", "格式錯誤"));
						}

						// 擔保金額 R 9(12)V99*
						String guarantee_amount = strQueue.popBytesString(14);
						data.setGuarantee_amount(ETL_Tool_StringX.strToBigDecimal(guarantee_amount, 2));

						if (ETL_Tool_FormatCheck.isEmpty(guarantee_amount)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "擔保金額 ", "空值"));
						} else if (!ETL_Tool_FormatCheck.checkNum(guarantee_amount)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "擔保金額 ", "格式錯誤"));
						}

						// 擔保品描述 O X(40)
						String collateral_desc = strQueue.popBytesString(40);
						data.setCollateral_desc(collateral_desc);

						if (advancedCheck && ETL_Tool_FormatCheck.isEmpty(collateral_desc)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "擔保品描述 ", "空值"));
						}

						// 所有權人統編 R X(11)*
						String relation_id = strQueue.popBytesString(11);
						data.setRelation_id(relation_id);

						if (ETL_Tool_FormatCheck.isEmpty(relation_id)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "所有權人統編", "空值"));
						}

						// 所有權人姓名 O X(40)
						String relation_name = strQueue.popBytesString(40);
						data.setRelation_name(relation_name);

						if (advancedCheck && ETL_Tool_FormatCheck.isEmpty(relation_name)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "所有權人姓名 ", "空值"));
						}

						// 與主債務人關係 R X(02)*
						String relation_type_code = strQueue.popBytesString(2);
						data.setRelation_type_code(relation_type_code);

						if (ETL_Tool_FormatCheck.isEmpty(relation_type_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "與主債務人關係", "空值"));
						} else if (!checkMaps.get("relation_type_code").containsKey(relation_type_code.trim())) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "與主債務人關係", "非預期"));
						}

						// 客戶(主債務人)統編 R X(11)*
						String party_number = strQueue.popBytesString(11);
						data.setParty_number(party_number);

						if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "客戶(主債務人)統編", "空值"));
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

				// Collateral_Data寫入DB
				insert_Collateral_Datas();

				// 尾錄檢查
				if ("".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

					// 整行bytes數檢核 (1 + 7 + 8 + 7 + 180 = 203)
					if (strQueue.getTotalByteLength() != 203) {
						fileFmtErrMsg = "尾錄位元數非預期203";
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
					iTotalCount = ETL_Tool_StringX.toInt(totalCount);
					
					if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
						fileFmtErrMsg = "尾錄總筆數格式錯誤";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"總筆數", fileFmtErrMsg));
					} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
						fileFmtErrMsg = "尾錄總筆數與統計不符";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
								String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
					}

					// 保留欄檢核(180)
					String reserve_field = strQueue.popBytesString(180);

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
				
				// ETL_FILE_Log寫入DB
				ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), pfn.getRecord_Date(), pfn.getFile_Type(), pfn.getFile_Name(), upload_no,
						"E", parseStartDate, parseEndDate, iTotalCount, successCount, failureCount, pfn.getFileName());
				
				// 累加PARTY_PHONE處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;
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
			ETL_P_Log.update_ETL_Detail_Log(
					batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", exe_result, exe_result_description, new Date());

		} catch (Exception ex) {

			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_COLLATERAL - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_COLLATERAL_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Collateral_Datas();
			this.dataCount = 0;
			this.dataList.clear();
		}
	}

	// 將COLLATERAL資料寫入資料庫
	private void insert_Collateral_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_COLLATERAL - insert_Collateral_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫COLLATERAL寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_COLLATERAL_TEMP(?)}");
		// DB2 type - COLLATERAL
		insertAdapter.setCreateStructTypeName("T_COLLATERAL");
		// DB2 array type - COLLATERAL
		insertAdapter.setCreateArrayTypesName("A_COLLATERAL");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);

		if (isSuccess) {
			System.out.println("insert_Collateral_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_Collateral_Datas 發生錯誤");
		}
	}

	public static void main(String[] argv) throws IOException {
		
		//讀取測試資料，並只列出明細錄欄位
	    Charset charset = Charset.forName("Big5");
		List<String> lines = Files.readAllLines(Paths.get("D:\\PSC\\Projects\\全國農業金庫洗錢防制系統案\\UNIT_TEST\\952_FR_COLLATERAL_20130807.txt"), charset);
		
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
				System.out.println("擔保品編號X(20): " + new String(Arrays.copyOfRange(tmp, 8, 28), "Big5"));
				System.out.println("異動代號X(01): " + new String(Arrays.copyOfRange(tmp, 28, 29), "Big5"));
				System.out.println("批覆書編號/申請書編號X(20): " + new String(Arrays.copyOfRange(tmp, 29, 49), "Big5"));
				System.out.println("額度編號X(20): " + new String(Arrays.copyOfRange(tmp, 49, 69), "Big5"));
				System.out.println("擔保品類別X(02): " + new String(Arrays.copyOfRange(tmp, 69, 71), "Big5"));
				System.out.println("鑑價金額9(12)V99: " + new String(Arrays.copyOfRange(tmp, 71, 85), "Big5"));
				System.out.println("擔保金額9(12)V99: " + new String(Arrays.copyOfRange(tmp, 85, 99), "Big5"));
				System.out.println("擔保品描述X(40): " + new String(Arrays.copyOfRange(tmp, 99, 139), "Big5"));
				System.out.println("所有權人統編X(11): " + new String(Arrays.copyOfRange(tmp, 139, 150), "Big5"));
				System.out.println("所有權人姓名X(40): " + new String(Arrays.copyOfRange(tmp, 150, 190), "Big5"));
				System.out.println("與主債務人關係X(02): " + new String(Arrays.copyOfRange(tmp, 190, 192), "Big5"));
				System.out.println("客戶(主債務人)統編X(11): " + new String(Arrays.copyOfRange(tmp, 192, 203), "Big5"));
				System.out.println("============================================================================================");
			}
		}
		
		//讀取測試資料，並運行程式
		ETL_E_COLLATERAL one = new ETL_E_COLLATERAL();
		String filePath = "D:\\PSC\\Projects\\全國農業金庫洗錢防制系統案\\UNIT_TEST";
		String fileTypeName = "COLLATERAL";
		one.read_Collateral_File(filePath, fileTypeName, 
				"ETL00001", "951", new Date(), "001", "ETL_E_COLLATERAL");
	}

}
