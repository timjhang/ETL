package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_PARTY_PHONE_Data;
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

public class ETL_E_PARTY_PHONE {
	
	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;
	
	// 欄位檢核用陣列
	private String[][] checkMapArray =
		{
			{"comm_file_type", "COMM_FILE_TYPE"}, // 業務別
			{"c-2", "COMM_DOMAIN_ID"}, // 本會代號
			{"c-4", "PARTY_PHONE_CHANGE_CODE"}, // 異動代號
			{"c-5", "PARTY_PHONE_PHONE_TYPE"} // 電話類別
		};
	
	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;
	
	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;
	
	// list data筆數
	private int dataCount = 0;
	
	// Data儲存List
	private List<ETL_Bean_PARTY_PHONE_Data> dataList = new ArrayList<ETL_Bean_PARTY_PHONE_Data>();
	
	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {
			
			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);
			
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_PARTY_PHONE 抓取checkMaps資料有誤!");
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
	public void read_Party_Phone_File(String filePath, String fileTypeName,
			String batch_no, String exc_central_no, Date exc_record_date, String upload_no, String program_no) throws Exception {
		
		System.out.println("#######Extrace - ETL_E_PARTY_PHONE - Start");
		
		try {
			// TODO V4  Start
			// 批次不重複執
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no)) {
				String inforMation = 
						"batch_no = " + batch_no + ", " +
						"exc_central_no = " + exc_central_no + ", " +
						"exc_record_date = " + exc_record_date + ", " +
						"upload_no = " + upload_no + ", " +
						"step_type = E, " +
						"program_no = " + program_no;
				
				System.out.println("#######Extrace - ETL_E_PARTY_PHONE - 不重複執行\n" + inforMation); // TODO V4
				System.out.println("#######Extrace - ETL_E_PARTY_PHONE - End");  // TODO V4
				
				return;
			}
			// TODO V4  End
			
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(
					batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no, "S", "", "", new Date(), null);
			
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
			for (int i = 0 ; i < fileList.size(); i++) {
				// 取得檔案
				File parseFile = fileList.get(i);
				
				// 檔名
				String fileName = parseFile.getName();
				Date parseStartDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " Start " + parseStartDate);
				
				// 解析fileName物件
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName);
				// 設定批次編號  // TODO V4  搬家
				pfn.setBatch_no(batch_no);
				// 設定上傳批號  // TODO V4
				pfn.setUpload_no(upload_no);
				
				// 報送單位非預期, 不進行解析
				if (exc_central_no == null || "".equals(exc_central_no.trim())) {
					System.out.println("## ETL_E_PARTY_PHONE - read_Party_Phone_File - 控制程式無提供報送單位，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供報送單位，不進行解析！\n";
					continue;
				} else if (!exc_central_no.trim().equals(pfn.getCentral_No().trim())) {
					System.out.println("##" + pfn.getFileName() + " 處理報送單位非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理報送單位非預期，不進行解析！\n";
					continue;
				}
				
				// 業務別非預期, 不進行解析
				if (pfn.getFile_Type() == null 
						|| "".equals(pfn.getFile_Type().trim()) 
						|| !checkMaps.get("comm_file_type").containsKey(pfn.getFile_Type().trim())) {
					
					System.out.println("##" + pfn.getFileName() + " 處理業務別非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理業務別非預期，不進行解析！\n";
					continue;
				}
				
				// 資料日期非預期, 不進行解析
				if (exc_record_date == null) {
					System.out.println("## ETL_E_PARTY_PHONE - read_Party_Phone_File - 控制程式無提供資料日期，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供資料日期，不進行解析！\n";
					continue;
				} else if (!exc_record_date.equals(pfn.getRecord_Date())) {
					System.out.println("## " + pfn.getFileName() + " 處理資料日期非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理資料日期非預期，不進行解析！\n";
					continue;
				}
				
//				System.out.println(parseFile.getAbsoluteFile()); // test
				// TODO V4
				// 設定批次編號 已搬家
				//pfn.setBatch_no(batch_no);
//				FileInputStream fis = new FileInputStream(parseFile);
//				BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
				
				// rowCount == 處理行數
				int rowCount = 1; // 從1開始
				// 成功計數
				int successCount = 0;
				// 失敗計數
				int failureCount = 0;
				// 尾錄總數
				int iTotalCount = 0;
				
				try {
					
					// 開始前ETL_FILE_Log寫入DB
					ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no() , pfn.getCentral_No(), exc_record_date, pfn.getFile_Type(), pfn.getFile_Name(), upload_no,
							"E", parseStartDate, null, 0, 0, 0, pfn.getFileName());
				
					// 嚴重錯誤訊息變數(讀檔)
					String fileFmtErrMsg = ""; 
					
					String lineStr = ""; // 行字串暫存區
					
					// ETL_字串處理Queue // TODO V4
					ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue(exc_central_no); 
					// ETL_Error Log寫入輔助工具
					ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();
					// TODO V4  Start
					// 讀檔並將結果注入ETL_字串處理Queue
					strQueue.setBytesList(ETL_Tool_FileByteUtil.getFilesBytes(parseFile.getAbsolutePath()));
					// 首、明細、尾錄, 基本組成檢查
					boolean isFileFormatOK = ETL_Tool_FileFormat.checkBytesList(strQueue.getBytesList());
					// TODO V4  End
					
					
					// 首錄檢查
					if (isFileFormatOK) { // TODO V4
//						lineStr = br.readLine(); // TODO V4
						
						// 注入首錄字串
//						strQueue.setTargetString(lineStr); // TODO V4
						
						// strQueue工具注入第一筆資料  // TODO V4
						strQueue.setTargetString();
						
						// 檢查整行bytes數(1 + 7 + 8 + 27 = 43)
						if (strQueue.getTotalByteLength() != 43) {
							fileFmtErrMsg = "首錄位元數非預期43:" + strQueue.getTotalByteLength();// TODO V4
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}
						
						// 區別瑪檢核(1)
						String typeCode = strQueue.popBytesString(1);
						if (!"1".equals(typeCode)) { // 首錄區別碼檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄區別碼有誤:"+typeCode;
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}
						
						// 報送單位檢核(7)   
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息 
							fileFmtErrMsg = "首錄報送單位代碼與檔名不符:" + central_no;  
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}
						
						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (record_date == null || "".equals(record_date.trim())) {
							fileFmtErrMsg = "首錄檔案日期空值";
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!record_date.equals(pfn.getRecord_Date_String())) {
							fileFmtErrMsg = "首錄檔案日期與檔名不符:" + record_date;  
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "首錄檔案日期格式錯誤:" + record_date; 
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}
						
						// 保留欄位檢核(27)
						String keepColumn = strQueue.popBytesString(27);
						
						rowCount++; // 處理行數  + 1
					}
					
					// 逐行讀取檔案
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行  // TODO V4
						while (strQueue.setTargetString() < strQueue.getByteListSize()) { // TODO V4
							
	//						lineStr = br.readLine();  // TODO V4
		//					System.out.println(lineStr); // test
	//						strQueue.setTargetString(lineStr); // queue裝入新String  // TODO V4
							
							// 生成一個Data
							ETL_Bean_PARTY_PHONE_Data data = new ETL_Bean_PARTY_PHONE_Data(pfn);
							// 寫入資料行數
							data.setRow_count(rowCount);
							
							// 區別碼(1)  // TODO V4
	//						String typeCode = strQueue.popBytesString(1);
	//						if ("3".equals(typeCode)) { // 區別碼為3, 跳出迴圈處理尾錄
	//							break;
	//						}
		
							// 整行bytes數檢核(1 + 7 + 11 + 1 + 3 + 20 = 43)
							if (strQueue.getTotalByteLength() != 43) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", "非預期43:" + strQueue.getTotalByteLength()));
								
								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行 
								failureCount++;
								rowCount++;
								continue;
							}
							
							// 區別碼檢核 c-1*
							String typeCode = strQueue.popBytesString(1); // TODO V4
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "區別碼", "非預期:" + typeCode));
							}
							
							// 本會代號檢核(7) c-2*
							String domain_id = strQueue.popBytesString(7);
							data.setDomain_id(domain_id);
							if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "本會代號", "空值"));
							} else if (advancedCheck && !checkMaps.get("c-2").containsKey(domain_id)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "本會代號", "非預期:" + domain_id));
							}
							
							// 客戶統編檢核(11) c-3*
							String party_number = strQueue.popBytesString(11);
							data.setParty_number(party_number);
							if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "客戶統編", "空值"));
							}
							
							// 異動代號檢核(1) c-4*
							String change_code = strQueue.popBytesString(1);
							data.setChange_code(change_code);
							if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "異動代號", "空值"));
							} else if (advancedCheck && !checkMaps.get("c-4").containsKey(change_code)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "異動代號", "非預期:" + change_code));
							}
							
							// 電話類別檢核(3) c-5
							String phone_type = strQueue.popBytesString(3);
							data.setPhone_type(phone_type);
							if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(phone_type) && !checkMaps.get("c-5").containsKey(phone_type)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "電話類別", "非預期:" + phone_type));
							}
							
							// 電話號碼檢核(20) c-6*
							String phone_number = strQueue.popBytesString(20);
							data.setPhone_number(phone_number);
							if (ETL_Tool_FormatCheck.isEmpty(phone_number)) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "電話號碼", "空值"));
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
					}
					
					System.out.println("this.dataList size = " + this.dataList.size());// test  temp
					// Party_Phone_Data寫入DB
					insert_Party_Phone_Datas();
					
					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行  // TODO V4
						
						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 20 = 43)
						if (strQueue.getTotalByteLength() != 43) {
							fileFmtErrMsg = "尾錄位元數非預期43:" + strQueue.getTotalByteLength();  // TODO V4
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}
						
						// 區別碼檢核(1) // TODO V4  Start
						String typeCode = strQueue.popBytesString(1);
						if (!"3".equals(typeCode)) {
							fileFmtErrMsg = "尾錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}
						// TODO V4  End
						
						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "尾錄報送單位代碼與檔名不符:" + central_no;  
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}
						
						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (record_date == null || "".equals(record_date.trim())) {
							fileFmtErrMsg = "尾錄檔案日期空值";
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!record_date.equals(pfn.getRecord_Date_String())) {
							fileFmtErrMsg = "尾錄檔案日期與檔名不符:" + record_date;  
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "尾錄檔案日期格式錯誤:" + record_date;  
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}
						
						// 總筆數檢核(7)
						String totalCount = strQueue.popBytesString(7);
						iTotalCount = ETL_Tool_StringX.toInt(totalCount);
						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤:" + totalCount;  
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符:" + totalCount;  
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}
						
						// 保留欄檢核(20)
						String keepColumn = strQueue.popBytesString(20);
						
						
						// 程式統計檢核 
						if ((rowCount - 2) != (successCount + failureCount)) {
							fileFmtErrMsg = "總筆數 <> 成功比數 + 失敗筆數";
							errWriter.addErrLog(
									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "程式檢核", fileFmtErrMsg));
						}
						
						// 多餘行數檢查  // TODO  V4
//						if (br.ready()) {
//							fileFmtErrMsg = "出現多餘行數";
//							errWriter.addErrLog(
//									new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount), "檔案總行數", fileFmtErrMsg));
//							rowCount++;
//						}
					}
					
//					fis.close(); // TODO V4
					Date parseEndDate = new Date(); // 開始執行時間
					System.out.println("解析檔案： " + fileName + " End " + parseEndDate);
					
					// Error_Log寫入DB  // TODO V4
//					errWriter.insert_Error_Log();
					
					// 執行結果
					String file_exe_result;
					// 執行結果說明
					String file_exe_result_description;
					
					if (!isFileFormatOK) {  // TODO V4
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
//						file_exe_result_description = "錯誤資料筆數: " + detail_ErrorCount; // TODO V4
						file_exe_result_description = "錯誤資料筆數: " + failureCount; // TODO V4
					}
					
					// Error_Log寫入DB  // TODO V4  搬家
					errWriter.insert_Error_Log();
					
					// 處理後更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no() , pfn.getCentral_No(), exc_record_date, pfn.getFile_Type(), pfn.getFile_Name(), upload_no,
							"E", parseEndDate, iTotalCount , successCount, failureCount, file_exe_result, file_exe_result_description);
					
				} catch (Exception ex) {
					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no() , pfn.getCentral_No(), exc_record_date, pfn.getFile_Type(), pfn.getFile_Name(), upload_no,
							"E", new Date(), 0, 0, 0, "S", ex.getMessage()); // TODO V4  (0, 0, 0)<=(iTotalCount, successCount, failureCount)
					processErrMsg = processErrMsg + ex.getMessage() + "\n";
					
					ex.printStackTrace();
				}
				
				// 累加PARTY_PHONE處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;
			}
			
			// 執行結果
			String detail_exe_result;
			// 執行結果說明
			String detail_exe_result_description;
			
			if (fileList.size() == 0) { // TODO V4
				detail_exe_result = "S";
				detail_exe_result_description = "缺檔案類型：" + fileTypeName + " 檔案";
				
				// ETL_Error Log寫入輔助工具
				ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();
				// 寫入一筆Error Log
				errWriter.addErrLog(
						new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, null, fileTypeName, 
								upload_no, "E", "0", "ETL_E_PARTY_PHONE程式處理", detail_exe_result_description, null)); // TODO V4
				// Error_Log寫入DB
				errWriter.insert_Error_Log();
				
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
			ETL_P_Log.update_End_ETL_Detail_Log(
					batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", detail_exe_result, detail_exe_result_description, new Date());
		
		} catch (Exception ex) {
			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log (
					batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());
			
			ex.printStackTrace();
		}
		
		System.out.println("#######Extrace - ETL_E_PARTY_PHONE - End");
	}
	
	// List增加一個data
	private void addData(ETL_Bean_PARTY_PHONE_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;
		
		if (dataCount == stageLimit) {
			insert_Party_Phone_Datas();
//			this.dataCount = 0; // TODO V4
//			this.dataList.clear();
		}
	}
	
	// 將PARTY_PHONE資料寫入資料庫
	private void insert_Party_Phone_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_PARTY_PHONE - insert_Party_Phone_Datas 無寫入任何資料");
			return;
		}
		
		InsertAdapter insertAdapter = new InsertAdapter(); 
		insertAdapter.setSql("{call SP_INSERT_PARTY_PHONE_TEMP(?)}"); // 呼叫PARTY_PHONE寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_PARTY_PHONE"); // DB2 array type - PARTY_PHONE
		insertAdapter.setCreateStructTypeName("T_PARTY_PHONE"); // DB2 type - PARTY_PHONE
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage);  // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.dataList, insertAdapter);
		
		if (isSuccess) {
			System.out.println("insert_Party_Phone_Datas 寫入 " + this.dataList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_Party_Phone_Datas 發生錯誤");
		}
		
		// TODO V4
		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}
	
	public static void main(String[] argv) throws Exception {
		ETL_E_PARTY_PHONE one = new ETL_E_PARTY_PHONE();
		String filePath = "C:/Users/10404003/Desktop/農經/2018/180205/test";
		String fileTypeName = "PARTY_PHONE";
		one.read_Party_Phone_File(filePath, fileTypeName, 
				"tim00003", "600", new SimpleDateFormat("yyyyMMdd").parse("20171206"), "001", "ETL_E_PARTY_PHONE");
	}
	
}
