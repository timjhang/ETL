package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.ETL_Q_ColumnCheckCodes;
import Tool.ETL_Tool_FormatCheck;

public class ETL_E_Wrong_File {

	// 欄位檢核用母Map
	private static Map<String, Map<String, String>> checkMaps;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "comm_central_no", "COMM_CENTRAL_NO" }, // 報送單位
			{ "comm_file_name", "COMM_FILE_NAME" } // 程式檔案名稱

	};

	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {
			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;

			System.out.println("ETL_E_FCX 抓取checkMaps資料有誤!"); 
			ex.printStackTrace();
		}
	};

	// 取得錯誤檔名 並寫入到error Log
	public void record_Error_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		try {
			System.out.println("#######Extrace - ETL_E_Wrong_File - Start");

			// ETL_Error Log寫入輔助工具
			ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();
			
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "Y",
					"", "", new Date(), null);

			// 開始前ETL_FILE_Log寫入DB
			ETL_P_Log.write_ETL_FILE_Log(batch_no, exc_central_no, exc_record_date, "", "", upload_no, "E", new Date(),
					null, 0, 0, 0, "");

			// 讀取路徑, 先檢查相關權限是否ok
			File file = new File(filePath);
			if (!file.exists()) {
				throw new Exception(filePath + " 此路徑不存在! 請確認");
			} else if (!file.isDirectory()) {
				throw new Exception(filePath + " 並非資料夾路徑! 請確認");
			} else if (!file.canRead()) {
				throw new Exception(filePath + " 此路徑無讀取權限! 請確認");
			}

			// 取得檔名list
			String[] fileNameArray = file.list();

			System.out.println("ETL_Tool_FileReader 所有檔名");
			for (int i = 0; i < fileNameArray.length; i++) {
				System.out.println(fileNameArray[i]);
			}

			// 程式執行錯誤訊息 //
			String processErrMsg = "";

			// ETL_Tool_ParseFileName pfn;
			for (int i = 0; i < fileNameArray.length; i++) {

				// 取得副檔名
				String[] bits = fileNameArray[i].split("\\.");
				String lastOne = bits[bits.length - 1];

				// 檢查副檔名
				if ((!"TXT".equals(lastOne.toUpperCase())) || bits.length > 2) {
					System.out.println(fileNameArray[i] + "  副檔名錯誤");
					//注意  row_count 為了不讓 pk 重複所以各項row_count用String.valueOf(i) 
					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "", "",
							upload_no, "E", String.valueOf(i), "", "E001", fileNameArray[i]));
					processErrMsg = "上傳檔名錯誤";
					continue;
				}

				// 檢查 _ 切割後的陣列 小於三
				if ((fileNameArray[i].split("\\_").length < 3)) {
					System.out.println(fileNameArray[i] + " 檔名格式錯誤");
					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "", "",
							upload_no, "E", String.valueOf(i), "", "E001", fileNameArray[i]));
					processErrMsg = "上傳檔名錯誤";
					continue;
				}

				// 解析檔名成為物件
				FileNameParseBean fileNameParseBean = parseFileName(fileNameArray[i]);

//				if (!checkMaps.get("comm_central_no").containsKey(fileNameParseBean.getCentral_No())) {
//					System.out.println(fileNameArray[i] + " 報送單位錯誤");
//					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "", "",
//							upload_no, "E", String.valueOf(i), "", "E002", fileNameArray[i]));
//					processErrMsg = "上傳檔名錯誤";
//					continue;
//				}
				
				
				if (!exc_central_no.equals(fileNameParseBean.getCentral_No())) {
					System.out.println(fileNameArray[i] + " 報送單位錯誤");
					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "", "",
							upload_no, "E", String.valueOf(i), "", "E002", fileNameArray[i]));
					processErrMsg = "上傳檔名錯誤";
					continue;
				}

				// 檢查業務別
				if (!checkMaps.get("comm_file_type").containsKey(fileNameParseBean.getFile_Type())) {
					if (("FCX".equals(fileNameParseBean.getFile_Name())
							|| "CALENDAR".equals(fileNameParseBean.getFile_Name()))
							&& fileNameParseBean.getFile_Type() == null) {
					
						// FCX ,CALENDAR File_Type = null 正常
						
					} else {
						System.out.println(fileNameArray[i] + " 業務別錯誤");
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "",
								"", upload_no, "E", String.valueOf(i), "", "E004", fileNameArray[i]));
						processErrMsg = "上傳檔名錯誤";
						continue;
					}
				}

				// 檢查檔案名稱 ex: FCX
				if (!checkMaps.get("comm_file_name").containsKey(fileNameParseBean.File_Name)) {
					System.out.println(fileNameArray[i] + " 檔案名稱錯誤");
					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "", "",
							upload_no, "E", String.valueOf(i), "", "E001", fileNameArray[i]));
					processErrMsg = "上傳檔名錯誤";
					continue;
				}

				// 檢查日期格式
				if (ETL_Tool_FormatCheck.isEmpty(fileNameParseBean.getRecord_Date_String())) {
					System.out.println(fileNameArray[i] + " 日期格式錯誤");
					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "", "",
							upload_no, "E", String.valueOf(i), "", "E003", fileNameArray[i]));
					processErrMsg = "上傳檔名錯誤";
					continue;
				} else if (!ETL_Tool_FormatCheck.checkDate(fileNameParseBean.getRecord_Date_String())) {
					System.out.println(fileNameArray[i] + " 日期格式錯誤");
					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(batch_no, exc_central_no, exc_record_date, "", "",
							upload_no, "E", String.valueOf(i), "", "E003", fileNameArray[i]));
					processErrMsg = "上傳檔名錯誤";
					continue;
				}

			}

			errWriter.insert_Error_Log();

			// 檢查有無錯誤
			String detail_exe_result = "";
			String file_exe_result ="";
			if (!"".equals(processErrMsg)) {
				detail_exe_result = "y";
				file_exe_result = "D";
			} else {
				detail_exe_result = "N";
				file_exe_result = "Y";

			}

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", detail_exe_result, processErrMsg, new Date());
			
			// 處理後更新ETL_FILE_Log
			ETL_P_Log.update_End_ETL_FILE_Log(batch_no, exc_central_no, exc_record_date,
					"", "", upload_no, "E", new Date() , 0,
					0, 0, file_exe_result, processErrMsg);

		} catch (Exception ex) {
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());
			
			// 處理後更新ETL_FILE_Log
			ETL_P_Log.update_End_ETL_FILE_Log(batch_no, exc_central_no, exc_record_date,
					"", "", upload_no, "E", new Date() , 0,
					0, 0, "S",  ex.getMessage());
			
			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_Wrong_File - End"); 

	}
	
	// 將檔名解析成物件
	private FileNameParseBean parseFileName(String fileName) {
		String[] fileNameSplit = fileName.split("\\_");
		FileNameParseBean fileNameParseBean = new FileNameParseBean();
		fileNameParseBean.setFileName(fileName);
		fileNameParseBean.setCentral_No(fileNameSplit[0]);

		// 剪頭 去除 FCX CALENDAR 例外....
		if (fileNameSplit.length != 3) {
			fileNameParseBean.setFile_Type(fileNameSplit[1]);
			// 剪頭 //去尾 其他就是檔名 ex:600_CF_GUARANTOR_20179920.txt 擷取字串 頭=600_CF_ 的長度 +2
			// 是為了沒算到_符號 尾= 全部長度-尾巴的長度-1 -1是為了去除最後位的 _符號
			fileNameParseBean
					.setFile_Name(fileName.substring((fileNameSplit[0].length() + fileNameSplit[1].length() + 2),
							fileName.length() - (fileNameSplit[fileNameSplit.length - 1]).length() - 1));
			fileNameParseBean.setRecord_Date_String((fileNameSplit[fileNameSplit.length - 1]).split("\\.")[0]);
		} else {
			fileNameParseBean.setFile_Type(null);
			fileNameParseBean.setFile_Name(fileNameSplit[1]);
			fileNameParseBean.setRecord_Date_String(fileNameSplit[2]);
		}

		return fileNameParseBean;

	}

	class FileNameParseBean {
		// 檔案名
		private String FileName;
		// 報送單位
		private String Central_No;
		// 業務名稱
		private String File_Type;
		// 處理檔名
		private String File_Name;
		// 檔案日期文字
		private String Record_Date_String;

		public String getFileName() {
			return FileName;
		}

		public void setFileName(String fileName) {
			FileName = fileName;
		}

		public String getCentral_No() {
			return Central_No;
		}

		public void setCentral_No(String central_No) {
			Central_No = central_No;
		}

		public String getFile_Type() {
			return File_Type;
		}

		public void setFile_Type(String file_Type) {
			File_Type = file_Type;
		}

		public String getFile_Name() {
			return File_Name;
		}

		public void setFile_Name(String file_Name) {
			File_Name = file_Name;
		}

		public String getRecord_Date_String() {
			return Record_Date_String;
		}

		public void setRecord_Date_String(String record_Date_String) {
			Record_Date_String = record_Date_String;
		}

	}

	public static void main(String[] argv) throws Exception {
		String filePath = "D:\\company\\pershing\\agribank\\test_data\\test";
		ETL_E_Wrong_File checkFile = new ETL_E_Wrong_File();

		checkFile.record_Error_File(filePath, "", "987", "600", new SimpleDateFormat("yyyyMMdd").parse("20180130"), "987",
				"");

	}

}
