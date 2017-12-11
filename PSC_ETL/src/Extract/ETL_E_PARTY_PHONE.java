package Extract;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.omg.PortableInterceptor.SUCCESSFUL;

import Bean.ETL_P_ErrorLog_Data;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_Q_ColumnCheckCodes;
import Profile.ETL_Profile;
import Tool.ETF_Tool_FileReader;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;

public class ETL_E_PARTY_PHONE {
	
	// 欄位檢核用陣列
	private String[][] checkMapArray =
		{
			{"ETL_E_PARTY_COLUMN_1", "TimTest"}, 
			{"ETL_E_PARTY_COLUMN_2", "TimTest"},
			{"ETL_E_PARTY_COLUMN_3", "TimTest"}
		};
	
	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;
	
	// class生成時, 取得所有檢核用子map, 置入母map內
	{
		try {
			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_PARTY 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}
	};
	
	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void readPartyFile(String filePath, String fileTypeName) {
		
		System.out.println("#######Extrace - ETL_E_PARTY - Start");
		
		try {
			// test
			filePath = "C:/TEST/ETL_SFTP/ETL600";
			fileTypeName = "PARTY";
			// test
			
			// 取得目標檔案File
			List<File> fileList = ETF_Tool_FileReader.getTargetFileList(filePath, fileTypeName);
			
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
				System.out.println("解析檔案： " + fileName + " Start");
				
				// 解析fileName物件
				ETL_Tool_ParseFileName fileNameData = new ETL_Tool_ParseFileName(fileName);
				
//				System.out.println(parseFile.getAbsoluteFile()); // test
				FileInputStream fis = new FileInputStream(parseFile);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
				
				// rowCount == 處理總計數
				int rowCount = 0;
				// 成功計數
				int successCount = 0;
				// 失敗計數
				int failureCount = 0;
				
				// 嚴重錯誤訊息變數
				String fileFmtErrMsg = ""; 
				// 錯誤欄位紀錄變數
				String errorColumn = "";
				
				String lineStr = ""; // 字串暫存區
				
				// ETL_字串處理Queue
				ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue();
				// ETL_Error Log寫入輔助工具
				ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();
				
				// 首錄檢查
				if (br.ready()) {
					lineStr = br.readLine();
					
					// 注入首錄字串
					strQueue.setTargetString(lineStr);
					
					// 檢查整行bytes數(1 + 7 + 8 + 24 = 40)
					if (strQueue.getTotalByteLength() != 40) {
						fileFmtErrMsg = "首錄位元數非預期";
//						errWriter.addErrLog(new ETL_P_ErrorLog_Data()); test
					}
					
					// 區別瑪(1)
					String typeCode = strQueue.popBytesString(1);
					if (!"1".equals(typeCode)) { // 首錄區別碼有錯 , 嚴重錯誤, 跳出迴圈並記錄錯誤訊息
						fileFmtErrMsg = "首錄區別碼有誤";
					}
					
					// 報送單位(7)
					String central_no = strQueue.popBytesString(7);
					
					
					// 檔案日期(8)
					String record_date = strQueue.popBytesString(8);
					
					// 保留欄位(24)
					String keepColumn = strQueue.popBytesString(24);
					
					if (!"".equals(fileFmtErrMsg)) {
						// 錯誤寫入  test
						
						failureCount++; // 錯誤計數 + 1
						rowCount++; // 處理總計數  + 1
					} else {
						successCount++; // 成功計數 + 1
						rowCount++; // 處理總計數 + 1
					}
				}
				
				// 逐行讀取檔案
				while (br.ready() && "".equals(fileFmtErrMsg)) {
					
					lineStr = br.readLine();
					System.out.println(lineStr);
					
					rowCount++; // rowCount + 1
				}
				fis.close();

				System.out.println("解析檔案： " + fileName + " End");
			}
		
		} catch (Exception ex) {
			
			ex.printStackTrace();
		}
		
		System.out.println("#######Extrace - ETL_E_PARTY - End");
	}
	
	// 資料寫進DB2(做一個工具) test
	
	// Error Log寫進DB2(做一個工具) test
	
	// ETL Log寫進DB2
	
	
	public static void main(String[] argv) {
//		System.out.println("start");
		ETL_E_PARTY_PHONE one = new ETL_E_PARTY_PHONE();
//		one.readPartyFile(null, null);
//		System.out.println("end");
		
//		System.out.println("checkMaps size = " + one.checkMaps.size());
	}
	
}
