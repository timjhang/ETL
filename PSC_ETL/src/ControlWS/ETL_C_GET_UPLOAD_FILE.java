package ControlWS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import FTP.ETL_SFTP;
import Tool.ETL_Tool_FileName_Encrypt;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ZIP;

public class ETL_C_GET_UPLOAD_FILE {
	
	public static boolean download_SFTP_Files(String central_no, String[] downloadFileInfo) {
		
		try {
			
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_Files Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
			// 確認是否執行過  BATCH_MASTER_LOG ????
			if (hasRun()) {
				
				System.out.println("#### ETL_C_GET_UPLOAD_FILE 下載排程已執行過!! 不再執行");
				return false;
			}
			
			// 取得需要下載的共用中心  ETL_CENTRAL_INFO
//			List<String> download_Central_No = getUseCentralNo();
			List<String> download_Central_No = new ArrayList<String>();
			download_Central_No.add(central_no);
			
			// 共用中心逐一進行處理
			for (int cen_index = 0; cen_index < download_Central_No.size(); cen_index++) {
				// 待處理共用中心代號
				String central_No = download_Central_No.get(cen_index);
				
				// 搜尋MASTER檔, 取得 List<資料日期|上傳批號 |zip檔名>
//				String[] dataInfo = new String[2];
				List<String> zipFiles = new ArrayList<String>();
				try {
					zipFiles = checkHasMaster(central_No);
				} catch (Exception ex) {
					System.out.println("解析" + central_No + "Master檔出現問題!");
					ex.printStackTrace();
				}
				
				// 若存在則進行開路徑 & 下載 & 解壓縮作業
				if (zipFiles != null) {
					for (int file_index = 0; file_index < zipFiles.size(); file_index++) {
						String[] dataInfo = zipFiles.get(file_index).split("\\|");
						
						// 正常情況只會有一筆, 這邊只取第一筆
						if (file_index == 0) {
							downloadFileInfo[0] = zipFiles.get(file_index);
						}
						
						// 檢核資料日期 + 上傳批號, 是否曾經有跑過  ????
						
						
						File downloadDir = new File(ETL_C_Profile.ETL_Download_localPath + central_No + "/" + dataInfo[0] + "/" + dataInfo[1]);
						
						if (!downloadDir.exists()) {
							downloadDir.mkdirs();
						}
						// for test
//						else {
//							System.out.println(file.getAbsolutePath() + " 已經存在, 請確認是否重複執行。");
//							continue;
//						}
						
						// 下載ZIP檔
						if (downloadZipFile(central_No, dataInfo[2])) {
							System.out.println("下載檔案:" + dataInfo[2] + " 成功!");
						} else {
							System.out.println("下載檔案:" + dataInfo[2] + " 發生錯誤!");
							break;
						}
						
						// 解壓縮檔案到新批號目錄底下
						String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD/" + dataInfo[2];
						String localExtractDir = ETL_C_Profile.ETL_Download_localPath + central_No + "/" + dataInfo[0] + "/" + dataInfo[1] + "/";
						String password = ETL_Tool_FileName_Encrypt.encode(dataInfo[2]);
						if (ETL_Tool_ZIP.extractZipFiles(localDownloadFilePath, localExtractDir, password)) {
							System.out.println("解壓縮檔案:" + dataInfo[2] + " 成功！");
							
							// 紀錄解壓縮成功
							// ????
						} else {
							System.out.println("解壓縮檔案:" + dataInfo[2] + " 失敗！");
							// 紀錄解壓縮失敗
							// ????
						}
						
					}
					
				} else {
					// 若無收到Master檔, 給出訊息
					System.out.println(central_No + " 查無Master檔, 不進行下載。");
				}
				
			}
			
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_Files End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_Files 發生錯誤!! " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			return false;
		}
		
	}
	
	// 確認是否執行過BATCH_MASTER_LOG  ????
	private static boolean hasRun() {
		
		return false;
	}
	
	// 取得有效共用中心代號  ????
	private static List<String> getUseCentralNo() {
		List<String> resultList = new ArrayList<String>();
		resultList.add("600");
//		resultList.add("018");
		
		return resultList;
	}
	
	// 確認是否有對應Master檔
	private static List<String> checkHasMaster(String central_No) throws Exception {
		
		// 結果字串
		List<String> resultList = new ArrayList<String>();
		
		String masterFileName = central_No + "MASTER.txt";
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		boolean hasMaster = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile);
		
		if (!hasMaster) {
			System.out.println("找不到" + remoteMasterFile + "檔案!");
			return null;
		}
		
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD";
		File localDownloadFileDir = new File(localDownloadFilePath);
		if (!localDownloadFileDir.exists()) {
			localDownloadFileDir.mkdir();
		}
		
		String localMasterFile = localDownloadFilePath + "/" + masterFileName;
		
//		System.out.println(localMasterFile); // for test
//		System.out.println(remoteMasterFile); // for test
		
		// download Master檔
		if (ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localMasterFile, remoteMasterFile)) {
			System.out.println("Download: " + remoteMasterFile + " 成功!");
		} else {
			System.out.println("Download: " + remoteMasterFile + " 失敗!");
			return null;
			// throw exception
		}
			
		// 讀取master檔內明細資料, 回傳zip檔list
		File parseFile = new File(localMasterFile);
		FileInputStream fis = new FileInputStream(parseFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
		
		String masterLineStr = "";
		String resultStr = "";
		while (br.ready()) {
			masterLineStr = br.readLine();
			System.out.println(masterLineStr); // for test
			
			resultStr = checkMasterLineString(central_No, masterLineStr);
			if (resultStr == null) {
				throw new Exception("解析" + parseFile.getName() + "解析出現問題!");
			}
			
			resultList.add(resultStr);
		}
		
		return resultList;
	}
	
	// 解析檢核Master File當中String
	private static String checkMasterLineString(String central_No, String input) {
		try {
			String[] strAry =  input.split("\\,");
			if (strAry.length != 2) {
				System.out.println("無法以,分隔"); // for test
				return null;
			}
			
			// 檢核record_Date + upload_No
			if (strAry[0].length() != 11) {
				System.out.println("格式不正確, 前字長度不足。"); // for test
				return null;
			}
			
			// 資料日期
			String record_Date = strAry[0].substring(0, 8);
			// 上傳批號
			String upload_No = strAry[0].substring(8, 11);
			
			// 檢核日期格式
			if (!ETL_Tool_FormatCheck.checkDate(record_Date)) {
				return null;
			}
			
			// 檢核zip檔名
			String zipFileName = "AML_" + central_No + "_" + strAry[0] + ".zip";
			if (!zipFileName.equals(strAry[1])) {
				System.out.println("檔名檢核不通過:" + zipFileName + " - " + strAry[1]); // for test
				return null;
			}
			
			// 回傳  "(資料日期)|(上傳批號)|(zip檔名)"
			return record_Date + "|" + upload_No + "|" + zipFileName;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("發生錯誤"); // for test
			return null;
		}
	}
	
	// 下載zip檔案
	private static boolean downloadZipFile(String central_No, String zipFileName) {
		
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD/";
		String remoteFile = remoteFilePath + zipFileName;
		String localDownloadFile = localDownloadFilePath + zipFileName;
		
		boolean downLoadOK = ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localDownloadFile, remoteFile);
		
		return downLoadOK;
	}

	public static void main(String[] args) {
		
		String[] downloadFileInfo = new String[1];
		download_SFTP_Files("600", downloadFileInfo);
		System.out.println("downloadFileInfo = " + downloadFileInfo[0]);
	}

}
