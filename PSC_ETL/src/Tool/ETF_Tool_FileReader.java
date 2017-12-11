package Tool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ETF_Tool_FileReader {
	
	public static boolean forTest = false;
	
	// 取得目標檔名資料List
	public static List<File> getTargetFileList(String filePath, String fileTypeName) throws Exception {
		
		List<File> tartgetFileList = new ArrayList<File>();
		
		// 讀取路徑, 先檢查相關權限是否ok
		File file = new File(filePath);
		if (!file.exists()) {
//			System.out.println(filePath + " 此路徑不存在! 請確認");
			throw new Exception(filePath + " 此路徑不存在! 請確認");
		} else if (!file.isDirectory()) {
//			System.out.println(filePath + " 並非資料夾路徑! 請確認");
			throw new Exception(filePath + " 並非資料夾路徑! 請確認");
		} else if (!file.canRead()) {
//			System.out.println(filePath + " 此路徑無讀取權限! 請確認");
			throw new Exception(filePath + " 此路徑無讀取權限! 請確認");
		}
		
		// 取得檔名list
		String[] fileNameArray = file.list();
		
		if (forTest) { // test
			System.out.println("所有檔名");
			for (int i = 0; i < fileNameArray.length; i++) {
				System.out.println(fileNameArray[i]);
			}
		}
		
		if (forTest) { // test
			System.out.println("取得目標檔名");
		}
		for (int i = 0; i < fileNameArray.length; i++) {
			// test  暫定簡單的檢核方式, 檔名格式確認後, 可使用更精確的檢核方式
			if (fileNameArray[i].contains(fileTypeName)) {
				
				if (forTest) { // test
					System.out.println(fileNameArray[i]);
				}
				
				File tempFile = new File(filePath + "/" + fileNameArray[i]);
				tartgetFileList.add(tempFile);
			}
		}
		
		// 回傳目標檔案File
		return tartgetFileList;
	}

}
