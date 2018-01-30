import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import Extract.ETL_E_PARTY;
import Extract.ETL_E_PARTY_PARTY_REL;
import Extract.ETL_E_PARTY_PHONE;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;

public class Test {

	public static void main(String[] argv) throws Exception {
//		PARTY();
//		PARTY_PHONE();
//		PARTY_PARTY_REL();
		
//		List<String> lines = getProperties("C:\\ETL\\properties.txt");
////		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
//		
//		List<File> fileList = ETL_Tool_FileReader.getTargetFileList(lines.get(0), "PARTY_ADDRESS");
//		
//		System.out.println("共有檔案 " + fileList.size() + " 個！");
//		System.out.println("===============");
//		for (int i = 0; i < fileList.size(); i++) {
//			System.out.println(fileList.get(i).getName());
//		}
//		System.out.println("===============");
//		
//		for (int i = 0 ; i < fileList.size(); i++) {
//			// 取得檔案
//			File parseFile = fileList.get(i);
//			
//			// 檔名
//			String fileName = parseFile.getName();
//			
//			// 解析fileName物件
//			ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName);
//			
//			FileInputStream fis = new FileInputStream(parseFile);
//			BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
//			
//			// ETL_字串處理Queue
////			ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue();
//			
////			String lineStr = "";
//			int count = 1;
//			while (br.ready()) {
////				br.readLine().getBytes().length;
////				System.out.println(lineStr); // test
////				strQueue.setTargetString(lineStr); // queue裝入新String
//				System.out.println("i = " + count + ", bytes = " + br.readLine().getBytes().length);
//				count++;
//			}
//		}
	}

	public static void PARTY() throws IOException {
		ETL_E_PARTY program = new ETL_E_PARTY();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = new Date();

		try {
			filePath = lines.get(0);
			fileTypeName = "PARTY";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			String str_exc_record_date = lines.get(3);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(str_exc_record_date);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Party_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,
					upload_no, program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void PARTY_PHONE() throws IOException {
		ETL_E_PARTY_PHONE program = new ETL_E_PARTY_PHONE();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = new Date();

		try {
			filePath = lines.get(0);
			fileTypeName = "PARTY_PHONE";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			String str_exc_record_date = lines.get(3);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(str_exc_record_date);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Party_Phone_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,
					upload_no, program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void PARTY_PARTY_REL() throws IOException {
		ETL_E_PARTY_PARTY_REL program = new ETL_E_PARTY_PARTY_REL();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = new Date();

		try {
			filePath = lines.get(0);
			fileTypeName = "PARTY_PARTY_REL";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			String str_exc_record_date = lines.get(3);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(str_exc_record_date);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Party_Party_Rel_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,
					upload_no, program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<String> getProperties(String path) throws IOException {
		Charset charset = Charset.forName("Big5");
		List<String> lines = Files.readAllLines(Paths.get(path), charset);
		return lines;
	}
}
