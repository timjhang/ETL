import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import Extract.ETL_E_ACCOUNT;
import Extract.ETL_E_COLLATERAL;
import Extract.ETL_E_LOAN;
import Extract.ETL_E_LOAN_DETAIL;
import Extract.ETL_E_PARTY_ADDRESS;
import Extract.ETL_E_TRANSACTION;
import Tool.ETL_Tool_FileByteUtil;

public class Test {
	public static boolean isMatch(byte[] pattern, byte[] input, int pos) {
		for (int i = 0; i < pattern.length; i++) {
			if (pattern[i] != input[pos + i]) {
				return false;
			}
		}
		return true;
	}

	// public static List<byte[]> split(byte[] pattern, byte[] input) {
	// List<byte[]> l = new LinkedList<byte[]>();
	// int blockStart = 0;
	//
	// int count = 100000;
	// int find_num = 0;
	// for (int i = 0; i < input.length; i++) {
	// if (isMatch(pattern, input, i)) {
	// l.add(Arrays.copyOfRange(input, blockStart, i));
	// blockStart = i + pattern.length;
	// i = blockStart;
	// find_num++;
	// }
	// if(find_num == count)
	// break;
	// }
	//// l.add(Arrays.copyOfRange(input, blockStart, input.length));
	// return l;
	// }

	public static List<byte[]> split(byte[] pattern, byte[] input) {
		int a = 0;
		List<byte[]> l = new LinkedList<byte[]>();
		int blockStart = 0;
		for (int i = 0; i < input.length; i++) {
			if (isMatch(pattern, input, i)) {
				a++;
				// l.add(Arrays.copyOfRange(input, blockStart, i));
				// blockStart = i+pattern.length;
				// i = blockStart;
			}
		}
		// l.add(Arrays.copyOfRange(input, blockStart, input.length ));
		System.out.println(a);
		return l;
	}

	public static void readLines(byte[] data) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)));
		// String line;

		for (byte b : data) {
			System.out.print(String.format("%02X", b));
		}

//		byte[] line = null;
//		while ((line = reader.readLine().getBytes("big5")) != null) {
//			for (byte b : line) {
//				System.out.print(String.format("%02X", b));
//			}
//			System.out.println();
//		}
	}

	public static void main(String[] argv) throws IOException {
		List<String> lines = getProperties("C:\\ETL\\file_properties.txt");
		// String file = lines.get(0);
//		 String file ="D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\data\\928_P_TRANSACTION_20180105.TXT";
		 String file ="D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\data\\b.txt";
//		String file = "D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\data\\b.TXT";
//		 String file = "C:\\Users\\Ian\\Desktop\\018\\新文字文件.txt";
//		byte[] data = ETL_Tool_FileByteUtil.toByteArrayUseMappedByte(file);

//		byte[] line = { (byte) 0x0d, (byte) 0x0a };
		long time1, time2;
		time1 = System.currentTimeMillis();
//
//		List<byte[]> split = split(line, data);

		// for (byte[] a : split) {
		// System.out.println(new String(a, "big5"));
		// }


		// System.out.println(split.size());

//		 readLines(data);

		FileInputStream fileInputStream = new FileInputStream(file);
//		InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, Charset.forName("Big5"));
		JBReader bufferedReader = new JBReader(fileInputStream);
		byte[] lineStr = null;
		int i =0;
		while ((lineStr = bufferedReader.readLineInBinary()) != null) {
			i++;
//			for (byte b : lineStr) {
//				System.out.print(String.format("%02X", b));
//			}
//			System.out.println();
		}
		System.out.println(i);
		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");
//		System.out.println(i);
	}

	public static void main1(String[] argv) throws IOException {

		long time1, time2;
		time1 = System.currentTimeMillis();

		TRANSACTION();

		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");
	}

	public static void PARTY_ADDRESS() throws IOException {
		ETL_E_PARTY_ADDRESS program = new ETL_E_PARTY_ADDRESS();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "PARTY_ADDRESS";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Party_Address_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,
					upload_no, program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void ACCOUNT() throws IOException {
		ETL_E_ACCOUNT program = new ETL_E_ACCOUNT();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "ACCOUNT";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Account_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void TRANSACTION() throws IOException {
		ETL_E_TRANSACTION program = new ETL_E_TRANSACTION();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "TRANSACTION";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Transaction_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void LOAN_DETAIL() throws IOException {
		ETL_E_LOAN_DETAIL program = new ETL_E_LOAN_DETAIL();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "LOAN_DETAIL";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Loan_Detail_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void LOAN() throws IOException {
		ETL_E_LOAN program = new ETL_E_LOAN();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "LOAN";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Loan_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void COLLATERAL() throws IOException {
		ETL_E_COLLATERAL program = new ETL_E_COLLATERAL();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "COLLATERAL";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Collateral_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
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
