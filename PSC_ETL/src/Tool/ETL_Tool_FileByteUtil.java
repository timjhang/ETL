package Tool;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ibm.db2.jcc.am.l;

import Profile.ETL_Profile;

public class ETL_Tool_FileByteUtil {
	private FileInputStream fileInputStream = null;

	private List<byte[]> lineList = null;
	private int buffer_size = 0;
	private ETL_Tool_JBReader bufferedReader = null;

	public ETL_Tool_FileByteUtil() {
	}

	public ETL_Tool_FileByteUtil(String path, Class<?> clazz) throws FileNotFoundException {
		this.fileInputStream = new FileInputStream(path);
		this.bufferedReader = new ETL_Tool_JBReader(fileInputStream);

		String theme = clazz.getSimpleName();

		switch (theme) {
		case "ETL_E_PARTY":
			this.buffer_size = ETL_Profile.ETL_E_PARTY;
			break;
		case "ETL_E_PARTY_PARTY_REL":
			this.buffer_size = ETL_Profile.ETL_E_PARTY_PARTY_REL;
			break;
		case "ETL_E_PARTY_PHONE":
			this.buffer_size = ETL_Profile.ETL_E_PARTY_PHONE;
			break;
		case "ETL_E_PARTY_ADDRESS":
			this.buffer_size = ETL_Profile.ETL_E_PARTY_ADDRESS;
			break;
		case "ETL_E_ACCOUNT":
			this.buffer_size = ETL_Profile.ETL_E_ACCOUNT;
			break;
		case "ETL_E_TRANSACTION":
			this.buffer_size = ETL_Profile.ETL_E_TRANSACTION;
			break;
		case "ETL_E_LOAN_DETAIL":
			this.buffer_size = ETL_Profile.ETL_E_LOAN_DETAIL;
			break;
		case "ETL_E_LOAN":
			this.buffer_size = ETL_Profile.ETL_E_LOAN;
			break;
		case "ETL_E_COLLATERAL":
			this.buffer_size = ETL_Profile.ETL_E_COLLATERAL;
			break;
		case "ETL_E_GUARANTOR":
			this.buffer_size = ETL_Profile.ETL_E_GUARANTOR;
			break;
		case "ETL_E_FX_RATE":
			this.buffer_size = ETL_Profile.ETL_E_FX_RATE;
			break;
		case "ETL_E_SERVICE":
			this.buffer_size = ETL_Profile.ETL_E_SERVICE;
			break;
		case "ETL_E_TRANSFER":
			this.buffer_size = ETL_Profile.ETL_E_TRANSFER;
			break;
		case "ETL_E_FCX":
			this.buffer_size = ETL_Profile.ETL_E_FCX;
			break;
		case "ETL_E_CALENDAR":
			this.buffer_size = ETL_Profile.ETL_E_CALENDAR;
			break;
		}
	}

	public static byte[] toByteArrayUseMappedByte(String filename) throws IOException {

		FileChannel fc = null;
		try {
			fc = new RandomAccessFile(filename, "r").getChannel();
			MappedByteBuffer byteBuffer = fc.map(MapMode.READ_ONLY, 0, fc.size()).load();
			byte[] result = new byte[(int) fc.size()];
			if (byteBuffer.remaining() > 0) {
				byteBuffer.get(result, 0, byteBuffer.remaining());
			}
			return result;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				fc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public List<byte[]> getFilesBytes() throws IOException {

		this.lineList = new ArrayList<byte[]>();

		for (int i = 0; i < ETL_Profile.ETL_E_Stage; i++) {
			byte[] line = bufferedReader.readLineInBinary();

			if (line != null)
				lineList.add(line);

		}
		return lineList;
	}

	public List<byte[]> getFilesByteForOneRow() throws IOException {

		this.lineList = new ArrayList<byte[]>();
		
		byte[] line = bufferedReader.readLineInBinary();
		if (line != null)
			lineList.add(line);
		
		return lineList;
	}
	
//	public boolean isFileOK(String path) throws IOException {
//		boolean isFileOK = false;
//		List<Byte> list = new ArrayList<Byte>();
//
//		 FileInputStream fileInputStream = new FileInputStream(path);
//		ETL_Tool_JBReader bufferedReader = new ETL_Tool_JBReader(fileInputStream, buffer_size);
//
//		byte[] line = null;
//		byte head = (byte) 49;
//		byte body = (byte) 50;
//		byte foot = (byte) 51;
//		
//		while ((line = bufferedReader.readLineInBinary()) != null) {
//			list.add(line[0]);
//		}
//
//		if (list.size() < 2) {
//			return isFileOK;
//		}
//
//		for (int i = 0; i < list.size(); i++) {
//			byte now = list.get(i);
//			if (i != 0 && !isFileOK)
//				break;
//			if (i == 0) {
//				isFileOK = head == now ? true : false;
//				if (!isFileOK)
//					break;
//			} else if (i != (list.size() - 1)) {
//				isFileOK = body == now ? true : false;
//			} else {
//				isFileOK = foot == now ? true : false;
//			}
//
//		}
//		return isFileOK;
//	}
	
	public int isFileOK(String path) throws IOException {
		int isFileOK = 0;
		List<Byte> list = new ArrayList<Byte>();

		 FileInputStream fileInputStream = new FileInputStream(path);
		ETL_Tool_JBReader bufferedReader = new ETL_Tool_JBReader(fileInputStream, buffer_size);

		byte[] line = null;
		byte head = (byte) 49;
		byte body = (byte) 50;
		byte foot = (byte) 51;
		
		while ((line = bufferedReader.readLineInBinary()) != null) {
			list.add(line[0]);
		}

		if (list.size() < 2) {
			return isFileOK;
		}

		for (int i = 0; i < list.size(); i++) {
			byte now = list.get(i);
			if (i != 0 && isFileOK == 0)
				break;
			if (i == 0) {
				isFileOK = head == now ? 1 : 0;
				if (isFileOK == 0)
					break;
			} else if (i != (list.size() - 1)) {
				isFileOK = body == now ? 1 : 0;
			} else {
				isFileOK = foot == now ? 1 : 0;
			}

		}
		return isFileOK == 1 ? list.size() : 0;
	}

//	public static boolean getFilesBytes(String file_path, Class<?> clazz) throws IOException {
//		List<byte[]> list = new ArrayList<byte[]>();
//		int count = 0;
//
//		FileInputStream fileInputStream = new FileInputStream(file_path);
//		ETL_Tool_JBReader bufferedReader = new ETL_Tool_JBReader(fileInputStream);
//
//		byte[] line = null;
//		while ((line = bufferedReader.readLineInBinary()) != null) {
//			count++;
//		}
//		System.out.println(count);
//
//		String theme = clazz.getName();
//		int buffer_size = 0;
//
//		switch (theme) {
//		case "ETL_E_PARTY":
//			buffer_size = ETL_Profile.ETL_E_PARTY;
//			break;
//		case "ETL_E_PARTY_PARTY_REL":
//			buffer_size = ETL_Profile.ETL_E_PARTY_PARTY_REL;
//			break;
//		case "ETL_E_PARTY_PHONE":
//			buffer_size = ETL_Profile.ETL_E_PARTY_PHONE;
//			break;
//		case "ETL_E_PARTY_ADDRESS":
//			buffer_size = ETL_Profile.ETL_E_PARTY_ADDRESS;
//			break;
//		case "ETL_E_ACCOUNT":
//			buffer_size = ETL_Profile.ETL_E_ACCOUNT;
//			break;
//		case "ETL_E_TRANSACTION":
//			buffer_size = ETL_Profile.ETL_E_TRANSACTION;
//			break;
//		case "ETL_E_LOAN_DETAIL":
//			buffer_size = ETL_Profile.ETL_E_LOAN_DETAIL;
//			break;
//		case "ETL_E_LOAN":
//			buffer_size = ETL_Profile.ETL_E_LOAN;
//			break;
//		case "ETL_E_COLLATERAL":
//			buffer_size = ETL_Profile.ETL_E_COLLATERAL;
//			break;
//		case "ETL_E_GUARANTOR":
//			buffer_size = ETL_Profile.ETL_E_GUARANTOR;
//			break;
//		case "ETL_E_FX_RATE":
//			buffer_size = ETL_Profile.ETL_E_FX_RATE;
//			break;
//		case "ETL_E_SERVICE":
//			buffer_size = ETL_Profile.ETL_E_SERVICE;
//			break;
//		case "ETL_E_TRANSFER":
//			buffer_size = ETL_Profile.ETL_E_TRANSFER;
//			break;
//		case "ETL_E_FCX":
//			buffer_size = ETL_Profile.ETL_E_FCX;
//			break;
//		case "ETL_E_CALENDAR":
//			buffer_size = ETL_Profile.ETL_E_CALENDAR;
//			break;
//		}
//
//		return true;
//	}
//
//	public static List<byte[]> getFilesBytesV2(String path) throws IOException {
//		List<byte[]> list = new ArrayList<byte[]>();
//
//		byte[] bytes = toByteArrayUseMappedByte(path);
//		System.out.println("bytes length : " + bytes.length);
//
//		BigDecimal size = new BigDecimal(bytes.length);
//		System.out.println("size : " + size.intValue());
//
//		BigDecimal divisor = new BigDecimal(1 * 1024 * 1024 * 1024);
//		System.out.println("divisor : " + divisor.intValue());
//
//		BigDecimal split_length_buffer = size.divide(divisor, 0, RoundingMode.HALF_EVEN);
//		System.out.println("split_length_buffer : " + split_length_buffer.intValue());
//
//		BigDecimal array_length_buffer = size.divide(split_length_buffer, 0, RoundingMode.HALF_EVEN);
//		System.out.println("array_length_buffer : " + array_length_buffer.intValue());
//
//		StringBuffer hexStr = new StringBuffer(1 * 1024 * 1024 * 1024);
//
//		int split_length = split_length_buffer.intValue();
//		int array_length = array_length_buffer.intValue();
//		byte[][] result = new byte[array_length][];
//
//		int from, to;
//
//		for (int i = 0; i < array_length; i++) {
//
//			from = (int) (i * split_length);
//			to = (int) (from + split_length);
//
//			if (to > bytes.length)
//				to = bytes.length;
//
//			result[i] = Arrays.copyOfRange(bytes, from, to);
//		}
//		for (int i = 0; i < result.length; i++) {
//			for (int j = 0; j < result[i].length; j++) {
//				hexStr.append(String.format("%02X", result[i][j]));
//			}
//		}
//
//		// for (byte b : bytes) {
//		// hexStr.append(String.format("%02X", b));
//		// // hexStr += (String.format("%02X", b));
//		// }
//		String[] linesHexStr = hexStr.toString().split("0D0A");
//
//		for (String s : linesHexStr) {
//			list.add(hexStrToByteArray(s));
//		}
//		return list;
//	}
//
//	public static List<byte[]> getFilesBytesV1(String path) throws IOException {
//		List<byte[]> list = new ArrayList<byte[]>();
//
//		byte[] bytes = Files.readAllBytes(Paths.get(path));
//
//		StringBuffer hexStr = new StringBuffer(bytes.length * 2);
//
//		for (byte b : bytes) {
//			hexStr.append(String.format("%02X", b));
//		}
//		String[] linesHexStr = hexStr.toString().split("0D0A");
//
//		for (String s : linesHexStr) {
//			list.add(hexStrToByteArray(s));
//		}
//		return list;
//	}

	private static byte[][] split_bytes(byte[] bytes, int copies) {

		double split_length = Double.parseDouble(copies + "");

		int array_length = (int) Math.ceil(bytes.length / split_length);
		// int array_length = 231939126;
		System.out.println("array_length:" + array_length);
		byte[][] result = new byte[array_length][];

		int from, to;

		for (int i = 0; i < array_length; i++) {

			from = (int) (i * split_length);
			to = (int) (from + split_length);

			if (to > bytes.length)
				to = bytes.length;

			result[i] = Arrays.copyOfRange(bytes, from, to);
		}

		return result;
	}

	private static byte[] hexStrToByteArray(String str) {
		if (str == null) {
			return null;
		}
		if (str.length() == 0) {
			return new byte[0];
		}
		byte[] byteArray = new byte[str.length() / 2];
		for (int i = 0; i < byteArray.length; i++) {
			String subStr = str.substring(2 * i, 2 * i + 2);
			byteArray[i] = ((byte) Integer.parseInt(subStr, 16));
		}
		return byteArray;
	}

	public void main(String[] args) throws Exception {
		String path = "C:\\Users\\Ian\\Desktop\\018\\018_CALENDAR_20180116.TXT";

		long time1, time2;
		time1 = System.currentTimeMillis();
		// getFilesBytes(path);
		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");
	}
}