package Tool;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class CharsetUtils {
//	private static final Logger logger = LogManager.getLogger(CharsetUtils.class);

	public static void main(String[] argv) throws Exception {
		Path path = Paths.get("D:/PSC/Projects/AgriBank/UNIT_TEST", "600_CF_PARTY_20180327.TXT");
		byte[] array = Files.readAllBytes(path);
		System.out.println(byteArrayToHexStr(array));
		System.out.println(convertHexToString("31"));
	}
	
	/**
	 * 把byte array 轉換成16進位字串
	 * @param src 來源陣列
	 * @return 16進位字串
	 */
	public static String byteArrayToHexStr(byte[] byteArray) {
		if (byteArray == null) {
			return null;
		}
		char[] hexArray = "0123456789ABCDEF".toCharArray();
		char[] hexChars = new char[byteArray.length * 2];
		for (int j = 0; j < byteArray.length; j++) {
			int v = byteArray[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	/**
	 * 轉換16進位為10進位字串
	 * @param hex 來源字串
	 * @return 10進位字串
	 */
	public static String convertHexToString(String hex) {

		StringBuilder sb = new StringBuilder();
		StringBuilder temp = new StringBuilder();

		// 49204c6f7665204a617661 split into two characters 49, 20, 4c...
		for (int i = 0; i < hex.length() - 1; i += 2) {

			// grab the hex in pairs
			String output = hex.substring(i, (i + 2));
			// convert hex to decimal
			int decimal = Integer.parseInt(output, 16);
			// convert the decimal to character
			sb.append((char) decimal);

			temp.append(decimal);
		}

		return sb.toString();
	}

	public static byte[] hexStrToByteArray(String str) {
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

	/**
	 * 把byte array 轉換成16進位字串
	 * @param src 來源陣列
	 * @return 16進位字串
	 */
	public static String bytesToHexString(byte[] src) {
		StringBuilder stringBuilder = new StringBuilder("");
		if (src == null || src.length <= 0) {
			return null;
		}
		for (int i = 0; i < src.length; i++) {
			int v = src[i] & 0xFF;
			String hv = Integer.toHexString(v);
			if (hv.length() < 2) {
				stringBuilder.append(0);
			}
			stringBuilder.append(hv.toUpperCase());
		}
		return stringBuilder.toString();
	}

	/**
	 * 判斷檔案編碼
	 * @param path 檔案路徑
	 * @return 編碼格式
	 * @throws IOException
	 */
	public static String easyDetectCharset(String path) throws IOException {
		BufferedInputStream bStream = new BufferedInputStream(new FileInputStream(path));
		String charset = null;
		int key = (bStream.read() << 8) + bStream.read();
		switch (key) {
		case 0xefbb:// 要有bom
			charset = "UTF-8";
			break;
		case 0xfffe:
			charset = "Unicode";
			break;
		case 0xfeff:
			charset = "UTF-16BE";
			break;
		default:
			charset = "BIG5";
			break;
		}
		return charset;

	}
	
	/**
	 * 判斷字碼是否存在於Big5造字區中
	 * @param code Big5字碼
	 * @return trur 存在 / false 不存在
	 */
	/*
	 * 造字範圍		字數
	 * FA40-FEFE	785
	 * 8E40-A0FE	2983	
	 * 8140-8DFE	2041
	 */
	public static boolean  isBig5DifficultWord(String code){
		boolean is = false;
		
		try {
			int decimal =Integer.parseInt(code, 16 );
			
			int range_1_start =Integer.parseInt("FA40", 16 );
			int range_1_end =Integer.parseInt("FEFE", 16 );
			
			int range_2_start =Integer.parseInt("8E40", 16 );
			int range_2_end =Integer.parseInt("A0FE", 16 );
			
			int range_3_start =Integer.parseInt("8140", 16 );
			int range_3_end =Integer.parseInt("8DFE", 16 );
			
			if(((range_1_start <= decimal) && (decimal <= range_1_end))|
					((range_2_start <= decimal) && (decimal <= range_2_end))|
					((range_3_start <= decimal) && (decimal <= range_3_end)))
				is = true;
		} catch (Exception e) {
			return is;
		}
		
		return is;
	}
	
	/**
	 * 轉換檔案為Byte Array
	 * @param path 檔案路徑
	 * @return 檔案轉換後的byte array
	 * @throws IOException
	 */
	public static byte[] fileToBytes(String path) throws IOException{
		return Files.readAllBytes(Paths.get(path));
	}
	
	/**
	 * 判斷檔案編碼
	 * 
	 * @param path 檔案路徑
	 * @return 回傳編碼類型
	 */
	public static Charset detectCharset(String path) {
		File file = new File(path);
		Charset charset = null;

		String[] charsets = { "UTF-8", "big5" };

		for (String charsetName : charsets) {
			charset = detectCharset(file, Charset.forName(charsetName));
			if (charset != null) {
				break;
			}
		}
		return charset;
	}

	/**
	 * 判斷檔案編碼-實作
	 * 
	 * @param f 要進行檢測的File物件
	 * @param charset 進行比對測試的編碼
	 * @return 編碼型態，如不符合，則會回傳null
	 */
	private static Charset detectCharset(File f, Charset charset) {
		try {
			BufferedInputStream input = new BufferedInputStream(new FileInputStream(f));

			CharsetDecoder decoder = charset.newDecoder();
			decoder.reset();

			byte[] buffer = new byte[512];
			boolean identified = false;
			while ((input.read(buffer) != -1) && (!identified)) {
				identified = identify(buffer, decoder);
			}

			input.close();

			if (identified) {
				return charset;
			} else {
				return null;
			}

		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * 字符解碼器，依照解碼器，把資料來源進行轉換。
	 * 
	 * @param bytes 資料來源
	 * @param decoder 對應的解碼器
	 * @return trur 符合的解碼器 / false 不符合的解碼器
	 */
	private static boolean identify(byte[] bytes, CharsetDecoder decoder) {
		try {
			decoder.decode(ByteBuffer.wrap(bytes));
		} catch (CharacterCodingException e) {
			return false;
		}
		return true;
	}

	/**
	 * 用指定的編碼格式讀取檔案
	 * 
	 * @param path 被讀取的檔案之路徑
	 * @param encoding 指定的編碼格式
	 * @return 檔案內容
	 * @throws IOException
	 */
	public static String readFileEencod(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	/**
	 * 取兩位數的16進位
	 * 
	 * @param bytes 資料來源
	 * @return 兩位數的16進位，不足兩位則補0
	 */
	private static String bytes2Hex(byte[] bytes) {
		String str = "";
		for (byte b : bytes) {
			str += String.format("%02X", b);
		}
		return str;
	}

	/**
	 * 打印來源字串的Big5、utf8、unicode編碼
	 * 
	 * @param str 來源字串
	 * @throws UnsupportedEncodingException
	 */
	public static void checkCode(String str) throws UnsupportedEncodingException {
		byte[] big5Str = str.getBytes("Big5");
		byte[] utf8Str = str.getBytes("UTF-8");
		byte[] unicodeStr = str.getBytes("Unicode");
		byte[] deftStr = str.getBytes();

		// FEFF: Big-Endian
		// FFFE: Little- Endian
//		logger.debug("\n[內容]：{}\n[編碼-Big5]: {}\n[編碼-UTF-8 (hex)]: {}\n[編碼-Unicode (Big-Endian)]: {}\n[編碼-default]: {}",
//				str, bytes2Hex(big5Str), bytes2Hex(utf8Str), bytes2Hex(unicodeStr), bytes2Hex(deftStr));
	}

//  改用Excel方式，棄用該方法
//	/**
//	 * 拿到Big自訂字碼與Unicode系統字碼的Map
//	 * 
//	 * @param path 來源資料檔案路徑
//	 * @return key是Big5自訂字碼，Value是Unicode系統字的Map
//	 * @throws IOException
//	 */
//	public static Map<String, String> get_Big5_Provision_And_Unicode_System_Map(String path, String encoding)
//			throws IOException {
//		Map<String, String> map = new HashMap<String, String>();
//		byte[] encoded = Files.readAllBytes(Paths.get(path));
//		String content = new String(encoded, encoding);
//
//		String[] group = content.split(",");
//
//		for (int i = 0; i < group.length; i++) {
//			String[] detail = group[i].split("\\|", -1);
//			for (int j = 0; j < detail.length; j++) {
//
//				String big5_provision = detail[0];
//
//				String unicode_system = detail[2];
//				map.put(big5_provision, unicode_system);
//			}
//		}
//		return map;
//	}

//  改用Excel方式，棄用該方法
//	/**
//	 * 拿到Big自訂字碼與Unicode自訂字碼的Map
//	 * 
//	 * @param path 來源資料檔案路徑
//	 * @return key是Big5自訂字碼，Value是Unicode自訂字碼的Map
//	 * @throws IOException
//	 */
//	public static Map<String, String> get_Big5_Provision_And_Unicode_Provision_Map(String path, String encoding)
//			throws IOException {
//		Map<String, String> map = new HashMap<String, String>();
//		byte[] encoded = Files.readAllBytes(Paths.get(path));
//		String content = new String(encoded, encoding);
//
//		String[] group = content.split(",");
//
//		for (int i = 0; i < group.length; i++) {
//			String[] detail = group[i].split("\\|", -1);
//			for (int j = 0; j < detail.length; j++) {
//
//				String big5_provision = detail[0];
//
//				String unicode_provision = detail[1];
//				map.put(big5_provision, unicode_provision);
//			}
//		}
//		return map;
//	}

	/**
	 * 採用默認編碼方式讀取檔案Byte
	 * 
	 * @param path 資料來源檔案路徑
	 * @return byte[]型態的檔案內容
	 * @throws Exception
	 */
	public static byte[] readFileBytes(String path) throws Exception {
		InputStream targetStream = new FileInputStream(path);

		return toBytes(targetStream);
	}

	/**
	 * 輸入串流轉換為byte[]
	 * 
	 * @param input 來源輸入串流
	 * @return 轉換後的byte[]
	 * @throws Exception
	 */
	public static byte[] toBytes(InputStream input) throws Exception {
		byte[] data = null;
		try {
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			byte[] b = new byte[1024];
			int read = 0;
			while ((read = input.read(b)) > 0) {
				byteOut.write(b, 0, read);
			}
			data = byteOut.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			input.close();
		}
		return data;
	}
}
