package Tool;

import java.io.IOException;
import java.io.RandomAccessFile;
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

public class ETL_Tool_FileByteUtil {

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

	public static List<byte[]> getFilesBytes(String path) throws IOException {
		List<byte[]> list = new ArrayList<byte[]>();

		byte[] bytes = toByteArrayUseMappedByte(path);
		System.out.println("bytes length : " + bytes.length);

		BigDecimal size = new BigDecimal(bytes.length);
		System.out.println("size : " + size.intValue());
		
		BigDecimal divisor = new BigDecimal(1 * 1024 * 1024 * 1024);
		System.out.println("divisor : " + divisor.intValue());
		
		BigDecimal split_length_buffer = size.divide(divisor, 0, RoundingMode.HALF_EVEN);
		System.out.println("split_length_buffer : " + split_length_buffer.intValue());
		
		BigDecimal array_length_buffer = size.divide(split_length_buffer, 0, RoundingMode.HALF_EVEN);
		System.out.println("array_length_buffer : " + array_length_buffer.intValue());

		StringBuffer hexStr = new StringBuffer(1 * 1024 * 1024 * 1024);

		int split_length = split_length_buffer.intValue();
		int array_length = array_length_buffer.intValue();
		byte[][] result = new byte[array_length][];

		int from, to;

		for (int i = 0; i < array_length; i++) {

			from = (int) (i * split_length);
			to = (int) (from + split_length);

			if (to > bytes.length)
				to = bytes.length;

			result[i] = Arrays.copyOfRange(bytes, from, to);
		}
		for (int i = 0; i < result.length; i++) {
			for (int j = 0; j < result[i].length; j++) {
				hexStr.append(String.format("%02X", result[i][j]));
			}
		}

		// for (byte b : bytes) {
		// hexStr.append(String.format("%02X", b));
		// // hexStr += (String.format("%02X", b));
		// }
		String[] linesHexStr = hexStr.toString().split("0D0A");

		for (String s : linesHexStr) {
			list.add(hexStrToByteArray(s));
		}
		return list;
	}
	// public static List<byte[]> getFilesBytes(String path) throws IOException
	// {
	// List<byte[]> list = new ArrayList<byte[]>();
	//
	// byte[] bytes = Files.readAllBytes(Paths.get(path));
	//
	// // BigDecimal size = new BigDecimal(bytes.length);
	// //
	// // BigDecimal divisor = new BigDecimal(10);
	// //
	// // BigDecimal buffer = size.divide(divisor, 0, RoundingMode.HALF_EVEN);
	// //
	// // BigDecimal count = size.divide(buffer, 0, RoundingMode.HALF_EVEN);
	// //
	// // int copies = count.intValue();
	// //
	// // byte[][] split_bytes = split_bytes(bytes, 50000000) ;
	// //
	// // String hexStr = "";
	// // for (int i = 0; i < split_bytes.length; i++) {
	// //
	// // StringBuffer tmp = new StringBuffer(Integer.MAX_VALUE);
	// //
	// // for (int j = 0; j < split_bytes[i].length; j++) {
	// // byte now = split_bytes[i][j] ;
	// //
	// // tmp.append(String.format("%02X", now));
	// // }
	// //
	// // hexStr += tmp.toString();
	// // }
	//
	// StringBuffer hexStr = new StringBuffer(bytes.length * 2);
	// // String hexStr = "";
	//
	// for (byte b : bytes) {
	// hexStr.append(String.format("%02X", b));
	// // hexStr += (String.format("%02X", b));
	// }
	// String[] linesHexStr = hexStr.toString().split("0D0A");
	//
	// for (String s : linesHexStr) {
	// list.add(hexStrToByteArray(s));
	// }
	// return list;
	// }

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
		getFilesBytes(path);
		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");
	}
}