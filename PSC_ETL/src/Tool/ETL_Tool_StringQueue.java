package Tool;

import java.io.UnsupportedEncodingException;

public class ETL_Tool_StringQueue {
	// 截字輔助工具截byte(預設轉BIG5)
	
	// 轉換編碼格式
	private static final String format = "BIG5";
//	private final String format = "UTF-8";
	
	// 字串總長度
	private int totalLength = 0;
	// 字串
	private String targetString;
	// 已pop長度
	private int popOutLength = 0;
	
	// 字串總長度(byte)
	private int totalByteLength = 0;
	// 字串(byte)
	private byte[] targetStringBytes;
	// 已pop Bytes長度
	private int popOutBytesLength = 0;
	

	public String getTargetString() {
		return targetString;
	}
	
	// 取得字串Byte長度
	public int getTotalByteLength() {
		return totalByteLength;
	}

	// 注入字串, 並設定相關參數
	// 重新set後, 完全更新Queue
	public void setTargetString(String targetString) throws UnsupportedEncodingException {
		this.totalLength = targetString.length();
		this.targetString = targetString;
		this.popOutLength = 0;
		
		this.totalByteLength = targetString.getBytes(format).length;
		this.targetStringBytes = targetString.getBytes(format);
		this.popOutBytesLength = 0;
	}
	
	// pop出擷取字串(字串長度切)
	public String popString(int popLength) throws Exception {
		if (popLength < 0) {
			throw new Exception("ETL_Tool_TokenString - popString - 輸入參數須為正！");
		}
		
		if (popOutLength >= totalLength) {
			throw new Exception("ETL_Tool_TokenString - popString - 字串已pop完畢!");
		}
		
		if (popOutLength + popLength > totalLength) {
			throw new Exception("ETL_Tool_TokenString - popString - 超出pop範圍!");
		}
		
		// pop起始位置
		int tokenStartIndex = popOutLength;
		// 累加已pop次數
		popOutLength = popOutLength + popLength;
		
		// 回傳擷取String
		return targetString.substring(tokenStartIndex, popOutLength);
		
	}
	
	// pop出擷取bytes(bytes長度切)
	public byte[] popBytes(int popLength) throws Exception {
		if (popLength < 0) {
			throw new Exception("ETL_Tool_TokenString - popBytes - 輸入參數須為正！");
		}
		
		if (popOutBytesLength >= totalByteLength) {
			throw new Exception("ETL_Tool_TokenString - popBytes - 字串已pop完畢!");
		}
		
		if (popOutBytesLength + popLength > totalByteLength) {
			throw new Exception("ETL_Tool_TokenString - popBytes - 超出pop範圍!");
		}
		
		// 結果array
		byte[] resultBytes = new byte[popLength];
		// pop起始位置
		int tokenStartIndex = popOutBytesLength;
		// 累加已pop次數
		popOutBytesLength = popOutBytesLength + popLength;
		
		// 複製所需長度array至resultBytes上
		System.arraycopy(targetStringBytes, tokenStartIndex, resultBytes, 0, popLength);
		
		// 回傳結果array
		return resultBytes;
	}
	
	// pop出擷取字串(bytes長度切)
	public String popBytesString(int popLength) throws Exception {
		if (popLength < 0) {
//			throw new Exception("ETL_Tool_TokenString - popBytes - 輸入參數須為正！");
			System.out.println("ETL_Tool_TokenString - popBytes - 輸入參數須為正！");
		}
		
		if (popOutBytesLength >= totalByteLength) {
//			throw new Exception("ETL_Tool_TokenString - popBytes - 字串已pop完畢!");
			System.out.println("ETL_Tool_TokenString - popBytes - 字串已pop完畢!");
		}
		
		if (popOutBytesLength + popLength > totalByteLength) {
//			throw new Exception("ETL_Tool_TokenString - popBytes - 超出pop範圍!");
			System.out.println("ETL_Tool_TokenString - popBytes - 超出pop範圍!");
		}
		
		// 結果array
		byte[] resultBytes = new byte[popLength];
		// pop起始位置
		int tokenStartIndex = popOutBytesLength;
		// 累加已pop次數
		popOutBytesLength = popOutBytesLength + popLength;
		
		// 複製所需長度array至resultBytes上
		System.arraycopy(targetStringBytes, tokenStartIndex, resultBytes, 0, popLength);
		
		// 回傳結果array
		return new String(resultBytes, format);
	}
	
	// test
	public static void main(String[] argv) {
		try {
			ETL_Tool_StringQueue one = new ETL_Tool_StringQueue();
			String temp = "臣亮言：先帝創業未半，而中道崩殂。今天下三分，益";
			one.setTargetString(temp);
			System.out.println(one.popString(1));
			System.out.println(one.popString(2));
			System.out.println(one.popString(10));
			System.out.println(one.popString(temp.length() - (1 + 2 + 10)));
			
			System.out.println(new String(one.popBytes(3), one.format));
			System.out.println(one.popBytesString(3));
			
			temp = "123牽著手456抬起頭";
			one.setTargetString(temp);
			System.out.println(one.getTotalByteLength());
			System.out.println(one.popBytesString(9));
			System.out.println(one.getTotalByteLength());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
