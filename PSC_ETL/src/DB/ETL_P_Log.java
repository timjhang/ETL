package DB;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import Profile.ETL_Profile;

public class ETL_P_Log {

	public static void main(String[] args) throws Exception {
		String CENTRAL_NO = "951";
		DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		java.util.Date date = new java.util.Date();
		java.util.Date RECORD_DATE = formatter.parse(formatter.format(date));
		String FILE_TYPE = "CF";
		String FILE_NAME = "PARTY";
		String UPLOAD_NO = "001";
		String STEP_TYPE = "E";
		formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		java.util.Date START_DATETIME = formatter.parse(formatter.format(date));
		java.util.Date END_DATETIME = formatter.parse(formatter.format(date));
		int TOTAL_CNT = 5;
		int SUCCESS_CNT = 5;
		int FAILED_CNT = 0;
		String SRC_FILE = "951_CF_PARTY_20171211.TXT";
		ETL_P_Log.write_ETL_Log(CENTRAL_NO, RECORD_DATE, FILE_TYPE, FILE_NAME, UPLOAD_NO, STEP_TYPE, START_DATETIME,
				END_DATETIME, TOTAL_CNT, SUCCESS_CNT, FAILED_CNT, SRC_FILE);
	}

	/**
	 * ETL_Log格式
	 * @param CENTRAL_NO 報送單位
	 * @param RECORD_DATE 檔案日期
	 * @param FILE_TYPE 檔名業務別
	 * @param FILE_NAME 檔案名稱
	 * @param UPLOAD_NO 上傳批號
	 * @param STEP_TYPE 步驟 STEP_TYPE
	 * @param START_DATETIME 執行開始日期時間
	 * @param END_DATETIME 執行結束日期時間
	 * @param TOTAL_CNT 總筆數
	 * @param SUCCESS_CNT 成功筆數
	 * @param FAILED_CNT 失敗筆數
	 * @param SRC_FILE 來源檔案
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void write_ETL_Log(String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE, String FILE_NAME,
			String UPLOAD_NO, String STEP_TYPE, java.util.Date START_DATETIME, java.util.Date END_DATETIME,
			int TOTAL_CNT, int SUCCESS_CNT, int FAILED_CNT, String SRC_FILE)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String insert_query = " INSERT INTO " + ETL_Profile.db2TableSchema + ".ETL_LOG (" +
				"CENTRAL_NO," +
				"RECORD_DATE," +
				"FILE_TYPE," +
				"FILE_NAME," +
				"UPLOAD_NO," +
				"STEP_TYPE," +
				"START_DATETIME," +
				"END_DATETIME," +
				"TOTAL_CNT," +
				"SUCCESS_CNT," +
				"FAILED_CNT," +
				"SRC_FILE" +
				") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(insert_query);

		pstmt.setString(1, CENTRAL_NO);
		pstmt.setDate(2, new Date(RECORD_DATE.getTime()));
		pstmt.setString(3, FILE_TYPE);
		pstmt.setString(4, FILE_NAME);
		pstmt.setString(5, UPLOAD_NO);
		pstmt.setString(6, STEP_TYPE);
		pstmt.setDate(7, new Date(START_DATETIME.getTime()));
		pstmt.setDate(8, new Date(END_DATETIME.getTime()));
		pstmt.setInt(9, TOTAL_CNT);
		pstmt.setInt(10, SUCCESS_CNT);
		pstmt.setInt(11, FAILED_CNT);
		pstmt.setString(12, SRC_FILE);

		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}

	}

	/**
	 * Error_Log格式
	 * @param CENTRAL_NO 報送單位
	 * @param RECORD_DATE 檔案日期
	 * @param FILE_TYPE 檔名業務別
	 * @param FILE_NAME 檔案名稱
	 * @param UPLOAD_NO 上傳批號
	 * @param STEP_TYPE 步驟
	 * @param ROW_COUNT 行數
	 * @param FIELD_NAME 欄位中文名稱
	 * @param ERROR_DESCRIPTION 錯誤描述
	 * @param SRC_FILE 來源檔案
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void write_Error_Log(String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE,
			String FILE_NAME, String UPLOAD_NO, String STEP_TYPE, String ROW_COUNT, String FIELD_NAME,
			String ERROR_DESCRIPTION, String SRC_FILE) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String insert_query = " INSERT INTO ETL_LOG (" +
				"CENTRAL_NO," +
				"RECORD_DATE," +
				"FILE_TYPE," +
				"FILE_NAME," +
				"UPLOAD_NO," +
				"STEP_TYPE," +
				"ROW_COUNT," +
				"FIELD_NAME," +
				"ERROR_DESCRIPTION," +
				"SRC_FILE" +
				") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(insert_query);

		pstmt.setString(1, CENTRAL_NO);
		pstmt.setDate(2, new Date(RECORD_DATE.getTime()));
		pstmt.setString(3, FILE_TYPE);
		pstmt.setString(4, FILE_NAME);
		pstmt.setString(5, UPLOAD_NO);
		pstmt.setString(6, STEP_TYPE);
		pstmt.setString(7, ROW_COUNT);
		pstmt.setString(8, FIELD_NAME);
		pstmt.setString(9, ERROR_DESCRIPTION);
		pstmt.setString(10, SRC_FILE);

		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}
	}

}
