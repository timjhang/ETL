package Bean;

public class ETL_P_ErrorLog_Data {
	// ETL Error Log Data
	
	private String CENTRAL_NO; // 報送單位
	private java.util.Date RECORD_DATE; // 檔案日期
	private String FILE_TYPE; // 檔名業務別
	private String FILE_NAME; // 檔案名稱
	private String UPLOAD_NO; // 上傳批號
	private String STEP_TYPE; // 步驟
	private String ROW_COUNT; // 行數
	private String FIELD_NAME; // 欄位中文名稱
	private String ERROR_DESCRIPTION; // 錯誤描述
	private String SRC_FILE; // 來源檔案
	
	// ETL_P_ErrorLog_Data's Constructor
	public ETL_P_ErrorLog_Data(String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE, String FILE_NAME, String UPLOAD_NO,
			String STEP_TYPE, String ROW_COUNT, String FIELD_NAME, String ERROR_DESCRIPTION, String SRC_FILE) {
		
		this.CENTRAL_NO = CENTRAL_NO;
		this.RECORD_DATE = RECORD_DATE;
		this.FILE_TYPE = FILE_TYPE;
		this.FILE_NAME = FILE_NAME;
		this.UPLOAD_NO = UPLOAD_NO;
		this.STEP_TYPE = STEP_TYPE;
		this.ROW_COUNT = ROW_COUNT;
		this.FIELD_NAME = FIELD_NAME;
		this.ERROR_DESCRIPTION = ERROR_DESCRIPTION;
		this.SRC_FILE = SRC_FILE;
	}
	
	public String getCENTRAL_NO() {
		return CENTRAL_NO;
	}
	
	public void setCENTRAL_NO(String cENTRAL_NO) {
		CENTRAL_NO = cENTRAL_NO;
	}

	public java.util.Date getRECORD_DATE() {
		return RECORD_DATE;
	}

	public void setRECORD_DATE(java.util.Date rECORD_DATE) {
		RECORD_DATE = rECORD_DATE;
	}

	public String getFILE_TYPE() {
		return FILE_TYPE;
	}

	public void setFILE_TYPE(String fILE_TYPE) {
		FILE_TYPE = fILE_TYPE;
	}

	public String getFILE_NAME() {
		return FILE_NAME;
	}

	public void setFILE_NAME(String fILE_NAME) {
		FILE_NAME = fILE_NAME;
	}

	public String getUPLOAD_NO() {
		return UPLOAD_NO;
	}

	public void setUPLOAD_NO(String uPLOAD_NO) {
		UPLOAD_NO = uPLOAD_NO;
	}

	public String getSTEP_TYPE() {
		return STEP_TYPE;
	}

	public void setSTEP_TYPE(String sTEP_TYPE) {
		STEP_TYPE = sTEP_TYPE;
	}

	public String getROW_COUNT() {
		return ROW_COUNT;
	}

	public void setROW_COUNT(String rOW_COUNT) {
		ROW_COUNT = rOW_COUNT;
	}

	public String getFIELD_NAME() {
		return FIELD_NAME;
	}

	public void setFIELD_NAME(String fIELD_NAME) {
		FIELD_NAME = fIELD_NAME;
	}

	public String getERROR_DESCRIPTION() {
		return ERROR_DESCRIPTION;
	}

	public void setERROR_DESCRIPTION(String eRROR_DESCRIPTION) {
		ERROR_DESCRIPTION = eRROR_DESCRIPTION;
	}

	public String getSRC_FILE() {
		return SRC_FILE;
	}

	public void setSRC_FILE(String sRC_FILE) {
		SRC_FILE = sRC_FILE;
	}
	
}
