package Bean;

import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_PARTY_PHONE_Data {
	
	// 報送單位
	private String central_no;
	// 檔案日期
	private Date record_date = new Date(); // test  temp
	// 檔名業務別
	private String file_type;
	// 本會代號
	private String domain_id;
	// 客戶統編
	private String party_number;
	// 異動代號
	private String change_code;
	// 電話類別
	private String phone_type;
	// 電話號碼
	private String phone_number;
	// 錯誤註記
	private String error_mark = "";// 預設無錯誤
	
	// Constructor
	public ETL_Bean_PARTY_PHONE_Data(ETL_Tool_ParseFileName pfn) {
		
		this.central_no = pfn.getCentral_No();
		this.record_date = pfn.getRecord_Date();
		this.file_type = pfn.getFile_Type();
	}
	
	public String getCentral_no() {
		return central_no;
	}
	
	public void setCentral_no(String central_no) {
		this.central_no = central_no;
	}

	public Date getRecord_date() {
		return record_date;
	}

	public void setRecord_date(Date record_date) {
		this.record_date = record_date;
	}

	public String getFile_type() {
		return file_type;
	}

	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}

	public String getDomain_id() {
		return domain_id;
	}

	public void setDomain_id(String domain_id) {
		this.domain_id = domain_id;
	}

	public String getParty_number() {
		return party_number;
	}

	public void setParty_number(String party_number) {
		this.party_number = party_number;
	}

	public String getChange_code() {
		return change_code;
	}

	public void setChange_code(String change_code) {
		this.change_code = change_code;
	}

	public String getPhone_type() {
		return phone_type;
	}

	public void setPhone_type(String phone_type) {
		this.phone_type = phone_type;
	}

	public String getPhone_number() {
		return phone_number;
	}

	public void setPhone_number(String phone_number) {
		this.phone_number = phone_number;
	}

	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}
	
}
