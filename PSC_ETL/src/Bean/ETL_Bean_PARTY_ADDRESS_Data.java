package Bean;

import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_PARTY_ADDRESS_Data {

	private String central_no;// 報送單位
	private Date record_date;// 檔案日期
	private String file_type;// 檔名業務別
	private Integer row_count;// 行數
	private String domain_id;// 本會代號
	private String party_number;// 客戶統編
	private String change_code;// 異動代號
	private String address_type;// 地址類別
	private String country;// 地址國別
	private String po_box;// 郵遞區號
	private String address_line_1;// 地址
	private String error_mark = "";// 錯誤註記
	private String upload_no;// 上傳批號(測試用) 

	public ETL_Bean_PARTY_ADDRESS_Data(ETL_Tool_ParseFileName pfn) {

		this.central_no = pfn.getCentral_No();
		this.record_date = pfn.getRecord_Date();
		this.file_type = pfn.getFile_Type();
		this.upload_no = pfn.getUpload_no();
	}

	public int getRow_count() {
		return row_count;
	}

	public void setRow_count(int row_count) {
		this.row_count = row_count;
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

	public String getAddress_type() {
		return address_type;
	}

	public void setAddress_type(String address_type) {
		this.address_type = address_type;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getPo_box() {
		return po_box;
	}

	public void setPo_box(String po_box) {
		this.po_box = po_box;
	}

	public String getAddress_line_1() {
		return address_line_1;
	}

	public void setAddress_line_1(String address_line_1) {
		this.address_line_1 = address_line_1;
	}

	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

	public String getUpload_no() {
		return upload_no;
	}

	public void setUpload_no(String upload_no) {
		this.upload_no = upload_no;
	}
}
