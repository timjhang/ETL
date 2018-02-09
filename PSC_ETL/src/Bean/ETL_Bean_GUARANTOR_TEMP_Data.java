package Bean;

import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_GUARANTOR_TEMP_Data {
	// 報送單位
	private String central_no;
	// 檔案日期
	private Date record_date;
	// 檔名業務別
	private String file_type;
	//行數
	private Integer row_count;
	// 本會代號
	private String domain_id;
	// 批覆書編號/申請書編號
	private String loan_master_number;
	// 異動代號
	private String change_code;
	// 額度編號
	private String loan_detail_number;
	// 保證人類別
	private String guaranty_type;
	// 保證人姓名
	private String last_name_1;
	// 保證人統編
	private String guarantor_id;
	// 保證人出生年月日
	private Date date_of_birth;
	// 保證人國籍
	private String address_country;
	// 與主債務人關係
	private String relation_type_code;
	// 客戶統編
	private String party_number;
	// 錯誤註記
	private String error_mark="";
	//上傳批號
	private String upload_no="";
	
	public ETL_Bean_GUARANTOR_TEMP_Data(ETL_Tool_ParseFileName pfn) {
		if (pfn != null) {
			this.central_no = pfn.getCentral_No();
			this.record_date = pfn.getRecord_Date();
			this.file_type = pfn.getFile_Type();
			this.upload_no = pfn.getUpload_no();
		}
	}
	
	
	public Integer getRow_count() {
		return row_count;
	}


	public void setRow_count(Integer row_count) {
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
	public String getLoan_master_number() {
		return loan_master_number;
	}
	public void setLoan_master_number(String loan_master_number) {
		this.loan_master_number = loan_master_number;
	}
	public String getChange_code() {
		return change_code;
	}
	public void setChange_code(String change_code) {
		this.change_code = change_code;
	}
	public String getLoan_detail_number() {
		return loan_detail_number;
	}
	public void setLoan_detail_number(String loan_detail_number) {
		this.loan_detail_number = loan_detail_number;
	}
	public String getGuaranty_type() {
		return guaranty_type;
	}
	public void setGuaranty_type(String guaranty_type) {
		this.guaranty_type = guaranty_type;
	}
	public String getLast_name_1() {
		return last_name_1;
	}
	public void setLast_name_1(String last_name_1) {
		this.last_name_1 = last_name_1;
	}
	public String getGuarantor_id() {
		return guarantor_id;
	}
	public void setGuarantor_id(String guarantor_id) {
		this.guarantor_id = guarantor_id;
	}
	public Date getDate_of_birth() {
		return date_of_birth;
	}
	public void setDate_of_birth(Date date_of_birth) {
		this.date_of_birth = date_of_birth;
	}
	public String getAddress_country() {
		return address_country;
	}
	public void setAddress_country(String address_country) {
		this.address_country = address_country;
	}
	public String getRelation_type_code() {
		return relation_type_code;
	}
	public void setRelation_type_code(String relation_type_code) {
		this.relation_type_code = relation_type_code;
	}
	public String getParty_number() {
		return party_number;
	}
	public void setParty_number(String party_number) {
		this.party_number = party_number;
	}
	public String getError_mark() {
		return error_mark;
	}
	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}
}
