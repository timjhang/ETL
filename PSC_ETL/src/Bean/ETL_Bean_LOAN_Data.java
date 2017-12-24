package Bean;

import java.math.BigDecimal;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_LOAN_Data {

	private String central_no;// 報送單位
	private Date record_date;// 檔案日期
	private String file_type;// 檔名業務別
	private int row_count;// 行數
	private String domain_id;// 本會代號
	private String party_number;// 客戶統編
	private String change_code;// 異動代號
	private String loan_master_number;// 批覆書編號
	private String loan_detail_number;// 額度編號
	private Date loan_application_date;// 批覆書申請日期
	private Date loan_approval_date;// 批覆書核准日期
	private Date approval_date;// 額度核准日
	private BigDecimal approval_amount;// 批准限額
	private BigDecimal remain_approval_amount;// 可動用限額
	private String error_mark = "";// 錯誤註記

	public ETL_Bean_LOAN_Data(ETL_Tool_ParseFileName pfn) {

		this.central_no = pfn.getCentral_No();// 報送單位
		this.record_date = pfn.getRecord_Date();// 檔案日期
		this.file_type = pfn.getFile_Type();// 檔名業務別
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

	public int getRow_count() {
		return row_count;
	}

	public void setRow_count(int row_count) {
		this.row_count = row_count;
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

	public String getLoan_master_number() {
		return loan_master_number;
	}

	public void setLoan_master_number(String loan_master_number) {
		this.loan_master_number = loan_master_number;
	}

	public String getLoan_detail_number() {
		return loan_detail_number;
	}

	public void setLoan_detail_number(String loan_detail_number) {
		this.loan_detail_number = loan_detail_number;
	}

	public Date getLoan_application_date() {
		return loan_application_date;
	}

	public void setLoan_application_date(Date loan_application_date) {
		this.loan_application_date = loan_application_date;
	}

	public Date getLoan_approval_date() {
		return loan_approval_date;
	}

	public void setLoan_approval_date(Date loan_approval_date) {
		this.loan_approval_date = loan_approval_date;
	}

	public Date getApproval_date() {
		return approval_date;
	}

	public void setApproval_date(Date approval_date) {
		this.approval_date = approval_date;
	}

	public BigDecimal getApproval_amount() {
		return approval_amount;
	}

	public void setApproval_amount(BigDecimal approval_amount) {
		this.approval_amount = approval_amount;
	}

	public BigDecimal getRemain_approval_amount() {
		return remain_approval_amount;
	}

	public void setRemain_approval_amount(BigDecimal remain_approval_amount) {
		this.remain_approval_amount = remain_approval_amount;
	}

	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

}
