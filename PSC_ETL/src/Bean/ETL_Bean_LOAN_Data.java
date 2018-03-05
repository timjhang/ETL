package Bean;

import java.math.BigDecimal;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_LOAN_Data {

	private String central_no;// 報送單位
	private Date record_date;// 檔案日期
	private String file_type;// 檔名業務別
	private Integer row_count;// 行數
	private String domain_id;// 本會代號
	private String party_number;// 客戶統編
	private String change_code;// 異動代號
	private String loan_number;// 放款帳號
	private Date loan_date;// 初貸日
	private String loan_master_number;// 批覆書編號
	private String loan_detail_number;// 額度編號
	private String loan_category_code;// 放款種類
	private String loan_type_code;// 利率類別
	private String loan_currency_code;// 幣別
	private BigDecimal loan_amount;// 貸放金額
	private String loan_status_code;// 放款狀態
	private BigDecimal outstanding_loan_balance;// 本金餘額
	private BigDecimal total_repayments_value;// 已還本金
	private Integer delinquency_days;// 違繳天數
	private String execution_branch_code;// 帳戶行
	private BigDecimal last_payment_value;// 到期一次還本金額
	private String cls;// 收回原因
	private String error_mark = "";// 錯誤註記
	private String upload_no;// 上傳批號(測試用)

	public ETL_Bean_LOAN_Data(ETL_Tool_ParseFileName pfn) {

		this.central_no = pfn.getCentral_No();// 報送單位
		this.record_date = pfn.getRecord_Date();// 檔案日期
		this.file_type = pfn.getFile_Type();// 檔名業務別
		this.upload_no = pfn.getUpload_no(); 
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

	public Integer getRow_count() {
		return row_count;
	}

	public void setRow_count(Integer row_count) {
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

	public String getLoan_number() {
		return loan_number;
	}

	public void setLoan_number(String loan_number) {
		this.loan_number = loan_number;
	}

	public Date getLoan_date() {
		return loan_date;
	}

	public void setLoan_date(Date loan_date) {
		this.loan_date = loan_date;
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

	public String getLoan_category_code() {
		return loan_category_code;
	}

	public void setLoan_category_code(String loan_category_code) {
		this.loan_category_code = loan_category_code;
	}

	public String getLoan_type_code() {
		return loan_type_code;
	}

	public void setLoan_type_code(String loan_type_code) {
		this.loan_type_code = loan_type_code;
	}

	public String getLoan_currency_code() {
		return loan_currency_code;
	}

	public void setLoan_currency_code(String loan_currency_code) {
		this.loan_currency_code = loan_currency_code;
	}

	public BigDecimal getLoan_amount() {
		return loan_amount;
	}

	public void setLoan_amount(BigDecimal loan_amount) {
		this.loan_amount = loan_amount;
	}

	public String getLoan_status_code() {
		return loan_status_code;
	}

	public void setLoan_status_code(String loan_status_code) {
		this.loan_status_code = loan_status_code;
	}

	public BigDecimal getOutstanding_loan_balance() {
		return outstanding_loan_balance;
	}

	public void setOutstanding_loan_balance(BigDecimal outstanding_loan_balance) {
		this.outstanding_loan_balance = outstanding_loan_balance;
	}

	public BigDecimal getTotal_repayments_value() {
		return total_repayments_value;
	}

	public void setTotal_repayments_value(BigDecimal total_repayments_value) {
		this.total_repayments_value = total_repayments_value;
	}

	public Integer getDelinquency_days() {
		return delinquency_days;
	}

	public void setDelinquency_days(Integer delinquency_days) {
		this.delinquency_days = delinquency_days;
	}

	public String getExecution_branch_code() {
		return execution_branch_code;
	}

	public void setExecution_branch_code(String execution_branch_code) {
		this.execution_branch_code = execution_branch_code;
	}

	public BigDecimal getLast_payment_value() {
		return last_payment_value;
	}

	public void setLast_payment_value(BigDecimal last_payment_value) {
		this.last_payment_value = last_payment_value;
	}

	public String getCls() {
		return cls;
	}

	public void setCls(String cls) {
		this.cls = cls;
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
