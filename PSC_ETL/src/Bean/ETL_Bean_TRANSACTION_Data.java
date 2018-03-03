package Bean;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_TRANSACTION_Data {

	private String central_no;// 報送單位
	private Date record_date;// 檔案日期
	private String file_type;// 檔名業務別
	private Integer row_count;// 行數
	private String domain_id;// 本會代號
	private String party_number;// 客戶統編
	private String account_id;// 帳號
	private String transaction_id;// 主機交易序號
	private Date transaction_date;// 作帳日
	private Timestamp transaction_time;// 實際交易時間
	private String currency_code;// 交易幣別
	private String amt_sign;// 交易金額正負號
	private BigDecimal amount;// 交易金額
	private String direction;// 存提區分
	private String transaction_type;// 交易類別
	private String channel_type;// 交易管道
	private String transaction_purpose;// 交易代號
	private String transaction_reference_number;// 原交易代號
	private String transaction_summary;// 交易摘要
	private String transaction_description;// 備註
	private String execution_branch_code;// 操作行
	private String executer_id;// 操作櫃員代號或姓名
	private String ordering_customer_party_name;// 匯款人姓名
	private String beneficiary_customer_party_name;// 受款人姓名
	private String beneficiary_customer_bank_bic;// 受款人銀行
	private String beneficiary_customer_account_number;// 受款人帳號
	private BigDecimal repayment_principal;// 還款本金
	private String ec_flag;// 更正記號
	private String ordering_customer_country;// 申報國別
	private String error_mark = "";// 錯誤註記
	private String upload_no;// 上傳批號(測試用)

	public ETL_Bean_TRANSACTION_Data(ETL_Tool_ParseFileName pfn) {

		this.central_no = pfn.getCentral_No();// 報送單位
		this.record_date = pfn.getRecord_Date();// 檔案日期
		this.file_type = pfn.getFile_Type();// 檔名業務別
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

	public String getFile_type() {
		return file_type;
	}

	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}

	public String getAccount_id() {
		return account_id;
	}

	public void setAccount_id(String account_id) {
		this.account_id = account_id;
	}

	public String getTransaction_id() {
		return transaction_id;
	}

	public void setTransaction_id(String transaction_id) {
		this.transaction_id = transaction_id;
	}

	public Date getTransaction_date() {
		return transaction_date;
	}

	public void setTransaction_date(Date transaction_date) {
		this.transaction_date = transaction_date;
	}

	public Timestamp getTransaction_time() {
		return transaction_time;
	}

	public void setTransaction_time(Timestamp transaction_time) {
		this.transaction_time = transaction_time;
	}

	public String getCurrency_code() {
		return currency_code;
	}

	public void setCurrency_code(String currency_code) {
		this.currency_code = currency_code;
	}

	public String getAmt_sign() {
		return amt_sign;
	}

	public void setAmt_sign(String amt_sign) {
		this.amt_sign = amt_sign;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	public String getTransaction_type() {
		return transaction_type;
	}

	public void setTransaction_type(String transaction_type) {
		this.transaction_type = transaction_type;
	}

	public String getChannel_type() {
		return channel_type;
	}

	public void setChannel_type(String channel_type) {
		this.channel_type = channel_type;
	}

	public String getTransaction_purpose() {
		return transaction_purpose;
	}

	public void setTransaction_purpose(String transaction_purpose) {
		this.transaction_purpose = transaction_purpose;
	}

	public String getTransaction_reference_number() {
		return transaction_reference_number;
	}

	public void setTransaction_reference_number(String transaction_reference_number) {
		this.transaction_reference_number = transaction_reference_number;
	}

	public String getTransaction_summary() {
		return transaction_summary;
	}

	public void setTransaction_summary(String transaction_summary) {
		this.transaction_summary = transaction_summary;
	}

	public String getTransaction_description() {
		return transaction_description;
	}

	public void setTransaction_description(String transaction_description) {
		this.transaction_description = transaction_description;
	}

	public String getExecution_branch_code() {
		return execution_branch_code;
	}

	public void setExecution_branch_code(String execution_branch_code) {
		this.execution_branch_code = execution_branch_code;
	}

	public String getExecuter_id() {
		return executer_id;
	}

	public void setExecuter_id(String executer_id) {
		this.executer_id = executer_id;
	}

	public String getOrdering_customer_party_name() {
		return ordering_customer_party_name;
	}

	public void setOrdering_customer_party_name(String ordering_customer_party_name) {
		this.ordering_customer_party_name = ordering_customer_party_name;
	}

	public String getBeneficiary_customer_party_name() {
		return beneficiary_customer_party_name;
	}

	public void setBeneficiary_customer_party_name(String beneficiary_customer_party_name) {
		this.beneficiary_customer_party_name = beneficiary_customer_party_name;
	}

	public String getBeneficiary_customer_bank_bic() {
		return beneficiary_customer_bank_bic;
	}

	public void setBeneficiary_customer_bank_bic(String beneficiary_customer_bank_bic) {
		this.beneficiary_customer_bank_bic = beneficiary_customer_bank_bic;
	}

	public String getBeneficiary_customer_account_number() {
		return beneficiary_customer_account_number;
	}

	public void setBeneficiary_customer_account_number(String beneficiary_customer_account_number) {
		this.beneficiary_customer_account_number = beneficiary_customer_account_number;
	}

	public BigDecimal getRepayment_principal() {
		return repayment_principal;
	}

	public void setRepayment_principal(BigDecimal repayment_principal) {
		this.repayment_principal = repayment_principal;
	}

	public String getEc_flag() {
		return ec_flag;
	}

	public void setEc_flag(String ec_flag) {
		this.ec_flag = ec_flag;
	}

	public String getOrdering_customer_country() {
		return ordering_customer_country;
	}

	public void setOrdering_customer_country(String ordering_customer_country) {
		this.ordering_customer_country = ordering_customer_country;
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
