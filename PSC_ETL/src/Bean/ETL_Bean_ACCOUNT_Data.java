package Bean;

import java.math.BigDecimal;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_ACCOUNT_Data {

	private String central_no;// 報送單位
	private Date record_date;// 檔案日期
	private String file_type;// 檔名業務別
	private String domain_id;// 本會代號
	private String party_number;// 客戶統編
	private String change_code;// 異動代號
	private String account_id;// 帳號
	private String branch_code;// 帳戶行
	private String account_type_code;// 帳戶類別
	private String property_code;// 連結服務
	private String currency_code;// 幣別
	private String status_code;// 帳戶狀態
	private String account_opening_channel;// 開戶管道
	private Date account_open_date;// 開戶日期
	private Date account_close_date;// 結清(銷戶)日期
	private String balance_acct_currency_sign;// 帳戶餘額正負號
	private BigDecimal balance_acct_currency_value;// 帳戶餘額
	private String balance_last_month_avg_sign;// 過去一個月平均餘額正負號
	private BigDecimal balance_last_month_avg_value;// 過去一個月平均餘額
	private String caution_note;// 警示註記
	private String error_mark = "";// 錯誤註記

	public ETL_Bean_ACCOUNT_Data(ETL_Tool_ParseFileName pfn, String domain_id, String party_number, String change_code,
			String account_id, String branch_code, String account_type_code, String property_code, String currency_code,
			String status_code, String account_opening_channel, Date account_open_date, Date account_close_date,
			String balance_acct_currency_sign, BigDecimal balance_acct_currency_value,
			String balance_last_month_avg_sign, BigDecimal balance_last_month_avg_value, String caution_note) {

		this.central_no = pfn.getCentral_No();// 報送單位
		this.record_date = pfn.getRecord_Date();// 檔案日期
		this.file_type = pfn.getFile_Type();// 檔名業務別
		this.domain_id = domain_id;// 本會代號
		this.party_number = party_number;// 客戶統編
		this.change_code = change_code;// 異動代號
		this.account_id = account_id;// 帳號
		this.branch_code = branch_code;// 帳戶行
		this.account_type_code = account_type_code;// 帳戶類別
		this.property_code = property_code;// 連結服務
		this.currency_code = currency_code;// 幣別
		this.status_code = status_code;// 帳戶狀態
		this.account_opening_channel = account_opening_channel;// 開戶管道
		this.account_open_date = account_open_date;// 開戶日期
		this.account_close_date = account_close_date;// 結清(銷戶)日期
		this.balance_acct_currency_sign = balance_acct_currency_sign;// 帳戶餘額正負號
		this.balance_acct_currency_value = balance_acct_currency_value;// 帳戶餘額
		this.balance_last_month_avg_sign = balance_last_month_avg_sign;// 過去一個月平均餘額正負號
		this.balance_last_month_avg_value = balance_last_month_avg_value;// 過去一個月平均餘額
		this.caution_note = caution_note;// 警示註記
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

	public String getAccount_id() {
		return account_id;
	}

	public void setAccount_id(String account_id) {
		this.account_id = account_id;
	}

	public String getBranch_code() {
		return branch_code;
	}

	public void setBranch_code(String branch_code) {
		this.branch_code = branch_code;
	}

	public String getAccount_type_code() {
		return account_type_code;
	}

	public void setAccount_type_code(String account_type_code) {
		this.account_type_code = account_type_code;
	}

	public String getProperty_code() {
		return property_code;
	}

	public void setProperty_code(String property_code) {
		this.property_code = property_code;
	}

	public String getCurrency_code() {
		return currency_code;
	}

	public void setCurrency_code(String currency_code) {
		this.currency_code = currency_code;
	}

	public String getStatus_code() {
		return status_code;
	}

	public void setStatus_code(String status_code) {
		this.status_code = status_code;
	}

	public String getAccount_opening_channel() {
		return account_opening_channel;
	}

	public void setAccount_opening_channel(String account_opening_channel) {
		this.account_opening_channel = account_opening_channel;
	}

	public Date getAccount_open_date() {
		return account_open_date;
	}

	public void setAccount_open_date(Date account_open_date) {
		this.account_open_date = account_open_date;
	}

	public Date getAccount_close_date() {
		return account_close_date;
	}

	public void setAccount_close_date(Date account_close_date) {
		this.account_close_date = account_close_date;
	}

	public String getBalance_acct_currency_sign() {
		return balance_acct_currency_sign;
	}

	public void setBalance_acct_currency_sign(String balance_acct_currency_sign) {
		this.balance_acct_currency_sign = balance_acct_currency_sign;
	}

	public BigDecimal getBalance_acct_currency_value() {
		return balance_acct_currency_value;
	}

	public void setBalance_acct_currency_value(BigDecimal balance_acct_currency_value) {
		this.balance_acct_currency_value = balance_acct_currency_value;
	}

	public String getBalance_last_month_avg_sign() {
		return balance_last_month_avg_sign;
	}

	public void setBalance_last_month_avg_sign(String balance_last_month_avg_sign) {
		this.balance_last_month_avg_sign = balance_last_month_avg_sign;
	}

	public BigDecimal getBalance_last_month_avg_value() {
		return balance_last_month_avg_value;
	}

	public void setBalance_last_month_avg_value(BigDecimal balance_last_month_avg_value) {
		this.balance_last_month_avg_value = balance_last_month_avg_value;
	}

	public String getCaution_note() {
		return caution_note;
	}

	public void setCaution_note(String caution_note) {
		this.caution_note = caution_note;
	}

	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

}
