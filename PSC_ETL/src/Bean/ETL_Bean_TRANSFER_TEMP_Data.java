package Bean;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_TRANSFER_TEMP_Data {
	//報送單位	
	private String central_no;
	//檔案日期	
	private Date record_date;
	//檔名業務別	
	private String file_type;
	//行數
	private Integer row_count;
	//本會代號	
	private String domain_id;
	//客戶統編	
	private String party_number;
	//匯款編號(交易編號)	
	private String transfer_id;
	//匯款日期	
	private Date transfer_date;
	//實際匯款時間;	
	private Timestamp transfer_time;
	//匯入或匯出	
	private String direction;
	//交易幣別	
	private String instructed_currency_code;
	//交易金額	
	private BigDecimal instructed_amount;
	//匯款人銀行帳戶編號	
	private String ordering_customer_account_number;
	//匯款人顧客編號	
	private String ordering_customer_party_id;
	//匯款人顧客姓名	
	private String rdering_customer_party_name;
	//匯款人顧客地址	
	private String ordering_customer_address_line;
	//匯款銀行bic 編碼	
	private String ordering_bank_bic;
	//受款人銀行帳戶編號	
	private String beneficiary_customer_account_number;
	//受款人姓名	
	private String beneficiary_customer_party_name;
	//受款銀行顧客編號	
	private String beneficiary_customer_party_id;
	//受款銀行bic 編碼	
	private String beneficiary_bank_bic;
	//交易類別	
	private String transaction_type;
	//匯款系統	
	private String transaction_system;
	//匯款管道類別	
	private String channel_type;
	//匯款管道編號	
	private String channel_id;
	//匯款執行分行	
	private String execution_branch_code;
	//匯款執行者代號	
	private String executer_id;
	//錯誤註記	
	private String error_mark;
	
	public ETL_Bean_TRANSFER_TEMP_Data(ETL_Tool_ParseFileName pfn) {
		if (pfn != null) {
			this.central_no = pfn.getCentral_No();
			this.record_date = pfn.getRecord_Date();
			this.file_type = pfn.getFile_Type();
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
	public String getParty_number() {
		return party_number;
	}
	public void setParty_number(String party_number) {
		this.party_number = party_number;
	}
	public String getTransfer_id() {
		return transfer_id;
	}
	public void setTransfer_id(String transfer_id) {
		this.transfer_id = transfer_id;
	}
	public Date getTransfer_date() {
		return transfer_date;
	}
	public void setTransfer_date(Date transfer_date) {
		this.transfer_date = transfer_date;
	}
	public Timestamp getTransfer_time() {
		return transfer_time;
	}
	public void setTransfer_time(Timestamp transfer_time) {
		this.transfer_time = transfer_time;
	}
	public String getDirection() {
		return direction;
	}
	public void setDirection(String direction) {
		this.direction = direction;
	}
	public String getInstructed_currency_code() {
		return instructed_currency_code;
	}
	public void setInstructed_currency_code(String instructed_currency_code) {
		this.instructed_currency_code = instructed_currency_code;
	}
	public BigDecimal getInstructed_amount() {
		return instructed_amount;
	}
	public void setInstructed_amount(BigDecimal instructed_amount) {
		this.instructed_amount = instructed_amount;
	}
	public String getOrdering_customer_account_number() {
		return ordering_customer_account_number;
	}
	public void setOrdering_customer_account_number(String ordering_customer_account_number) {
		this.ordering_customer_account_number = ordering_customer_account_number;
	}
	public String getOrdering_customer_party_id() {
		return ordering_customer_party_id;
	}
	public void setOrdering_customer_party_id(String ordering_customer_party_id) {
		this.ordering_customer_party_id = ordering_customer_party_id;
	}
	public String getRdering_customer_party_name() {
		return rdering_customer_party_name;
	}
	public void setRdering_customer_party_name(String rdering_customer_party_name) {
		this.rdering_customer_party_name = rdering_customer_party_name;
	}
	public String getOrdering_customer_address_line() {
		return ordering_customer_address_line;
	}
	public void setOrdering_customer_address_line(String ordering_customer_address_line) {
		this.ordering_customer_address_line = ordering_customer_address_line;
	}
	public String getOrdering_bank_bic() {
		return ordering_bank_bic;
	}
	public void setOrdering_bank_bic(String ordering_bank_bic) {
		this.ordering_bank_bic = ordering_bank_bic;
	}
	public String getBeneficiary_customer_account_number() {
		return beneficiary_customer_account_number;
	}
	public void setBeneficiary_customer_account_number(String beneficiary_customer_account_number) {
		this.beneficiary_customer_account_number = beneficiary_customer_account_number;
	}
	public String getBeneficiary_customer_party_name() {
		return beneficiary_customer_party_name;
	}
	public void setBeneficiary_customer_party_name(String beneficiary_customer_party_name) {
		this.beneficiary_customer_party_name = beneficiary_customer_party_name;
	}
	public String getBeneficiary_customer_party_id() {
		return beneficiary_customer_party_id;
	}
	public void setBeneficiary_customer_party_id(String beneficiary_customer_party_id) {
		this.beneficiary_customer_party_id = beneficiary_customer_party_id;
	}
	public String getBeneficiary_bank_bic() {
		return beneficiary_bank_bic;
	}
	public void setBeneficiary_bank_bic(String beneficiary_bank_bic) {
		this.beneficiary_bank_bic = beneficiary_bank_bic;
	}
	public String getTransaction_type() {
		return transaction_type;
	}
	public void setTransaction_type(String transaction_type) {
		this.transaction_type = transaction_type;
	}
	public String getTransaction_system() {
		return transaction_system;
	}
	public void setTransaction_system(String transaction_system) {
		this.transaction_system = transaction_system;
	}
	public String getChannel_type() {
		return channel_type;
	}
	public void setChannel_type(String channel_type) {
		this.channel_type = channel_type;
	}
	public String getChannel_id() {
		return channel_id;
	}
	public void setChannel_id(String channel_id) {
		this.channel_id = channel_id;
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
	public String getError_mark() {
		return error_mark;
	}
	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

}
