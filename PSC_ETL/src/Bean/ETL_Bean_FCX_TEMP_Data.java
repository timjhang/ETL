package Bean;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_FCX_TEMP_Data {
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
	// 顧客統編
	private String party_number;
	// 顧客姓名
	private String party_last_name_1;
	// 顧客生日
	private Date date_of_birth;
	// 顧客國籍
	private String nationality_code;
	// 交易編號
	private String transaction_id;
	// 交易日期
	private Date transaction_date;
	// 實際交易時間
	private Timestamp transaction_time;
	// 結構或結售
	private String direction;
	// 交易幣別
	private String currency_code;
	// 交易金額
	private BigDecimal amount;
	// 申報國別
	private String ordering_customer_country;
	// 交易管道類別
	private String channel_type;
	// 交易執行分行
	private String execution_branch_code;
	// 交易執行者代號
	private String executer_id;
	// 錯誤註記
	private String error_mark="";
	//上傳批號
	private String upload_no="";

	public ETL_Bean_FCX_TEMP_Data(ETL_Tool_ParseFileName pfn) {
		if (pfn != null) {
			this.central_no = pfn.getCentral_No();
			this.record_date = pfn.getRecord_Date();
			this.file_type = pfn.getFile_Type();
			this.upload_no = pfn.getUpload_no();
		}
	}

	
	public String getUpload_no() {
		return upload_no;
	}


	public void setUpload_no(String upload_no) {
		this.upload_no = upload_no;
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

	public String getParty_last_name_1() {
		return party_last_name_1;
	}

	public void setParty_last_name_1(String party_last_name_1) {
		this.party_last_name_1 = party_last_name_1;
	}

	public Date getDate_of_birth() {
		return date_of_birth;
	}

	public void setDate_of_birth(Date date_of_birth) {
		this.date_of_birth = date_of_birth;
	}

	public String getNationality_code() {
		return nationality_code;
	}

	public void setNationality_code(String nationality_code) {
		this.nationality_code = nationality_code;
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

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	public String getCurrency_code() {
		return currency_code;
	}

	public void setCurrency_code(String currency_code) {
		this.currency_code = currency_code;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public String getOrdering_customer_country() {
		return ordering_customer_country;
	}

	public void setOrdering_customer_country(String ordering_customer_country) {
		this.ordering_customer_country = ordering_customer_country;
	}

	public String getChannel_type() {
		return channel_type;
	}

	public void setChannel_type(String channel_type) {
		this.channel_type = channel_type;
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
