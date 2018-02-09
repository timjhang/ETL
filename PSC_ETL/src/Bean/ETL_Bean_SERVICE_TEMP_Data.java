package Bean;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_SERVICE_TEMP_Data {
	
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
	//服務編號	
	private String service_id;
	//服務日期	
	private Date service_date;
	//服務時間	
	private Time service_time;
	//服務類別	
	private String service_type;
	//服務管道類別	
	private String channel_type;
	//服務管道編號	
	private String channel_id;
	//服務參考編號	
	private String service_reference_number;
	//資料變更前詳細內容	
	private String previous_value;
	//資料變更後詳細內容	
	private String new_value;
	//服務詳細內容	
	private String service_description;
	//服務執行分行	
	private String execution_branch_code;
	//執行行員代號	
	private String executer_id;
	//錯誤註記	
	private String error_mark="";
	//上傳批號
	private String upload_no="";
	
	public ETL_Bean_SERVICE_TEMP_Data(ETL_Tool_ParseFileName pfn) {
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
	public String getService_id() {
		return service_id;
	}
	public void setService_id(String service_id) {
		this.service_id = service_id;
	}
	public Date getService_date() {
		return service_date;
	}
	public void setService_date(Date service_date) {
		this.service_date = service_date;
	}

	public Time getService_time() {
		return service_time;
	}

	public void setService_time(Time service_time) {
		this.service_time = service_time;
	}


	public String getService_type() {
		return service_type;
	}
	public void setService_type(String service_type) {
		this.service_type = service_type;
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
	public String getService_reference_number() {
		return service_reference_number;
	}
	public void setService_reference_number(String service_reference_number) {
		this.service_reference_number = service_reference_number;
	}
	public String getPrevious_value() {
		return previous_value;
	}
	public void setPrevious_value(String previous_value) {
		this.previous_value = previous_value;
	}
	public String getNew_value() {
		return new_value;
	}
	public void setNew_value(String new_value) {
		this.new_value = new_value;
	}
	public String getService_description() {
		return service_description;
	}
	public void setService_description(String service_description) {
		this.service_description = service_description;
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
