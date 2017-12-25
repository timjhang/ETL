package Bean;

import java.math.BigDecimal;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_FX_RATE_TEMP_Data {
	
	// 報送單位
	private String central_no;
	// 檔案日期
	private Date record_date;
	// 檔名業務別
	private String file_type;
	//行數
	private Integer row_count;
	// 匯率日期
	private Date value_date;
	// 外幣代號1
	private String currency_code_1;
	// 外幣代號2
	private String currency_code_2;
	// 匯率
	private BigDecimal exchange_rate;
	// 錯誤註記
	private String error_mark;
	
	public ETL_Bean_FX_RATE_TEMP_Data(ETL_Tool_ParseFileName pfn) {
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
	public Date getValue_date() {
		return value_date;
	}
	public void setValue_date(Date value_date) {
		this.value_date = value_date;
	}
	public String getCurrency_code_1() {
		return currency_code_1;
	}
	public void setCurrency_code_1(String currency_code_1) {
		this.currency_code_1 = currency_code_1;
	}
	public String getCurrency_code_2() {
		return currency_code_2;
	}
	public void setCurrency_code_2(String currency_code_2) {
		this.currency_code_2 = currency_code_2;
	}
	public BigDecimal getExchange_rate() {
		return exchange_rate;
	}
	public void setExchange_rate(BigDecimal exchange_rate) {
		this.exchange_rate = exchange_rate;
	}
	public String getError_mark() {
		return error_mark;
	}
	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}
	
}
