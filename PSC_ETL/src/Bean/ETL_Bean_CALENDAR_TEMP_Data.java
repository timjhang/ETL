package Bean;

import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_CALENDAR_TEMP_Data {

	//報送單位			
	private String central_no;
	//檔案日期			
	private Date record_date;
	//檔名業務別			
	private String file_type;
	//日期				
	private Date calendar_day;
	//是否為營業日		
	private String is_business_day;
	//錯誤註記			
	private String error_mark = "";	
	
	public ETL_Bean_CALENDAR_TEMP_Data(ETL_Tool_ParseFileName pfn, Date calendar_day, String is_business_day, String error_mark) {
		this.central_no = pfn.getCentral_No();
		this.record_date = pfn.getRecord_Date();
		this.file_type = pfn.getFile_Type();
		this.calendar_day = calendar_day;
		this.is_business_day = is_business_day;
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
	public Date getCalendar_day() {
		return calendar_day;
	}
	public void setCalendar_day(Date calendar_day) {
		this.calendar_day = calendar_day;
	}
	public String getIs_business_day() {
		return is_business_day;
	}
	public void setIs_business_day(String is_business_day) {
		this.is_business_day = is_business_day;
	}
	public String getError_mark() {
		return error_mark;
	}
	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}
	
}
