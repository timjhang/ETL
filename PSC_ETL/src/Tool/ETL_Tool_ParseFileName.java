package Tool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_Tool_ParseFileName {
	// 拆解檔案名工具

	// 檔案名
	private String FileName;
	// 報送單位
	private String Central_No;
	// 業務名稱
	private String File_Type;
	// 日期
	private Date Record_Date;
	
	// ETL_Tool_ParseFileName's Constructor
	public ETL_Tool_ParseFileName(String fileName) throws Exception {
		if (fileName == null || "".equals(fileName)) {
			throw new Exception("ETL_Tool_ParseFileName 建立檔名有誤!!");
		}
		
		this.FileName = fileName;
		
		String[] ary = FileName.split("\\_");
		this.Central_No = ary[0];
		this.File_Type = ary[1];
		String source = fileName.split("\\.")[0];
		source = source.substring(source.length() - 8, source.length());
		this.Record_Date = new SimpleDateFormat("yyyyMMdd").parse(source);
	}
	
	public String getFileName() {
		return FileName;
	}
	
//	public void setFileName(String fileName) {
//		FileName = fileName;
//	}
	
	public String getCentral_No() {
		return Central_No;
	}
	
//	public void setCentral_No(String central_No) {
//		Central_No = central_No;
//	}
	
	public String getFile_Type() {
		return File_Type;
	}
	
//	public void setFile_Type(String file_Type) {
//		File_Type = file_Type;
//	}
	
	public Date getRecord_Date() {
		return Record_Date;
	}
	
//	public void setRecord_Date(Date record_Date) {
//		Record_Date = record_Date;
//	}
	
}
