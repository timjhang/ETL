package Extract;

import java.util.Date;

public abstract class Extract implements Runnable {
	
	protected String filePath;
	protected String fileTypeName;
	protected String batch_no;
	protected String exc_central_no;
	protected Date exc_record_date;
	protected String upload_no;
	protected String program_no;
	
	public Extract() {
		
	}
	
	public Extract(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) {
		
		this.filePath = filePath;
		this.fileTypeName = fileTypeName;
		this.batch_no = batch_no;
		this.exc_central_no = exc_central_no;
		this.exc_record_date = exc_record_date;
		this.upload_no = upload_no;
		this.program_no = program_no;
	}
	
	abstract void read_File();
	
	@Override
	public void run() {
		read_File();
	}

	
//	public String getFilePath() {
//		return filePath;
//	}
//
//	public void setFilePath(String filePath) {
//		this.filePath = filePath;
//	}
//
//	public String getFileTypeName() {
//		return fileTypeName;
//	}
//
//	public void setFileTypeName(String fileTypeName) {
//		this.fileTypeName = fileTypeName;
//	}
//
//	public String getBatch_no() {
//		return batch_no;
//	}
//
//	public void setBatch_no(String batch_no) {
//		this.batch_no = batch_no;
//	}
//
//	public String getExc_central_no() {
//		return exc_central_no;
//	}
//
//	public void setExc_central_no(String exc_central_no) {
//		this.exc_central_no = exc_central_no;
//	}
//
//	public Date getExc_record_date() {
//		return exc_record_date;
//	}
//
//	public void setExc_record_date(Date exc_record_date) {
//		this.exc_record_date = exc_record_date;
//	}
//
//	public String getUpload_no() {
//		return upload_no;
//	}
//
//	public void setUpload_no(String upload_no) {
//		this.upload_no = upload_no;
//	}
//
//	public String getProgram_no() {
//		return program_no;
//	}
//
//	public void setProgram_no(String program_no) {
//		this.program_no = program_no;
//	}
	
}
