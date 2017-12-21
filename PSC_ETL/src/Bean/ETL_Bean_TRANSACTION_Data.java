package Bean;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_TRANSACTION_Data {

	private String central_no;// 報送單位
	private Date record_date;// 檔案日期
	private String file_type;// 檔名業務別
	private String account_id;// 帳號
	private String transaction_id;//主機交易序號
	private Date transaction_date;//作帳日
	private Timestamp TRANSACTION_TIME;//實際交易時間
	
	
	
	
	
	
	
	
	
	
	private String error_mark = "";// 錯誤註記

	public ETL_Bean_TRANSACTION_Data(ETL_Tool_ParseFileName pfn, String domain_id, String party_number, String change_code,
			String account_id, String branch_code, String account_type_code, String property_code, String currency_code,
			String status_code, String account_opening_channel, Date account_open_date, Date account_close_date,
			String balance_acct_currency_sign, BigDecimal balance_acct_currency_value,
			String balance_last_month_avg_sign, BigDecimal balance_last_month_avg_value, String caution_note) {

		this.central_no = pfn.getCentral_No();// 報送單位
		this.record_date = pfn.getRecord_Date();// 檔案日期
		this.file_type = pfn.getFile_Type();// 檔名業務別
	}

}
