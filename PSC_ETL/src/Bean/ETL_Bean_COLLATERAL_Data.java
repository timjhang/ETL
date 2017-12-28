package Bean;

import java.math.BigDecimal;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_COLLATERAL_Data {

	private String central_no;// 報送單位
	private Date record_date;// 檔案日期
	private String file_type;// 檔名業務別
	private Integer row_count;// 行數
	private String domain_id;// 本會代號
	private String collateral_id;// 擔保品編號
	private String change_code;// 異動代號
	private String loan_master_number;// 批覆書編號/申請書編號
	private String loan_detail_number;// 額度編號
	private String collateral_type;// 擔保品類別
	private BigDecimal collateral_value;// 鑑價金額
	private BigDecimal guarantee_amount;// 擔保金額
	private String collateral_desc;// 擔保品描述
	private String relation_id;// 所有權人統編
	private String relation_name;// 所有權人姓名
	private String relation_type_code;// 與主債務人關係
	private String party_number;// 客戶(主債務人)統編
	private String error_mark = "";// 錯誤註記

	public ETL_Bean_COLLATERAL_Data(ETL_Tool_ParseFileName pfn) {

		this.central_no = pfn.getCentral_No();// 報送單位
		this.record_date = pfn.getRecord_Date();// 檔案日期
		this.file_type = pfn.getFile_Type();// 檔名業務別
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

	public int getRow_count() {
		return row_count;
	}

	public void setRow_count(int row_count) {
		this.row_count = row_count;
	}

	public String getDomain_id() {
		return domain_id;
	}

	public void setDomain_id(String domain_id) {
		this.domain_id = domain_id;
	}

	public String getCollateral_id() {
		return collateral_id;
	}

	public void setCollateral_id(String collateral_id) {
		this.collateral_id = collateral_id;
	}

	public String getChange_code() {
		return change_code;
	}

	public void setChange_code(String change_code) {
		this.change_code = change_code;
	}

	public String getLoan_master_number() {
		return loan_master_number;
	}

	public void setLoan_master_number(String loan_master_number) {
		this.loan_master_number = loan_master_number;
	}

	public String getLoan_detail_number() {
		return loan_detail_number;
	}

	public void setLoan_detail_number(String loan_detail_number) {
		this.loan_detail_number = loan_detail_number;
	}

	public String getCollateral_type() {
		return collateral_type;
	}

	public void setCollateral_type(String collateral_type) {
		this.collateral_type = collateral_type;
	}

	public BigDecimal getCollateral_value() {
		return collateral_value;
	}

	public void setCollateral_value(BigDecimal collateral_value) {
		this.collateral_value = collateral_value;
	}

	public BigDecimal getGuarantee_amount() {
		return guarantee_amount;
	}

	public void setGuarantee_amount(BigDecimal guarantee_amount) {
		this.guarantee_amount = guarantee_amount;
	}

	public String getCollateral_desc() {
		return collateral_desc;
	}

	public void setCollateral_desc(String collateral_desc) {
		this.collateral_desc = collateral_desc;
	}

	public String getRelation_id() {
		return relation_id;
	}

	public void setRelation_id(String relation_id) {
		this.relation_id = relation_id;
	}

	public String getRelation_name() {
		return relation_name;
	}

	public void setRelation_name(String relation_name) {
		this.relation_name = relation_name;
	}

	public String getRelation_type_code() {
		return relation_type_code;
	}

	public void setRelation_type_code(String relation_type_code) {
		this.relation_type_code = relation_type_code;
	}

	public String getParty_number() {
		return party_number;
	}

	public void setParty_number(String party_number) {
		this.party_number = party_number;
	}

	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

}
