import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import Extract.ETL_E_ACCOUNT;
import Extract.ETL_E_COLLATERAL;
import Extract.ETL_E_LOAN;
import Extract.ETL_E_LOAN_DETAIL;
import Extract.ETL_E_PARTY_ADDRESS;
import Extract.ETL_E_TRANSACTION;

public class Test {

	public static void main(String[] argv) throws IOException {
		LOAN_DETAIL();
	}

	public static void PARTY_ADDRESS() throws IOException {
		ETL_E_PARTY_ADDRESS program = new ETL_E_PARTY_ADDRESS();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "PARTY_ADDRESS";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Party_Address_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,
					upload_no, program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void ACCOUNT() throws IOException {
		ETL_E_ACCOUNT program = new ETL_E_ACCOUNT();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "ACCOUNT";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Account_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void TRANSACTION() throws IOException {
		ETL_E_TRANSACTION program = new ETL_E_TRANSACTION();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "TRANSACTION";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Transaction_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void LOAN_DETAIL() throws IOException {
		ETL_E_LOAN_DETAIL program = new ETL_E_LOAN_DETAIL();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "LOAN_DETAIL";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Loan_Detail_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void LOAN() throws IOException {
		ETL_E_LOAN program = new ETL_E_LOAN();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "LOAN";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Loan_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void COLLATERAL() throws IOException {
		ETL_E_COLLATERAL program = new ETL_E_COLLATERAL();
		List<String> lines = getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, upload_no, program_no;
		Date exc_record_date = null;

		try {
			filePath = lines.get(0);
			fileTypeName = "COLLATERAL";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(lines.get(3));
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Collateral_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<String> getProperties(String path) throws IOException {
		Charset charset = Charset.forName("Big5");
		List<String> lines = Files.readAllLines(Paths.get(path), charset);
		return lines;
	}
}
