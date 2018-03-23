package Jar_Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import Extract.ETL_E_GUARANTOR;

public class GUARANTOR_K {

	public static void main(String[] args) throws IOException {
		ETL_E_GUARANTOR program = new ETL_E_GUARANTOR();
		List<String> lines = Jar_Test.getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,upload_no, program_no;

		try {
			filePath = lines.get(0);
			fileTypeName = "GUARANTOR";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = lines.get(3);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Guarantor_File(filePath, fileTypeName, batch_no, exc_central_no, new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no,
					program_no);

		} catch (Exception e) {
			e.printStackTrace();
		}
	

	}

}
