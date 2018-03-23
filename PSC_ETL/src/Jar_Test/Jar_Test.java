package Jar_Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;

import Extract.ETL_E_CALENDAR;
import Extract.ETL_E_FCX;
import Extract.ETL_E_FX_RATE;
import Extract.ETL_E_GUARANTOR;
import Extract.ETL_E_SERVICE;
import Extract.ETL_E_TRANSFER;

public class Jar_Test {

	public static void main(String[] argv) throws IOException {
		ETL_E_SERVICE();
	}

	public static void ETL_E_FCX() throws IOException {
		ETL_E_FCX program = new ETL_E_FCX();
		List<String> lines = Jar_Test.getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,upload_no, program_no;

		try {
			filePath = lines.get(0);
			fileTypeName = "FCX";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = lines.get(3);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_FCX_File(filePath, fileTypeName, batch_no, exc_central_no,new SimpleDateFormat("yyyyMMdd").parse(exc_record_date) , upload_no,
					program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void ETL_E_CALENDAR() throws IOException {
		ETL_E_CALENDAR program = new ETL_E_CALENDAR();
		List<String> lines = Jar_Test.getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,upload_no, program_no;

		try {
			filePath = lines.get(0);
			fileTypeName = "CALENDAR";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = lines.get(3);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_CALENDAR_File(filePath, fileTypeName, batch_no, exc_central_no,new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no,
					program_no);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void ETL_E_FX_RATE() throws IOException {
		ETL_E_FX_RATE program = new ETL_E_FX_RATE();
		List<String> lines = Jar_Test.getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,upload_no, program_no;
		

		try {
			filePath = lines.get(0);
			fileTypeName = "FX_RATE";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = lines.get(3);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Fx_Rate_File(filePath, fileTypeName, batch_no, exc_central_no, new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no,
					program_no);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void ETL_E_GUARANTOR() throws IOException {
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

	public static void ETL_E_SERVICE() throws IOException {
		ETL_E_SERVICE program = new ETL_E_SERVICE();
		List<String> lines = Jar_Test.getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,upload_no, program_no;

		try {
			filePath = lines.get(0);
			fileTypeName = "SERVICE";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = lines.get(3);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			program.read_Service_File(filePath, fileTypeName, batch_no, exc_central_no, new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no,
					program_no);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void ETL_E_TRANSFER() throws IOException {
		ETL_E_TRANSFER program = new ETL_E_TRANSFER();
		List<String> lines = Jar_Test.getProperties("C:\\ETL\\properties.txt");
		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,upload_no, program_no;

		try {
			filePath = lines.get(0);
			fileTypeName = "TRANSFER";
			batch_no = lines.get(1);
			exc_central_no = lines.get(2);
			exc_record_date = lines.get(3);
			upload_no = lines.get(4);
			program_no = lines.get(5);
			
			
			program.read_Transfer_File(filePath, fileTypeName, batch_no, exc_central_no, new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no,
					program_no);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
//	public static void checkProperties(List<String> lines) {
//		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date,upload_no, program_no;
//		filePath = lines.get(0);
//		batch_no = lines.get(1);
//		exc_central_no = lines.get(2);
//		exc_record_date = lines.get(3);
//		upload_no = lines.get(4);
//		program_no = lines.get(5);
//		
//		if() {
//			
//		}
//		
//	}

	public static List<String> getProperties(String path) throws IOException {
		Charset charset = Charset.forName("Big5");
		List<String> lines = Files.readAllLines(Paths.get(path), charset);
		return lines;
	}
}
