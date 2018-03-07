import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import Control.ETL_C_Profile;

public class KevinTest {

	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		  String encodedURL = URLEncoder.encode(ETL_C_Profile.ETL_Download_localPath+"/"+"123"+"/"+"456", "UTF-8");
		  
		  
		  
		encodedURL = URLDecoder.decode(encodedURL, "UTF-8").trim();
	
	System.out.println(encodedURL);
	}

}
