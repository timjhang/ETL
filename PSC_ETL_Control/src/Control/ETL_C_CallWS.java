package Control;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import Bean.ETL_Bean_Response;

public class ETL_C_CallWS {
	
	// 呼叫ETL Server getUploadFile, 並取得下載檔案資訊
	public static ETL_Bean_Response call_ETL_Server_getUploadFileInfo(String ip_port, String centralNo) {
		ETL_Bean_Response response = new ETL_Bean_Response();
		String[] fileInfoAry = new String[3];
	
		try {
//				URL url = new URL("http://localhost:8083/AML_ETL/rest/getUploadFile/WS1");
			System.out.println("call_ETL_Server_Efunction : 開始執行");

			String urlStr = "http://" + ip_port + "/AML_ETL/rest/getUploadFile/WS1?";
			urlStr = urlStr + "centralNo=" + centralNo;
			
			System.out.println("urlStr = " + urlStr);
			URL url = new URL(urlStr);
			
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
//				conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Accept", "application/xml");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			System.out.println("Output from Server .... \n");
//				String output;
//				while ((output = br.readLine()) != null) {
//					System.out.println(output);
//				}
			
			boolean exeReault = false;
			
			String outputStr; // WebService輸出字串  XML格式
			while ((outputStr = br.readLine()) != null) {
				InputStream is = new ByteArrayInputStream(outputStr.getBytes("UTF-8"));
				SAXReader reader = new SAXReader();
				Document document = reader.read(is);
				Element root = document.getRootElement();
				
				Element msg = root.element("msg");
				if (msg != null) {
					String msgText = msg.getTextTrim();
					System.out.println("msg = " + msgText);
					
					if ("SUCCESS".equals(msgText)) {
						Element fileInfo = root.element("fileInfo");
						try {
							String fileInfoStr = fileInfo.getTextTrim();
							fileInfoAry = fileInfoStr.split("\\|");
						} catch (Exception ex) {
							fileInfoAry = null;
							ex.printStackTrace();
							
							exeReault = false;
						}
						
						exeReault = true;
					} else if ("Exception".equals(msgText)) {
						
						String errorMessage = "";
						Element errorMsg = root.element("errorMsg");
						if (errorMsg != null) {
							String errorMsgText = errorMsg.getTextTrim();
							errorMessage = "發生錯誤:" + errorMsgText;
						}
						System.out.println(errorMessage);
						
						exeReault = false;
					};
					
				} else {
					System.out.println("root has no element names msg");
				}
				
				List<Element> logs = root.elements("logs");
//					System.out.println(msg.getTextTrim());
//					List<Element> logs = msg.elements("logs");
				if (logs != null) {
					for (int i = 0; i < logs.size(); i++) {
						String logText = logs.get(i).getTextTrim();
						System.out.println("logs = " + logText);
					}
				} else {
					System.out.println("root has no element names logs");
				}
				
				Element errorMsg = root.element("errorMsg");
				if (errorMsg != null) {
					String errorMsgText = errorMsg.getTextTrim();
					System.out.println("errorMsg = " + errorMsgText);
				} else {
					System.out.println("root has no element names errorMsg");
				}
			}
			
			conn.disconnect();
			
			System.out.println("call_ETL_Server_Efunction : 執行成功！");
			
			
			//TODO FOR TEST
			for(int i=0;i<fileInfoAry.length;i++) {
				System.out.println("####call_ETL_Server_getUploadFileInfo fileInfo"+i+" = "+fileInfoAry[i]);
			}
			
			//TODO FOR 依原本邏輯加工
			if(exeReault) {
				response.setSuccessObj(fileInfoAry);
			}

			return response;

		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Efunction : 發生錯誤");
			return response;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Efunction : 發生錯誤");
			return response;
		} catch (DocumentException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Efunction : 發生錯誤");
			return response;
		}
		
	}
	
	// 呼叫ETL Server Efunction
	public static boolean call_ETL_Server_Efunction(String ip_port, String filePath, 
			String batch_No, String exc_central_no, String record_DateStr, String upload_No) {
		
		try {
//			URL url = new URL("http://localhost:8083/AML_ETL/rest/Efunction/WS1");
			System.out.println("call_ETL_Server_Efunction : 開始執行");

//			filePath = ""; // ETL Server下載檔案位置路徑 encode碼 (D:/ETL/DB)  TODO ?? 單位 批號 
			//filePath = "C%3A%2Ftest2%2F600%2F001"; // for test

			//TODO  upload_No???
			filePath = ETL_C_Profile.ETL_Download_localPath + exc_central_no + "/" + record_DateStr + "/" + upload_No;
			URLEncoder.encode(filePath, "UTF-8");
			
			String urlStr = "http://" + ip_port + "/AML_ETL/rest/Efunction/WS1?";
			urlStr = urlStr + "filePath=" + filePath;
			urlStr = urlStr + "&batch_no=" + batch_No;
			urlStr = urlStr + "&exc_central_no=" + exc_central_no;
			urlStr = urlStr + "&exc_record_date=" + record_DateStr;
			urlStr = urlStr + "&upload_no=" + upload_No;
			
			System.out.println("urlStr = " + urlStr);
			URL url = new URL(urlStr);
			
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
//			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Accept", "application/xml");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			System.out.println("Output from Server .... \n");
//			String output;
//			while ((output = br.readLine()) != null) {
//				System.out.println(output);
//			}
			
			boolean exeReault = false;
			
			String outputStr; // WebService輸出字串  XML格式
			while ((outputStr = br.readLine()) != null) {
				InputStream is = new ByteArrayInputStream(outputStr.getBytes("UTF-8"));
				SAXReader reader = new SAXReader();
				Document document = reader.read(is);
				Element root = document.getRootElement();
				
				Element msg = root.element("msg");
				if (msg != null) {
					String msgText = msg.getTextTrim();
					System.out.println("msg = " + msgText);
					
					if ("SUCCESS".equals(msgText)) {
						
						exeReault = true;
					} else if ("Exception".equals(msgText)) {
						
						String errorMessage = "";
						Element errorMsg = root.element("errorMsg");
						if (errorMsg != null) {
							String errorMsgText = errorMsg.getTextTrim();
							errorMessage = "發生錯誤:" + errorMsgText;
						}
						System.out.println(errorMessage);
						
						exeReault = false;
					};
					
				} else {
					System.out.println("root has no element names msg");
				}
				
				List<Element> logs = root.elements("logs");
//				System.out.println(msg.getTextTrim());
//				List<Element> logs = msg.elements("logs");
				if (logs != null) {
					for (int i = 0; i < logs.size(); i++) {
						String logText = logs.get(i).getTextTrim();
						System.out.println("logs = " + logText);
					}
				} else {
					System.out.println("root has no element names logs");
				}
				
				Element errorMsg = root.element("errorMsg");
				if (errorMsg != null) {
					String errorMsgText = errorMsg.getTextTrim();
					System.out.println("errorMsg = " + errorMsgText);
				} else {
					System.out.println("root has no element names errorMsg");
				}
			}
			
			conn.disconnect();
			
			System.out.println("call_ETL_Server_Efunction : 執行成功！");
			
			return exeReault;

		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Efunction : 發生錯誤");
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Efunction : 發生錯誤");
			return false;
		} catch (DocumentException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Efunction : 發生錯誤");
			return false;
		}
		
	}
	
	// 呼叫ETL Server Tfunction
	public static boolean call_ETL_Server_Tfunction(String ip_port, String filePath, 
			String batch_No, String exc_central_no, String record_DateStr, String upload_No) {
		
		try {
//				URL url = new URL("http://localhost:8083/AML_ETL/rest/Tfunction/WS1");
			System.out.println("call_ETL_Server_Tfunction : 開始執行");

//				filePath = ""; // ETL Server下載檔案位置路徑 encode碼 (D:/ETL/DB)
			filePath = "C%3A%2Ftest2%2F600%2F001"; // for test
			
			String urlStr = "http://" + ip_port + "/AML_ETL/rest/Tfunction/WS1?";
			urlStr = urlStr + "filePath=" + filePath;
			urlStr = urlStr + "&batch_no=" + batch_No;
			urlStr = urlStr + "&exc_central_no=" + exc_central_no;
			urlStr = urlStr + "&exc_record_date=" + record_DateStr;
			urlStr = urlStr + "&upload_no=" + upload_No;
			
			System.out.println("urlStr = " + urlStr);
			URL url = new URL(urlStr);
			
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
//				conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Accept", "application/xml");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			System.out.println("Output from Server .... \n");
//				String output;
//				while ((output = br.readLine()) != null) {
//					System.out.println(output);
//				}
			
			boolean exeReault = false;
			
			String outputStr; // WebService輸出字串  XML格式
			while ((outputStr = br.readLine()) != null) {
				InputStream is = new ByteArrayInputStream(outputStr.getBytes("UTF-8"));
				SAXReader reader = new SAXReader();
				Document document = reader.read(is);
				Element root = document.getRootElement();
				
				Element msg = root.element("msg");
				if (msg != null) {
					String msgText = msg.getTextTrim();
					System.out.println("msg = " + msgText);
					
					if ("SUCCESS".equals(msgText)) {
						
						exeReault = true;
					} else if ("Exception".equals(msgText)) {
						
						String errorMessage = "";
						Element errorMsg = root.element("errorMsg");
						if (errorMsg != null) {
							String errorMsgText = errorMsg.getTextTrim();
							errorMessage = "發生錯誤:" + errorMsgText;
						}
						System.out.println(errorMessage);
						
						exeReault = false;
					};
					
				} else {
					System.out.println("root has no element names msg");
				}
				
				List<Element> logs = root.elements("logs");
//					System.out.println(msg.getTextTrim());
//					List<Element> logs = msg.elements("logs");
				if (logs != null) {
					for (int i = 0; i < logs.size(); i++) {
						String logText = logs.get(i).getTextTrim();
						System.out.println("logs = " + logText);
					}
				} else {
					System.out.println("root has no element names logs");
				}
				
				Element errorMsg = root.element("errorMsg");
				if (errorMsg != null) {
					String errorMsgText = errorMsg.getTextTrim();
					System.out.println("errorMsg = " + errorMsgText);
				} else {
					System.out.println("root has no element names errorMsg");
				}
			}
			
			conn.disconnect();
			
			System.out.println("call_ETL_Server_Tfunction : 執行成功！");
			
			return exeReault;

		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Tfunction : 發生錯誤");
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Tfunction : 發生錯誤");
			return false;
		} catch (DocumentException e) {
			e.printStackTrace();
			System.out.println("call_ETL_Server_Tfunction : 發生錯誤");
			return false;
		}
		
	}
	
	public static void main(String[] argv) throws DocumentException {
		try {

			URL url = new URL("http://localhost:8083/AML_ETL/rest/Efunction/WS1");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
//			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Accept", "application/xml");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			System.out.println("Output from Server .... \n");
//			String output;
//			while ((output = br.readLine()) != null) {
//				System.out.println(output);
//			}
			
			String outputStr; // WebService輸出字串  XML格式
			while ((outputStr = br.readLine()) != null) {
				InputStream is = new ByteArrayInputStream(outputStr.getBytes("UTF-8"));
				SAXReader reader = new SAXReader();
				Document document = reader.read(is);
				Element root = document.getRootElement();
				
				Element msg = root.element("msg");
				if (msg != null) {
					String msgText = msg.getTextTrim();
					System.out.println("msg = " + msgText);
				} else {
					System.out.println("root has no element names msg");
				}
				
				List<Element> logs = root.elements("logs");
//				System.out.println(msg.getTextTrim());
//				List<Element> logs = msg.elements("logs");
				if (logs != null) {
					for (int i = 0; i < logs.size(); i++) {
						String logText = logs.get(i).getTextTrim();
						System.out.println("logs = " + logText);
					}
				} else {
					System.out.println("root has no element names logs");
				}
				
				Element errorMsg = root.element("errorMsg");
				if (errorMsg != null) {
					String errorMsgText = errorMsg.getTextTrim();
					System.out.println("errorMsg = " + errorMsgText);
				} else {
					System.out.println("root has no element names errorMsg");
				}
			}
			
			conn.disconnect();

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
