import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TEST_PATRTY_PHONE {

	
	
	private static final String INSERT_SQL =    "INSERT INTO PARTY_PHONE_TEMP"
            +"( CENTRAL_NO,RECORD_DATE,FILE_TYPE,DOMAIN_ID,PARTY_NUMBER,CHANGE_CODE,PHONE_TYPE,PHONE_NUMBER,ERROR_MARK)"
              +" VALUES('600',NULL,'9','9','TESTTEST','9','9',?,'Y')";

	   public static void main(String[] args) {
	      String jdbcUrl = "jdbc:mysql://localhost:3306/BORAJI";
	      String username = "root";
	      String password = "admin";
	      try (
	    		  Connection conn = DriverManager.getConnection(jdbcUrl, username, password);) {

	         conn.setAutoCommit(false);
	         try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL);) {

	            // Insert sample records
	            for (int i = 0; i < 100000; i++) {
	               stmt.setString(1, "Java");
	               stmt.setString(2, "Sunil Singh");
	               
	               //Add statement to batch
	               stmt.addBatch();
	            }
	            //execute batch
	            stmt.executeBatch();
	            conn.commit();
	            System.out.println("Transaction is commited successfully.");
	         } catch (SQLException e) {
	            e.printStackTrace();
	            if (conn != null) {
	               try {
	                  System.out.println("Transaction is being rolled back.");
	                  conn.rollback();
	               } catch (Exception ex) {
	                  ex.printStackTrace();
	               }
	            }
	         }
	      } catch (SQLException e) {
	         e.printStackTrace();
	      }
	   }
	
}
