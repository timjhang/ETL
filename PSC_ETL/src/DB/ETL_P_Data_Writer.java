package DB;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;

import Tool.ETL_Tool_CastObjUtil;

import com.ibm.db2.jcc.am.SqlDataException;

/**
 * 
 * @author Kevin
 *	將資料寫入db2
 */
public class ETL_P_Data_Writer {
	
	public static <T> boolean insertByDefineArrayListObject(List<T> javaBeans, InsertAdapter insertAdapter) {

		boolean isSucess = true;
		int typeArrayLength = insertAdapter.getTypeArrayLength();
		final String sql = insertAdapter.getSql();
		try {
			Connection con = ConnectionHelper.getDB2Connection();
			con.setAutoCommit(false);
			CallableStatement cstmt = con.prepareCall(sql);

			List<Object[]> insertArrs = new ArrayList<Object[]>();

			List<Object[]> allObjectArr = ETL_Tool_CastObjUtil.castObjectArr(javaBeans);

			for (Object[] objArr : allObjectArr) {
				insertArrs.add(objArr);
				try {
					if (insertArrs.size() >= typeArrayLength) {
						Struct[] structArr = new Struct[typeArrayLength];
						for (int i = 0; i < structArr.length; i++) {
							structArr[i] = con.createStruct(insertAdapter.getCreateStructTypeName(), insertArrs.get(i));
						}

						Array array = con.createArrayOf(insertAdapter.getCreateStructTypeName(), structArr);

						cstmt.setArray(1, array);
						cstmt.execute();
						con.commit();
						insertArrs.clear();
					}
				} catch (SqlDataException e) {
					isSucess = false;
					insertArrs.clear();
				}

			}

			try {
				if (insertArrs.size() > 0) {
					Struct[] structArr = new Struct[insertArrs.size()];
					for (int i = 0; i < structArr.length; i++) {
						structArr[i] = con.createStruct(insertAdapter.getCreateStructTypeName(), insertArrs.get(i));
					}

					Array array = con.createArrayOf(insertAdapter.getCreateStructTypeName(), structArr);

					cstmt.setArray(1, array);
					cstmt.execute();
					con.commit();
					insertArrs.clear();
				}

			} catch (SqlDataException e) {
				isSucess = false;
				insertArrs.clear();
			}
			con.setAutoCommit(true);
		} catch (Exception e) {
			e.printStackTrace();
			isSucess = false;
		}

		return isSucess;
	}

}
