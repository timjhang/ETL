import java.util.ArrayList;
import java.util.List;

public class Test2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		System.out.println("123");
		
		List<String> list = new ArrayList<String>();
		list.add("121");
		list.add("122");
		list.add("123");
		list.add("124");
		list.add("125");
		testList(list);

	}
	
	private static void testList(List<String> list) {
		
		for (int i = 0; i < list.size(); i++) {
			System.out.println("size = " + list.size() + ", i = " + i + ", Str = " + list.get(i));
			
			
			if (list.get(i).equals("123")) {
				list.remove(i);
				i--;
			} else if (list.get(i).equals("124")) {
				list.remove(i);
				i--;
			}
		}
		
	}

}
