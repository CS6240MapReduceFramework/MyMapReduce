package hadoop;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class BytesDivider {
	
	public static void divideBytes(){
		File file1 = new File("abc.txt");
		System.out.println(file1.length());
		int partCounter = 1;

		int sizeOfFiles = 200;
		byte[] buffer = new byte[sizeOfFiles];
		
		try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file1))) {//try-with-resources to ensure closing stream
			String name = file1.getName();
			
			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {
				File newFile = new File(file1.getParent(), name + "." + String.format("%03d", partCounter++));
				try (FileOutputStream out = new FileOutputStream(newFile)) {
					out.write(buffer, 0, tmp);
				}
			}
		} catch (Exception e){
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		divideBytes();
	}
}
