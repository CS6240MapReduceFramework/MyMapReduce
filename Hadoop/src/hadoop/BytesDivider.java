package hadoop;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class BytesDivider {
	public static int partCounter = 1;
	public static int sizeOfFiles = 8192;
	public static byte[] buffer = new byte[sizeOfFiles];
	
	public static String getFileExtension(String fileName){
		return fileName.substring(fileName.lastIndexOf(".") + 1);
	}
	
	public static void writeToFiles(BufferedInputStream bis, String fileName) throws IOException {
		int tmp = 0;
		while ((tmp = bis.read(buffer)) > 0) {
			File newFile = new File(String.format("%03d", partCounter++) + "-"+ fileName);
			try (FileOutputStream out = new FileOutputStream(newFile)) {
				out.write(buffer, 0, tmp);
			}
		}
	}
	
	public static void handleCompressedFile(File file1) throws IOException {
		GZIPInputStream gis = new GZIPInputStream(new FileInputStream(file1));
		BufferedInputStream bis = new BufferedInputStream(gis);
		
		String name = file1.getName();
		String fileName = name.substring(0, name.lastIndexOf("."));
		writeToFiles(bis, fileName);
		
		bis.close();
		gis.close();
	}
	
	public static void handleRegularFile(File file1) throws IOException {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file1));
		writeToFiles(bis, file1.getName());
		bis.close();
	}
	
	public static void divideFile(File file1){
		try{
			String fileExtension = getFileExtension(file1.getName());
			if(fileExtension.equals("gz")){
				handleCompressedFile(file1);
			} else {
				handleRegularFile(file1);
			}
		} catch (Exception e){
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		File file1 = new File("551.csv.gz");
		divideFile(file1);
	}
}
