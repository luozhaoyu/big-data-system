package storm.starter.util;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;


public class MyLog {
	static public void MyPrint(String msg, String filePath) {
        try {
        	File file = new File(filePath);
        	if (!file.exists()) {
        		file.createNewFile();
        	}
        	FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
        	BufferedWriter bw = new BufferedWriter(fw);
			bw.write(new Date().getTime() + "\t" + msg + "\n");
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
