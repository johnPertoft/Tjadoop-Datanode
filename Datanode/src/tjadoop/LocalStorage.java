package tjadoop;

import java.io.*;

public class LocalStorage {

  public static synchronized void save(String filename, byte[] bytes, int offset, int len) throws IOException {
    File file = new File(filename);
    boolean append = true;
    FileOutputStream fout = new FileOutputStream(file, append);
    fout.write(bytes, offset, len);
    fout.close();
  }
}
