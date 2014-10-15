package tjadoop;

import java.io.*;

public class LocalStorage {

  public static synchronized void save(String filename, byte[] bytes, int offset, int len) throws IOException {
    File file = new File(filename);
    boolean append = true;
    FileOutputStream fout = new FileOutputStream(file, append);
    fout.write(bytes, offset, len);
    //fout.write(bytes, offset, bytes.length);
    fout.close();
  }

  public static synchronized void load(String filename, DataOutputStream dos) throws IOException {
    System.out.println("loading some stuff");
    // TODO: use piped input/output streams instead?
    File file = new File(filename);
    FileInputStream fin = new FileInputStream(file);
    byte[] buffer = new byte[1024*1024*32];
    long totalBytesRead = 0;
    long fileSize = file.length();

    while (totalBytesRead < fileSize) {
      long bytesRead = fin.read(buffer);
      totalBytesRead += bytesRead;

      dos.write(buffer, 0, (int) bytesRead);
    }

    fin.close();
  }

  public static synchronized void delete(String filename) throws IOException {
    // TODO:
  }
}
