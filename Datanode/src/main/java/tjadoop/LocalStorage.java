package tjadoop;

import java.io.*;

public class LocalStorage {

  public static synchronized void save(String filename, byte[] bytes, int offset, int len) throws IOException {
    System.out.println("Saving some stuff");
    File file = new File(filename);
    boolean append = true;
    FileOutputStream fout = new FileOutputStream(file, append);
    //fout.write(bytes, offset, len);
    fout.write(bytes, offset, bytes.length);
    fout.close();
  }

  public static synchronized void load(String filename, DataOutputStream dos) throws IOException {
    System.out.println("Loading some stuff");

    // TODO: use piped input/output streams instead?
    File file = new File(filename);
    FileInputStream fin = new FileInputStream(file);
    byte[] buffer = new byte[65536];
    long totalBytesRead = 0;
    long fileSize = file.length();

    while (totalBytesRead < fileSize) {
      long bytesRead = fin.read(buffer);
      totalBytesRead += bytesRead;

      dos.write(buffer, 0, (int) bytesRead);
    }

    fin.close();
  }
}
