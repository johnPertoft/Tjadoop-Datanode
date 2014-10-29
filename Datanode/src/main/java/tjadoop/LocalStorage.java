package tjadoop;

import java.io.*;

public class LocalStorage {

  public static final String RELATIVE_FILEPARTS_PATH = "fileparts/";

  public static synchronized void save(String filename, byte[] bytes, int offset, int len) throws IOException {
    System.out.println("saving stuff, offset: " + offset + ", len: " + len);

    if (len <= 0) return;

    File file = new File(RELATIVE_FILEPARTS_PATH + filename);
    boolean append = true;
    FileOutputStream fout = new FileOutputStream(file, append);
    fout.write(bytes, offset, len);
    fout.close();
  }

  public static synchronized void load(String filename, DataOutputStream dos) throws IOException {
    System.out.println("loading some stuff");
    // TODO: use piped input/output streams instead?
    File file = new File(RELATIVE_FILEPARTS_PATH + filename);
    FileInputStream fin = new FileInputStream(file);
    byte[] buffer = new byte[1024 * 1024 * 32];
    long totalBytesRead = 0;
    long fileSize = file.length();

    while (totalBytesRead < fileSize) {
      long bytesRead = fin.read(buffer);
      totalBytesRead += bytesRead;

      dos.write(buffer, 0, (int) bytesRead);
    }

    fin.close();
  }

  public static synchronized void delete(int fileHash) throws IOException {
    System.out.println("deleting a file");

    File directory = new File(RELATIVE_FILEPARTS_PATH);

    for (File f : directory.listFiles()) {
      if (f.getName().startsWith("" + fileHash)) {
        f.delete();
      }
    }
  }

  public static synchronized String getFilename(int fileHash, long byteStart, long byteEnd) {
    return "Filepart" + fileHash + "-" + byteStart + "-" + byteEnd;
  }
}

