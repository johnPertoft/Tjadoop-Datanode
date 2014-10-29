package tjadoop;

import java.io.*;

public class LocalStorage {

  public static final String RELATIVE_FILEPARTS_PATH = "fileparts/";

  public static synchronized void save(String filename, byte[] bytes, int offset, int len) throws IOException {
    //System.out.println("saving stuff, offset: " + offset + ", len: " + len);

    if (len <= 0) return;

    File file = new File(RELATIVE_FILEPARTS_PATH + filename);
    boolean append = true;
    FileOutputStream fout = new FileOutputStream(file, append);
    fout.write(bytes, offset, len);
    fout.close();
  }

  public static synchronized void load(String filename, DataOutputStream dos) throws IOException {
    // TODO: use piped input/output streams instead?
    File file = new File(RELATIVE_FILEPARTS_PATH + filename);
    System.out.println("loading filepart: " + file);
    FileInputStream fin = new FileInputStream(file);
    byte[] buffer = new byte[1024 * 1024];
    long totalBytesRead = 0;
    long fileSize = file.length();

    while (totalBytesRead < fileSize) {
      long bytesRead = fin.read(buffer);
      totalBytesRead += bytesRead;
      dos.write(buffer, 0, (int) bytesRead);
      dos.flush();
    }

    fin.close();
  }

  public static synchronized void delete(int fileHash) throws IOException {
    File directory = new File(RELATIVE_FILEPARTS_PATH);

    for (File f : directory.listFiles()) {
      if (f.getName().startsWith("Filepart-" + fileHash)) {
        System.out.println("deleting: " + f);
        f.delete();
      }
    }
  }

  public static synchronized void deleteEverything() throws IOException {
    File directory = new File(RELATIVE_FILEPARTS_PATH);

    for (File f : directory.listFiles()) {
      f.delete();
    }
  }

  public static synchronized String getFilename(int fileHash, long byteStart, long byteEnd) {
    return "Filepart-" + fileHash + "-" + byteStart + "-" + byteEnd;
  }
}

