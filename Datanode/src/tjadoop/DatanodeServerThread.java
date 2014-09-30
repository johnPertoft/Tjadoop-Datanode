package tjadoop;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DatanodeServerThread implements Runnable {

  private Datanode datanode;
  private Socket socket;
  private DataInputStream dis;
  private DataOutputStream dos;

  public DatanodeServerThread(Datanode datanode, Socket socket) throws IOException {
    this.datanode = datanode;
    this.socket = socket;

    dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
  }

  @Override
  public void run() {

    // implements the protocol defined in TODO

    try {
      byte requestType = dis.readByte();

      switch (requestType) {
        case DatanodeProtocol.CREATE:
          createRequest();
          break;

        case DatanodeProtocol.READ:

          break;

        case DatanodeProtocol.ACK:

          break;

        case DatanodeProtocol.DELETE:

          break;
      }
    } catch (IOException e) {
      // TODO: signal to Datanode that this was a malformed request
      return;
    }


    boolean receiving = true;
    try {
      while (receiving) {
        byte b = dis.readByte();
        System.out.println(b);
      }
    } catch (EOFException e) {

    } catch (IOException e) {
      // TODO: also signal that an error occurred

    }
  }

  private class NodeEntry {
    byte[] iaddr;
    long byteStart;
    long byteEnd;

    NodeEntry(byte[] iaddr, long bs, long be) {
      this.iaddr = iaddr;
      this.byteStart = bs;
      this.byteEnd = be;
    }
  }

  private void createRequest() throws IOException {
    short numNodes = dis.readShort();

    List<NodeEntry> nodeEntries = new LinkedList<NodeEntry>();

    for (int i = 0; i < numNodes; i++) {
      byte[] iaddr = new byte[16];
      dis.read(iaddr);
      long byteStart = dis.readLong();
      long byteEnd = dis.readLong();

      nodeEntries.add(new NodeEntry(iaddr, byteStart, byteEnd));
    }

    long dataLength = dis.readLong();
    long bytesRead = 0;

    while (bytesRead < dataLength) { // TODO: check for EOF too

    }
  }

  private void readRequest() throws IOException {
    // TODO
  }

  private void deleteRequest() throws IOException {
    // TODO
  }

  private void acknowledge() throws IOException {
    // TODO
  }
}
