package tjadoop;

import java.io.*;
import java.net.InetAddress;
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
      int fileHash = dis.readInt();

      switch (requestType) {
        case DatanodeProtocol.CREATE:
          createRequest(fileHash);
          break;

        case DatanodeProtocol.READ:
          readRequest();
          break;

        case DatanodeProtocol.ACK:

          break;

        case DatanodeProtocol.DELETE:

          break;
      }
    } catch (IOException e) {
      // TODO: signal to Datanode that this was a malformed request or whatever happened
      return;
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

  // TODO: this could need some refactoring maybe
  private void createRequest(int fileHash) throws IOException {
    short numNodes = dis.readShort();

    List<NodeEntry> nodeEntries = new LinkedList<NodeEntry>();

    // the byte start and end for this datanode
    long byteStart = 0;
    long byteEnd = 0;

    for (int i = 0; i < numNodes; i++) {
      byte[] iaddr = new byte[16];
      dis.read(iaddr);
      long bs = dis.readLong();
      long be = dis.readLong();

      if (isThisNodesIP(iaddr)) {
        byteStart = bs;
        byteEnd = be;

      } else {
        nodeEntries.add(new NodeEntry(iaddr, bs, be));
      }
    }

    byte[] iaddr = nodeEntries.get(0).iaddr;
    Socket nextNodeSocket = new Socket(InetAddress.getByAddress(iaddr), Datanode.PORT);
    DataInputStream nextNodeDis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    DataOutputStream nextNodeDos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

    // write to next node
    nextNodeDos.writeByte(DatanodeProtocol.CREATE);
    nextNodeDos.writeShort(nodeEntries.size());
    for (NodeEntry ne : nodeEntries) {
      nextNodeDos.write(ne.iaddr, 0, ne.iaddr.length);
      nextNodeDos.writeLong(ne.byteStart);
      nextNodeDos.writeLong(ne.byteEnd);
    }

    long dataLength = dis.readLong();
    nextNodeDos.writeLong(dataLength);
    long totalBytesRead = 0;
    byte[] byteBlock = new byte[65536];
    boolean savedLocalData = false;

    if (byteStart > byteEnd) {
      // TODO: throw some exception?
    }

    String filename = "TODO";

    while (totalBytesRead < dataLength) { // TODO: check for EOF too
      int bytesRead = dis.read(byteBlock);
      totalBytesRead += bytesRead;

      long currByteEnd = totalBytesRead;
      long currByteStart = totalBytesRead - bytesRead + 1;

      // if byteStart starts in this block
      if (byteStart >= currByteStart && byteStart <= currByteEnd) {
        int blockStart =  (int) (byteStart - currByteStart);

        if (byteEnd > currByteEnd) {
          int len = byteBlock.length - blockStart;
          LocalStorage.save(filename, byteBlock, blockStart, len);

        } else {
          int len = (int) (currByteEnd - currByteStart);
          LocalStorage.save(filename, byteBlock, blockStart, len);
        }
      }

      // if byteEnd is in this block, should always come after the previous if
      if (byteEnd >= currByteStart && byteStart <= currByteEnd) {
        int len = (int) (byteEnd - currByteStart);
        LocalStorage.save(filename, byteBlock, 0, len);
      }

      // if this whole block is within bytestart and byteend
      if (byteStart < currByteStart && byteEnd > currByteEnd) {
        LocalStorage.save(filename, byteBlock, 0, byteBlock.length);
      }

      // always pass it on to the next datanode
      nextNodeDos.write(byteBlock);
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

  private boolean isThisNodesIP(byte[] iaddr) {
    if (iaddr.length != datanode.IADDRESS.length) {
      return false;
    }

    for (int i = 0; i < iaddr.length; i++) {
      if (iaddr[i] != datanode.IADDRESS[i]) {
        return false;
      }
    }

    return true;
  }
}
