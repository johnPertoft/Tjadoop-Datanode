package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.MalformedInputException;
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

    System.out.println("running datanode server thread");
    // implements the protocol defined in TODO

    try {
      byte requestType = dis.readByte();
      int sequenceNumber;
      int fileHash;

      switch (requestType) {
        case DatanodeProtocol.CREATE:
          sequenceNumber = dis.readInt();
          fileHash = dis.readInt();
          createRequest(sequenceNumber, fileHash);
          break;

        case DatanodeProtocol.READ:
          sequenceNumber = dis.readInt();
          fileHash = dis.readInt();
          readRequest(sequenceNumber, fileHash);
          break;

        case DatanodeProtocol.ACK:

          break;

        case DatanodeProtocol.DELETE:
          sequenceNumber = dis.readInt();
          fileHash = dis.readInt();
          break;
      }
    } catch (IOException e) {
      // TODO: signal to Datanode that this was a malformed request or whatever happened
      return;
    } catch (JSONException e) {
      // json exception?
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
  private void createRequest(int sequenceNumber, int fileHash) throws IOException, JSONException {
    int numNodes = dis.readInt();

    List<NodeEntry> nodeEntries = new LinkedList<NodeEntry>();

    // the byte start and end for this datanode
    long byteStart = 0;
    long byteEnd = 0;

    // read header
    for (int i = 0; i < numNodes; i++) {
      byte[] iaddr = new byte[16];
      dis.read(iaddr);
      long bs = dis.readLong();
      long be = dis.readLong();

      // TODO: can have multiple bytestart and byteend for several file parts! FIX!
      if (isOwnIP(iaddr)) {
        byteStart = bs;
        byteEnd = be;

      } else {
        nodeEntries.add(new NodeEntry(iaddr, bs, be));
      }
    }

    long dataLength = dis.readLong();

    boolean isLastNode = nodeEntries.isEmpty();

    Socket nextNodeSocket = null;
    DataInputStream nextNodeDis = null;
    DataOutputStream nextNodeDos = null;

    if (!isLastNode) {
      byte[] iaddr = nodeEntries.get(0).iaddr;
      nextNodeSocket = new Socket(InetAddress.getByAddress(iaddr), Datanode.PORT);
      nextNodeDis = new DataInputStream(new BufferedInputStream(nextNodeSocket.getInputStream()));
      nextNodeDos = new DataOutputStream(new BufferedOutputStream(nextNodeSocket.getOutputStream()));

      // write header
      nextNodeDos.writeByte(DatanodeProtocol.CREATE);
      nextNodeDos.writeInt(sequenceNumber + 1);
      nextNodeDos.writeInt(fileHash);
      nextNodeDos.writeShort(nodeEntries.size());
      for (NodeEntry ne : nodeEntries) {
        nextNodeDos.write(ne.iaddr, 0, ne.iaddr.length);
        nextNodeDos.writeLong(ne.byteStart);
        nextNodeDos.writeLong(ne.byteEnd);
      }

      nextNodeDos.writeLong(dataLength);
    }

    if (byteStart > byteEnd) {
      throw new IllegalArgumentException("Malformed request");
    }

    long totalBytesRead = 0;
    byte[] byteBlock = new byte[1024*1024*32];

    String filename = getFilename(fileHash, byteStart, byteEnd);

    // TODO: this whole block could use some refactoring
    while (totalBytesRead < dataLength) { // TODO: check for EOF too
      int bytesRead = dis.read(byteBlock);
      System.out.println("Bytes read: " + bytesRead);
      totalBytesRead += bytesRead;

      long currByteEnd = totalBytesRead;
      long currByteStart = totalBytesRead - bytesRead;

      System.out.println(currByteEnd);
      System.out.println(currByteEnd);

      boolean hasWrittenToFile = false;

      // if byteStart starts in this block
      if (byteStart >= currByteStart && byteStart <= currByteEnd) {
        hasWrittenToFile = true;
        int blockStart = (int) (byteStart - currByteStart);

        if (byteEnd > currByteEnd) {
          int len = byteBlock.length - blockStart;
          LocalStorage.save(filename, byteBlock, blockStart, len);

        } else {
          int len = (int) (currByteEnd - currByteStart);
          LocalStorage.save(filename, byteBlock, blockStart, len);
        }
      }

      if (!hasWrittenToFile) {
        // if byteEnd is in this block, should always come after the previous if clause
        if (byteEnd >= currByteStart && byteStart <= currByteEnd) {
          int len = (int) (byteEnd - currByteStart);
          LocalStorage.save(filename, byteBlock, 0, len);
        } else if (byteStart < currByteStart && byteEnd > currByteEnd) { // if this whole block is within bytestart and byteend
          LocalStorage.save(filename, byteBlock, 0, byteBlock.length);
        }
      }

      // always pass it on to the next datanode if there is one
      if (!isLastNode) {
        nextNodeDos.write(byteBlock);
      }
    }

    if (isLastNode) {
      // TODO: add boolean flag and set it to false if any errors occurs
      // when saving all the stuff, and in that case send ERR instead of ACk
      dos.writeByte(DatanodeProtocol.ACK);

    } else {
      // wait for ACK here
      // TODO: later add timeout if it takes too long
      // TODO: check if ack or error was returned
      byte ackOrErr = nextNodeDis.readByte();
      nextNodeDis.close();
      nextNodeDos.close();
      nextNodeSocket.close();

      // Only the first datanode in the chain ACKs to namenode
      if (sequenceNumber == 0) {
        JSONObject json = new JSONObject();
        json.put("cmd", "ack-upload");
        json.put("id", fileHash);

        datanode.sendToNamenode(json.toString());
      }

      dos.writeByte(DatanodeProtocol.ACK);
    }

    dis.close();
    dos.close();
  }

  private void readRequest(int sequenceNumber, int fileHash) throws IOException {
    int numNodes = dis.readInt();

    List<NodeEntry> nodeEntries = new LinkedList<NodeEntry>();

    // the byte start and end for this datanode
    long byteStart = 0;
    long byteEnd = 0;

    // read header
    for (int i = 0; i < numNodes; i++) {
      byte[] iaddr = new byte[16];
      dis.read(iaddr);
      long bs = dis.readLong();
      long be = dis.readLong();

      if (isOwnIP(iaddr)) {
        byteStart = bs;
        byteEnd = be;

      } else {
        nodeEntries.add(new NodeEntry(iaddr, bs, be));
      }
    }

    // write the contents of this node's filepart as soon as we can
    // to the parent node
    String filename = getFilename(fileHash, byteStart, byteEnd);
    LocalStorage.load(filename, dos);

    boolean isLastNode = nodeEntries.isEmpty();

    Socket nextNodeSocket = null;
    DataInputStream nextNodeDis = null;
    DataOutputStream nextNodeDos = null;

    if (!isLastNode) {
      byte[] iaddr = nodeEntries.get(0).iaddr;
      nextNodeSocket = new Socket(InetAddress.getByAddress(iaddr), Datanode.PORT);
      nextNodeDis = new DataInputStream(new BufferedInputStream(nextNodeSocket.getInputStream()));
      nextNodeDos = new DataOutputStream(new BufferedOutputStream(nextNodeSocket.getOutputStream()));

      // write header
      nextNodeDos.writeByte(DatanodeProtocol.READ);
      nextNodeDos.writeInt(sequenceNumber + 1);
      nextNodeDos.writeInt(fileHash);
      nextNodeDos.writeShort(nodeEntries.size());
      for (NodeEntry ne : nodeEntries) {
        nextNodeDos.write(ne.iaddr, 0, ne.iaddr.length);
        nextNodeDos.writeLong(ne.byteStart);
        nextNodeDos.writeLong(ne.byteEnd);
      }
    }

    // Just pass the other dataparts through the chain
    // TODO: while there is data left, pass it through

  }

  private void deleteRequest() throws IOException {
    // TODO
  }

  private void acknowledge() throws IOException {
    // TODO, pass the ack to parent node
  }

  private String getFilename(int fileHash, long byteStart, long byteEnd) {
    return "Filepart" + fileHash + "-" + byteStart + "-" + byteEnd;
  }

  private boolean isOwnIP(byte[] iaddr) {
    // temp
    System.out.println("isOwnIP()");
    try {
      String ip = InetAddress.getByAddress(iaddr).toString();
      System.out.println(ip);
      if (ip.equals("/e:3133:302e:3232:392e:3134:352e:3934")) {
        System.out.println("IP matched!");
        return true;
      }
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    /////////

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
