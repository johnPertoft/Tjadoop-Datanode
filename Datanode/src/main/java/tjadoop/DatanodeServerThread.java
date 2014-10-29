package tjadoop;

import org.json.JSONException;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
          fileHash = dis.readInt();
          readRequest(fileHash);
          break;

        default:
          // unknown request type
          // TODO: do what?
      }
    } catch (IOException e) {
      // TODO: signal to Datanode that this was a malformed request or whatever happened
      return;
    } catch (JSONException e) {
      // json exception?
      return;
    }
  }

  // TODO: this could need some refactoring maybe
  private void createRequest(int sequenceNumber, int fileHash) throws IOException, JSONException {
    System.out.println("CREATE REQUEST");
    int numNodes = dis.readInt();

    List<NodeEntry> nodeEntries = new LinkedList<NodeEntry>();
    List<StartEndPair> startEndpairs = new LinkedList<StartEndPair>();

    // read header
    for (int i = 0; i < numNodes; i++) {
      byte[] iaddr = new byte[16];
      dis.read(iaddr);
      long bs = dis.readLong();
      long be = dis.readLong();

      if (isOwnIP(iaddr))
        startEndpairs.add(new StartEndPair(bs, be));
      else
        nodeEntries.add(new NodeEntry(iaddr, bs, be));
    }

    // sorts on increasing byteStarts
    Collections.sort(startEndpairs);
    System.out.println(startEndpairs);

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

    long totalBytesRead = 0;
    byte[] byteBlock = new byte[1024 * 1024 * 32];

    // This assumes that each of these pairs are ordered and not overlapping
    Iterator<StartEndPair> sepIt = startEndpairs.iterator();
    StartEndPair sep = sepIt.next();
    System.out.println(sep);

    // TODO: this whole block could use some refactoring
    // TODO: check for EOF too?
    while (totalBytesRead < dataLength) {
      int bytesRead = dis.read(byteBlock);
      totalBytesRead += bytesRead;

      // only one of the following cases should happen per block read
      boolean doneInBlock = false;

      // byte numbers in the total stream of bytes sent
      long currentLastByte = totalBytesRead - 1;
      long currentFirstByte = totalBytesRead - bytesRead;

      // If the whole nodeblock is within the currently read block
      if (sep.start >= currentFirstByte && sep.end <= currentLastByte) {
        String filename = LocalStorage.getFilename(fileHash, sep.start, sep.end);

        int startInBlock = (int) (sep.start - currentFirstByte);
        int len = (int) (sep.end - sep.start);

        System.out.println("FIRST SAVE CASE");
        LocalStorage.save(filename, byteBlock, startInBlock, len);

        // move to next StartEndPair
        if (sepIt.hasNext()) {
          sep = sepIt.next();
        }

        doneInBlock = true;
      }

      // If only the start of the current nodeblock is within the currently read block
      if (!doneInBlock && sep.start >= currentFirstByte && sep.start <= currentLastByte) {
        String filename = LocalStorage.getFilename(fileHash, sep.start, sep.end);

        int startInBlock = (int) (sep.start - currentFirstByte);
        int len = (int) (sep.start - currentFirstByte);

        System.out.println("SECOND SAVE CASE");
        LocalStorage.save(filename, byteBlock, startInBlock, len);

        doneInBlock = true;
      }

      // If neither the start or the end of the current nodeblock is within the currently read block
      // save all of it
      if (!doneInBlock && sep.start < currentFirstByte && sep.end > currentLastByte) {
        String filename = LocalStorage.getFilename(fileHash, sep.start, sep.end);

        System.out.println("THIRD SAVE CASE");
        LocalStorage.save(filename, byteBlock, 0, bytesRead);

        doneInBlock = true;
      }

      // If only the end of the current nodeblock is within the currently read block
      if (!doneInBlock && sep.end >= currentFirstByte && sep.end <= currentLastByte) {
        String filename = LocalStorage.getFilename(fileHash, sep.start, sep.end);

        int len = (int) (sep.end - currentFirstByte + 1);

        System.out.println("FOURTH SAVE CASE");
        LocalStorage.save(filename, byteBlock, 0, len);

        // move to next startEndPair
        if (sepIt.hasNext()) {
          sep = sepIt.next();
        }
      }

      // always pass the data on to the next datanode if there is one
      if (!isLastNode) {
        nextNodeDos.write(byteBlock, 0, bytesRead);
      }
    }

    if (isLastNode && sequenceNumber != 0) {
      // TODO: add boolean flag and set it to false if any errors occurs
      // when saving all the stuff, and in that case send ERR instead of ACK
      dos.writeByte(DatanodeProtocol.ACK);

    } else {
      // wait for ACK here
      // TODO: later add timeout if it takes too long
      // TODO: check if ack or error was returned
      // TODO: refactor this whole crap

      if (nextNodeDis != null) {
        byte ackOrErr = nextNodeDis.readByte();
        nextNodeDis.close();
        nextNodeDos.close();
        nextNodeSocket.close();
      }

      dos.writeByte(DatanodeProtocol.ACK);
    }

    // Only the first datanode in the chain ACKs to namenode
    if (sequenceNumber == 0) {
      datanode.sendToNamenode(NamenodeProtocol.ACK_UPLOAD(fileHash).toString());
    }

    dis.close();
    dos.close();

    System.out.println("create request finished");
  }

  private void readRequest(int fileHash) throws IOException {
    System.out.println("read request");
    long byteStart = dis.readLong();
    long byteEnd = dis.readLong();

    // write the contents of this node's filepart
    String filename = LocalStorage.getFilename(fileHash, byteStart, byteEnd);
    LocalStorage.load(filename, dos);

    System.out.println("finished read request");
  }

  private boolean isOwnIP(byte[] iaddr) {
    try {
      String ip = InetAddress.getByAddress(iaddr).toString();
      //System.out.println(ip);
      if (ip.equals("/e:3133:302e:3232:392e:3137:352e:3838")) {
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

  private class StartEndPair implements Comparable<StartEndPair> {
    long start;
    long end;

    StartEndPair(long start, long end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public int compareTo(StartEndPair startEndPair) {
      if (this.start < startEndPair.start) return -1;
      if (this.start > startEndPair.start) return 1;
      return 0;
    }

    @Override
    public String toString() {
      return "(" + start + ", " + end + ")";
    }
  }
}
