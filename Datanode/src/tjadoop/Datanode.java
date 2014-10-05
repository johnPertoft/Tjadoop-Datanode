package tjadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Map;

public class Datanode {

  public static final int PORT = 1234;
  public final byte[] IADDRESS;

  private DataInputStream namenodeInput;
  private DataOutputStream namenodeOutput;

  // TODO: using this from the DatanodeServerThread needs to be synchronized
  private ServerSocket datanodeServerSocket;

  public Datanode(DataInputStream dis, DataOutputStream dos, ServerSocket datanodeServerSocket, byte[] iaddr) {
    this.namenodeInput = dis;
    this.namenodeOutput = dos;

    this.datanodeServerSocket = datanodeServerSocket;

    IADDRESS = iaddr;
  }

  public void run() {
    try {
      setup();
    } catch (IOException e) {
      System.err.println("Failed during setup with name node");
      return;
    }

    // accept loop
    boolean running = true;
    while (running) {
      try {
        DatanodeServerThread dst = new DatanodeServerThread(this, datanodeServerSocket.accept());
        new Thread(dst).start();

      } catch (IOException e) {
        // running = false;
      }
    }
  }

  private void setup() throws IOException {
    // TODO:
    // first write to server that this datanode is now online
    // get some info in response, like an id and addresses to the other datanodes

    namenodeOutput.writeBytes("sup?");
  }
}
