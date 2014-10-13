package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class Datanode {

  public static final int PORT = 1234;
  public final byte[] IADDRESS;

  private DataInputStream namenodeInput;
  private DataOutputStream namenodeOutput;

  private ServerSocket datanodeServerSocket;

  public Datanode(DataInputStream dis, DataOutputStream dos, ServerSocket datanodeServerSocket, byte[] iaddr) {
    this.namenodeInput = dis;
    this.namenodeOutput = dos;

    this.datanodeServerSocket = datanodeServerSocket;

    IADDRESS = iaddr;
  }

  public synchronized void sendToNamenode(String json) throws IOException {
    namenodeOutput.writeBytes(json);
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
        System.out.println("Client connected!");
        new Thread(dst).start();

      } catch (IOException e) {
        // running = false;
      }
    }
  }

  private void setup() throws IOException {
    JSONObject json = new JSONObject();
    try {
      json.append("cmd", "init");
      json.append("ip", InetAddress.getByAddress(IADDRESS).toString());
      namenodeOutput.writeBytes(json.toString());

    } catch (JSONException e) {
    }

    StringBuilder sb = new StringBuilder();
    // TODO: read the response
  }
}
