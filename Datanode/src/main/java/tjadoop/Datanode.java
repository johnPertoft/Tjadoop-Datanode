package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class Datanode {

  public static final int PORT = 49377;
  public final byte[] IADDRESS;

  private DataInputStream namenodeInput;
  private DataOutputStream namenodeOutput;

  private ServerSocket datanodeServerSocket;

  private NamenodeThread namenodeThread;

  public Datanode(DataInputStream dis, DataOutputStream dos, ServerSocket datanodeServerSocket, byte[] iaddr) {
    this.datanodeServerSocket = datanodeServerSocket;
    this.namenodeInput = dis;
    this.namenodeOutput = dos;

    IADDRESS = iaddr;
  }

  // TODO: move this to namenodeThread class?
  public synchronized void sendToNamenode(String json) throws IOException {
    namenodeOutput.writeBytes(json);
  }

  public void run() {
    try {
      setup();
      namenodeThread = new NamenodeThread(this, namenodeInput, namenodeOutput);
      new Thread(namenodeThread).start();

    } catch (IOException e) {
      System.err.println("Failed during setup with name node");
      return;
    } catch (JSONException e) {
      System.err.println("Failed during setup with name node");
      System.err.println("json error");
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
        e.printStackTrace();
      }
    }
  }

  public synchronized void handleNamenodeMessage(JSONObject json) {
    // static variables in other classes are only known at runtime apparently
    // so cant use switch which needs them at compile time

    try {

      if (json.get("cmd").equals(NamenodeProtocol.CMD_RM_FILE)) {
        // TODO: we probably need some lock for the datanodeserverthreads that might be reading that file?
        LocalStorage.delete(json.getInt("id"));
      }

      // TODO: add the heart beat thing here as well?

    } catch (JSONException e) {
      // TODO: panic
    } catch (IOException e) {
      // TODO
    }
  }

  private void setup() throws IOException, JSONException {
    try {
      JSONObject json = NamenodeProtocol.INIT("130.229.175.88");
      namenodeOutput.writeBytes(json.toString());

    } catch (JSONException e) {
      System.err.println("Failed to send setup message to namenode");
      System.exit(1);
    }

    JSONObject jsonResp = JSONUtil.parseJSONStream(namenodeInput);

    if (jsonResp.getBoolean("success")) {
      System.out.println("Setup success");
    } else {
      System.out.println("Setup failed");
    }
  }
}
