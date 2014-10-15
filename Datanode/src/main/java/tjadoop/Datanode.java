package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class Datanode {

  public static final int PORT = 15568;
  public final byte[] IADDRESS;

  private DataInputStream namenodeInput;
  private DataOutputStream namenodeOutput;

  private ServerSocket datanodeServerSocket;

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
      NamenodeThread nnt = new NamenodeThread(namenodeInput, namenodeOutput);
      new Thread(nnt).start();

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
        // running = false;
      }
    }
  }

  private void setup() throws IOException, JSONException {
    JSONObject json = new JSONObject();
    try {
      json = NamenodeProtocol.INIT("130.229.145.94");
      namenodeOutput.writeBytes(json.toString());

    } catch (JSONException e) {
    }

    /*
    StringBuilder sb = new StringBuilder();
    // read until num left brackets == right brackets
    int lb = 0;
    int rb = 0;
    do {
      byte c = namenodeInput.readByte();
      sb.append((char) c);

      if (c == '{') lb++;
      if (c == '}') rb++;

    } while (lb != rb);

    JSONObject jsonResp = new JSONObject(sb.toString());
    */
    JSONObject jsonResp = JSONUtil.parseJSONStream(namenodeInput);

    if (jsonResp.getBoolean("success")) {
      System.out.println("Setup success");
    } else {
      System.out.println("Setup failed");
    }
  }
}
