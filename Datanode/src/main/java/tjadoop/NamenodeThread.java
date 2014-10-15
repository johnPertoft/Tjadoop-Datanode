package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class NamenodeThread implements Runnable {

  private DataInputStream namenodeInput;
  private DataOutputStream namenodeOutput;

  public NamenodeThread(DataInputStream dis, DataOutputStream dos) {
    this.namenodeInput = dis;
    this.namenodeOutput = dos;
  }

  @Override
  public void run() {
    // listen to requests from Namenode
    boolean listening = true;
    while (listening) {
      try {
        JSONObject json = JSONUtil.parseJSONStream(namenodeInput);
        System.out.println();
      } catch (IOException e) {
        // TODO: PANIC
      } catch (JSONException e) {
        // TODO: PANIC
      }
    }
  }
}
