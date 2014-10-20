package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Queue;

public class NamenodeThread implements Runnable {

  private Datanode datanode;
  private DataInputStream namenodeInput;
  private DataOutputStream namenodeOutput;

  private boolean listening;
  private Queue<JSONObject> messageQueue;

  public NamenodeThread(Datanode datanode, DataInputStream dis, DataOutputStream dos) {
    this.datanode = datanode;
    this.namenodeInput = dis;
    this.namenodeOutput = dos;
    this.listening = true;
  }

  @Override
  public void run() {
    // listen to requests from Namenode
    while (listening) {
      try {
        JSONObject json = JSONUtil.parseJSONStream(namenodeInput);
        System.out.println("new message from namenode received");
        datanode.handleNamenodeMessage(json);
        //addToCommandQueue(json);

      } catch (IOException e) {
        // TODO: PANIC
      } catch (JSONException e) {
        // TODO: PANIC
      }
    }
  }

  public synchronized JSONObject getNextCommand() {
    return null;
  }

  private synchronized void addToCommandQueue(JSONObject json) {
    messageQueue.add(json);
    notify();
  }
}
