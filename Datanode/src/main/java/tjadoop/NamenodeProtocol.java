package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

public class NamenodeProtocol {
  public static JSONObject INIT(String ip) throws JSONException {
    JSONObject json = new JSONObject();
    json.put("cmd", "init");
    json.put("ip", ip);

    return json;
  }

  public static JSONObject ACK_RM_FILE(int fileHash) throws JSONException {
    JSONObject json = new JSONObject();
    json.put("cmd", "ack-rmfile");
    json.put("id", fileHash);

    return json;
  }
}
