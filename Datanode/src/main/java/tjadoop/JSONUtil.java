package tjadoop;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.IOException;

public class JSONUtil {
  public static JSONObject parseJSONStream(DataInputStream dis) throws IOException, JSONException {
    StringBuilder sb = new StringBuilder();
    // read until num left brackets == right brackets
    int lb = 0;
    int rb = 0;
    do {
      byte c = dis.readByte();
      sb.append((char) c);

      if (c == '{') lb++;
      if (c == '}') rb++;

    } while (lb != rb);

    return new JSONObject(sb.toString());
  }
}
