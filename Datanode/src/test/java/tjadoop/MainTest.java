package tjadoop;

import junit.framework.TestCase;

import java.net.InetAddress;

public class MainTest extends TestCase {

  public void testGetIP() throws Exception {
    byte[] ip = Main.getIP();
    System.out.println(ip.length);
    System.out.println(InetAddress.getByAddress(ip));
  }
}