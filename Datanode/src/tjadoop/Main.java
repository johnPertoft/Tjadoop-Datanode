package tjadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {

  private final static String USAGE = "Need address and port to the Namenode";

  private static String namenodeAddress = "";
  private static int namenodePort;

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println(USAGE);
      System.exit(1);
    }

    namenodeAddress = args[0];
    try {
      namenodePort = Integer.parseInt(args[1]);
    } catch (NumberFormatException nfe) {
      System.exit(1);
    }

    Socket socket = null;
    ServerSocket serverSocket = null;

    boolean nnSocketEstablished = false;
    try {
      socket = new Socket(namenodeAddress, namenodePort);
      nnSocketEstablished = true;

    } catch (IOException e) {
      System.err.println("Failed to setup socket to namennode");
      System.exit(1);
    }

    boolean serverSocketEstablished = false;
    try {
      serverSocket = new ServerSocket(Datanode.PORT);
      serverSocketEstablished = true;

    } catch (IOException e) {
      System.err.println("Failed to setup datanodes server socket");
      System.exit(1);
    }

    if (nnSocketEstablished && serverSocketEstablished) {
      try {
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

        byte[] iaddr = InetAddress.getLocalHost().getAddress();

        new Datanode(dis, dos, serverSocket, iaddr).run();

        socket.close();

      } catch (IOException e) {
        System.err.println("Failed to setup input or output stream");
        System.exit(1);
      }
    }
  }
}
