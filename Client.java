import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;


public class Client{
    
    DFS dfs;
    public Client(int p) throws Exception {
        dfs = new DFS(p);
        Scanner in = new Scanner(System.in);

        while(true) {
            System.out.print("Enter command: $ ");
            String[] choice = in.nextLine().split(" ");
            String command = choice[0];
            if (choice.length == 1) {
                if (command.equals("ls")) {
                    System.out.println(dfs.ls());
                } else if (command.equals("exit") || command.equals("quit")) {
                    break;
                } else {
                    System.out.println("Invalid command.");
                }
            } else if (choice.length == 2) {
                if (command.equals("join")) {
                    dfs.join("localHost", Integer.parseInt(choice[1]));
                } else if (command.equals("touch")) {
                    dfs.touch(choice[1]);
                } else if (command.equals("delete")) {
                    dfs.delete(choice[1]);
                } else if (command.equals("tail")) {
                    byte[] tail = dfs.tail(choice[1]);
                    System.out.println(new String(tail).replace("/n", "\n"));
                } else if (command.equals("head")) {
                    byte[] head = dfs.head(choice[1]);
                    System.out.println(new String(head).replace("/n", "\n"));
                } else if (command.equals("append")) {
                    System.out.println("Appending the contents of \"append.txt\" to the end of " + choice[1]);
                    byte[] b = Files.readAllBytes(Paths.get("append.txt"));
                    dfs.append(choice[1], b);
                } else {
                    System.out.println("Invalid command.");
                }
            } else if (choice.length == 3) {
                if (command.equals("read")) {
                    System.out.println(new String(dfs.read(choice[1], Integer.parseInt(choice[2]))));
                } else if (command.equals("mv")){
                    dfs.mv(choice[1], choice[2]);
                } else {
                    System.out.println("Invalid command.");
                }
            }
        }
        in.close();
        System.out.println("Closing connection to DFS.\nGoodbye.");
        System.exit(0);
    }

    static public void main(String args[]) throws Exception {
        System.out.print("Please enter the port number you would like to connect to: ");
        Scanner in = new Scanner(System.in);
        int port = in.nextInt();
        Client client=new Client(port);
    }
}

