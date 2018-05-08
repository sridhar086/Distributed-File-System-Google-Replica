/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sridhar
 */



public class Client {
    
    private Socket socket;
    
    private String WriteRequest(String ControllerHost, int ControllerPort)
    {

        try {
        InetAddress addr = InetAddress.getByName(ControllerHost);
        int port = 80;
        SocketAddress sockaddr = new InetSocketAddress(addr, ControllerPort);
        socket.connect(sockaddr);
        
        //String message="WRITEREQUEST"
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("WRITEREQUEST");
            String response = in.readUTF();
            return response;
            
            
        } catch (Exception ex) {
            System.out.println("Exception in Client.java WriteRequest");
            return "";
            
        } 
        
    }
    
    private void Write()
    {
        
    }
    
    public Client(String[] args)
    {
        socket = new Socket();
        String response = this.WriteRequest(args[0], Integer.parseInt(args[1]));
        String[] arg;
        if (!response.isEmpty())
        {
            arg = response.split(" ");
            String[] CServer1 = arg[1].split("/");
            String[] CServer2 = arg[2].split("/");
            String[] CServer3 = arg[3].split("/");
            String ChunkServerMessage = "WRITE "+arg[2]+" "+ arg[3];
            
        }
        
        
              
    }
    
    
    
}
