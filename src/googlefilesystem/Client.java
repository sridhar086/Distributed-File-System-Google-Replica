/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
        socket = new Socket();
        InetAddress addr = InetAddress.getByName(ControllerHost);
        SocketAddress sockaddr = new InetSocketAddress(addr, ControllerPort);
        socket.connect(sockaddr);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        out.writeUTF("WRITEREQUEST");
        String response = in.readUTF();
        //System.out.println("response "+response);
        String[] arg;
        if (!response.isEmpty())
        {
            arg = response.split(" ");
            String[] CServer1 = arg[1].split("/");
            String[] CServer2 = arg[2].split("/");
            String[] CServer3 = arg[3].split("/");
            String ChunkServerMessage = "WRITE "+arg[2]+" "+ arg[3];
            
        }
            return response;
            
            
        } catch (Exception ex) {
            System.out.println("Exception in Client.java WriteRequest");
            return "";
            
        } 
        
    }
    
    private void Write()
    {
        
    }
    
    public void WriteFileToDS(String ControllerHost, int ControllerPort)
    {
        
        
    }
    
    public byte[] deepcopy(byte[] arr, int length)
    {
        byte[] array = new byte[length];
        for(int i =0;i<length;i++)
        {
            array[i] = arr[i];
        }
        return array;
    }
    
    public void ReadFile()
    {
        
        try {
            File file = new File("TestFiles/image.jpg");
            FileInputStream is = new FileInputStream(file);
            byte[] chunk = new byte[65536];
            int chunkLen = 0;
            while ((chunkLen = is.read(chunk)) != -1) {
                System.out.println("The chunk size is "+chunk.length+" "+chunkLen);
                byte[] chunk_sent = deepcopy(chunk, chunkLen);
                System.out.println("The chunk size is "+chunk_sent.length+" "+chunkLen);                
            }
            } catch (Exception e) {
            // file not found, handle case
            }
        
    }
    
    public Client()
    {       
        
              
    }
    
    
    
}
