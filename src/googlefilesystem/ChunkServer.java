/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import static googlefilesystem.Listener.hashtable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sridhar
 */


class heartbeats implements Runnable
{

    static void calculate()
    {
        
    }    
    
    
    @Override
    public void run() {
        try {
            Socket heartbeat = new Socket(ChunkServer.conthostname, ChunkServer.contportnum);
            
        } catch (IOException ex) {
            
        }
       
    }
    
}


class ChunkServerListenener implements Runnable
{
    private Socket soc;
    private static ServerSocket chunklistener;
    public static String SHA1FromBytes(byte[] data) 
    {  
        try 
        {            
            MessageDigest digest;
            digest = MessageDigest.getInstance("SHA1");
            byte[] hash = digest.digest(data);
            BigInteger hashInt = new BigInteger(1, hash);
            return hashInt.toString(16);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
    }
      
    public static void answer(Socket soc)
    {
        try{
        DataInputStream in = new DataInputStream(soc.getInputStream());
        DataOutputStream out = new DataOutputStream(soc.getOutputStream());
        String message = in.readUTF();      
        
        String[] args = message.split(" ");
        switch(args[0])
        {
            case "WRITE":
                int NumChunks = Integer.parseInt(message.split(" ")[1]);
                while(NumChunks != 0){
                int i = in.readInt();
                byte[] r_byte = new byte[i];
                in.readFully(r_byte);
                String st = new String(r_byte,"ISO-8859-1");
                String[] arg = st.split(" ",3);
                byte[] wtf = arg[2].getBytes("ISO-8859-1");
                System.out.println("hashcode "+SHA1FromBytes(wtf));
                NumChunks -=1;            
                //System.out.println(SHA1FromBytes(received_byte)+" "+length_bytes+" "+slice.length+" "+received_byte.length);
                }
                break;               
            
            case "WRITEFORWARD":
                break;
                
            case "READ":
                break;
            default:
                System.out.println("");             
            
        }
        
        }catch(Exception e){}
        
    }

    @Override
    public void run() {
        try {
        //System.out.println("waiting for a client from chunkserver");
        chunklistener = new ServerSocket(ChunkServer.myportnum);
        while(true)
        {            
            soc = chunklistener.accept();
            answer(soc);
            
            
        }
        } catch (Exception ex) 
        {//System.out.println("connection closed");
            
        }
        
    }
    
}


public class ChunkServer {

    static int myportnum;
    static String myhostname;
    static String chunkserverID;
    
    static int contportnum;
    static String conthostname;
    
    public ChunkServer(String[] args) {
        
        try {
            conthostname = args[0];
            contportnum = Integer.parseInt(args[1]);
            myhostname = args[2];
            myportnum = Integer.parseInt(args[3]);
            chunkserverID = args[4];
            

            Socket soc = new Socket(conthostname, contportnum);            
            DataOutputStream out = new DataOutputStream(soc.getOutputStream());
            DataInputStream in = new DataInputStream(soc.getInputStream());
            String request = "NEWCHUNKSERVER "+myhostname+" "+myportnum+" "+chunkserverID;
            out.writeUTF(request);
            String response = in.readUTF();
            if (response.equals("OK"))
            {System.out.println("The response ok is received ");}
            soc.close();            
            new Thread(new ChunkServerListenener()).start();           
        } catch (IOException ex) {
            
        }
    }   
}
