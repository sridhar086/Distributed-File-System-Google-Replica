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
                //int NumChunks = Integer.parseInt(message.split(" ")[1]);
                //while(NumChunks != 0){
                int NumHosts = Integer.parseInt(args[1]);
                int i = in.readInt();
                byte[] r_byte = new byte[i];
                in.readFully(r_byte);
                //out.writeUTF("OK");
                soc.close();
                String st = new String(r_byte,"ISO-8859-1");
                String[] arg = st.split(" ",NumHosts+1);               
                byte[] wtf = arg[arg.length-1].getBytes("ISO-8859-1");
                System.out.println("hashcode "+SHA1FromBytes(wtf));
                
                System.out.println("My host name and port number is "+ChunkServer.myhostname+" "+ChunkServer.myportnum+" no of hosts "+NumHosts);
                /*saving the file with filename as a chunk */
                //NumChunks -=1;            
                //System.out.println(SHA1FromBytes(received_byte)+" "+length_bytes+" "+slice.length+" "+received_byte.length);
                
                if (NumHosts != 0)
                {
                //buggy
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                    try{
                    String[] forward = st.split(" ",2);
                    String[] forwardhost = forward[0].split("/");
                    System.out.println("forward host and port number is "+forwardhost[0]+" "+forwardhost[1]);
                    Socket WriteForwardSocket = new Socket(forwardhost[0], Integer.parseInt(forwardhost[1]));
                    //byte[] wtf = forward[1].getBytes("ISO-8859-1");
                    //String forward_string = new String(wtf,"ISO-8859-1");
                    //String forward_write = arg[1]+" "+forward_string;
                    byte[] forward_byte  = forward[1].getBytes("ISO-8859-1");
                    DataOutputStream out = new DataOutputStream(WriteForwardSocket.getOutputStream());
                    DataInputStream in = new DataInputStream(WriteForwardSocket.getInputStream());
                    out.writeUTF("WRITE "+(NumHosts-1)+" filename");
                    out.writeInt(forward_byte.length);
                    out.write(forward_byte);
                    in.readUTF();
                        }catch(Exception e){}
                    }
                }).start();
                }
                
                //}
                break;               
            /*
            case "WRITEFORWARD":
                int forwardhostsnum = Integer.parseInt(args[1]);
                int j = in.readInt();
                byte[] fr_byte = new byte[j];
                in.readFully(fr_byte);
                out.writeUTF("OK");
                String fst = new String(fr_byte,"ISO-8859-1");
                String[] farg = fst.split(" ",forwardhostsnum);
                byte[] fwtf = farg[forwardhostsnum].getBytes("ISO-8859-1");
                System.out.println("hashcode "+SHA1FromBytes(fwtf));
                
                
                //buggy
                
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try{
                        String[] forwardhost = farg[0].split("/");
                Socket WriteForwardSocket = new Socket(forwardhost[0], Integer.parseInt(forwardhost[1]));
                String forward_string = new String(fwtf,"ISO-8859-1");
                String forward_write = farg[1]+" "+forward_string;
                byte[] forward_byte  = forward_write.getBytes("ISO-8859-1");
                DataOutputStream out = new DataOutputStream(WriteForwardSocket.getOutputStream());
                DataInputStream in = new DataInputStream(WriteForwardSocket.getInputStream());
                out.writeUTF("WRITEFORWARD 1 "+"filename");
                out.writeInt(forward_byte.length);
                out.write(forward_byte);
                in.readUTF();
                        }catch(Exception e){}
                    }
                });
                break;
            */    
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
