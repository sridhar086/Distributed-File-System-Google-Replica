/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import static googlefilesystem.Listener.hashtable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
       
       
    }
    
}


class ChunkServerListenener implements Runnable
{
    //private Socket soc;
    private ServerSocket chunklistener;
    private String myhostname;
    private int myportnum;
    private int chunkserverID;
    public ChunkServerListenener(String myhostname,int myportnum, int chunkserverID)
    {
        this.myhostname = myhostname;
        this.myportnum = myportnum;
        this.chunkserverID = chunkserverID;
    }
    
    public String SHA1FromBytes(byte[] data) 
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
      
    public void answer(Socket soc)
    {
        try{
        DataInputStream in = new DataInputStream(soc.getInputStream());
        DataOutputStream out = new DataOutputStream(soc.getOutputStream());
        String message = in.readUTF();      
        //System.out.println("Received a new message "+myhostname+" "+myportnum);
        String[] args = message.split(" ");
        switch(args[0])
        {
            case "WRITE":
                
                int NumHosts = Integer.parseInt(args[1]);
                String FileName = new String(args[2]);
                //System.out.println("The file name is "+args[2]);
                int i = in.readInt();
                byte[] r_byte = new byte[i];
                in.readFully(r_byte);
                
                soc.close();
                String st = new String(r_byte,"ISO-8859-1");
                String[] arg = st.split(" ",NumHosts+1);               
                byte[] wtf = arg[arg.length-1].getBytes("ISO-8859-1");
                //System.out.println("hashcode "+SHA1FromBytes(wtf));
                
                //System.out.println("No of hosts "+NumHosts);
                
                
                /*saving the file with filename as a chunk*/
                
                File file = new File("Chunks/"+FileName+"_"+chunkserverID);
                FileOutputStream Fout = new FileOutputStream(file);
                Fout.write(wtf);
                Fout.close();
                
                
                if (NumHosts != 0)
                {
                //buggy
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                    try{
                    String[] forward = st.split(" ",2);
                    String[] forwardhost = forward[0].split("/");
                    //System.out.println("forward host and port number is "+forwardhost[0]+" "+forwardhost[1]);
                    Socket WriteForwardSocket = new Socket(forwardhost[0], Integer.parseInt(forwardhost[1]));
                    
                    byte[] forward_byte  = forward[1].getBytes("ISO-8859-1");
                    DataOutputStream out = new DataOutputStream(WriteForwardSocket.getOutputStream());
                    DataInputStream in = new DataInputStream(WriteForwardSocket.getInputStream());
                    out.writeUTF("WRITE "+(NumHosts-1)+" "+FileName);
                    out.writeInt(forward_byte.length);
                    out.write(forward_byte);
                    in.readUTF();
                        }catch(Exception e){}
                    }
                }).start();
                
                }
                Thread.sleep(100);
                
                
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
        chunklistener = new ServerSocket(myportnum);
        while(true)
        {
            System.out.println("The chunk server is listening on port "+myportnum);
            Socket soc = chunklistener.accept();
            //System.out.println(chunklistener.getLocalSocketAddress().toString());
            answer(soc);
            
            
        }
        } catch (Exception ex) 
        {//System.out.println("connection closed");
            
        }
        
    }
    
}


public class ChunkServer {

    int myportnum;
    String myhostname;
    int chunkserverID;
    int contportnum;
    String conthostname;
    
    public ChunkServer(String[] args) {
        
        try {
            conthostname = args[0];
            contportnum = Integer.parseInt(args[1]);
            myhostname = args[2];
            myportnum = Integer.parseInt(args[3]);
            chunkserverID = Integer.parseInt(args[4]);
            

            Socket soc = new Socket(conthostname, contportnum);            
            DataOutputStream out = new DataOutputStream(soc.getOutputStream());
            DataInputStream in = new DataInputStream(soc.getInputStream());
            String request = "NEWCHUNKSERVER "+myhostname+" "+myportnum+" "+chunkserverID;
            out.writeUTF(request);
            String response = in.readUTF();
            if (response.equals("OK"))
            {System.out.println("The response ok is received ");}
            soc.close();            
            new Thread(new ChunkServerListenener(myhostname,myportnum,chunkserverID)).start();           
        } catch (IOException ex) {
            
        }
    }   
}
