/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
    static ServerSocket chunklistener;

    @Override
    public void run() {
        try {
        chunklistener = new ServerSocket(ChunkServer.myportnum);
        while(true)
        {
            Socket soc = chunklistener.accept();
            DataInputStream in = new  DataInputStream(soc.getInputStream());
            DataOutputStream out = new  DataOutputStream(soc.getOutputStream());
            
            
        }
        } catch (IOException ex) {
            
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
