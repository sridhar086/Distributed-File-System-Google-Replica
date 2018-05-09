/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sridhar
 */
public class ChunkServer {

    static int myportnum;
    static String myhostname;
    static String chunkserverID;
    
    public ChunkServer(String[] args) {
        
        try {
            myhostname = args[2];
            myportnum = Integer.parseInt(args[3]);
            chunkserverID = args[4];
            Socket soc = new Socket(args[0], Integer.parseInt(args[1]));            
            DataOutputStream out = new DataOutputStream(soc.getOutputStream());
            DataInputStream in = new DataInputStream(soc.getInputStream());
            String request = "NEWCHUNKSERVER "+myhostname+" "+myportnum+" "+chunkserverID;
            out.writeUTF(request);
            String response = in.readUTF();
            if (response.equals("OK"))
            {System.out.println("The response ok is received ");}
            
        } catch (IOException ex) {
            
        }
    }
    
    
    
    
}
