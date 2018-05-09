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
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sridhar
 */

class Listener implements Runnable {

    public static ServerSocket Serversocket;
    public static Hashtable<Integer,String> hashtable;
       
    public static String answer(String message)
    {
        String[] args = message.split(" ");
        switch(args[0])
        {
            case "WRITEREQUEST":
                break;
            case "MAJORHEARTBEAT":
                    break;
            case "MINORHEARTBEAT":
                    break;
            case "NEWCHUNKSERVER":
                String str = args[1]+"/"+args[2];
                hashtable.put(Integer.parseInt(args[3]), str);
                return "OK";
                
            default:
                System.out.println("");
                
            
        }
        return "";
        
    }
    
    
    @Override
    public void run() {
    
        try {
            while(true)
            {
            Socket Childsoc = Serversocket.accept();
            String message = new DataInputStream(Childsoc.getInputStream()).readUTF();
            String response = answer(message);
            new DataOutputStream(Childsoc.getOutputStream()).writeUTF(response);
            
            }
        } catch (IOException ex) {
            System.out.println("This is an exception");
        }
        
        
    }

    
    
}

public class Controller {

    public Controller() {
                    
        try {
            Listener.Serversocket = new ServerSocket(9091);
            hashtable = new Hashtable<Integer,String>();
            new Thread(new Listener()).start();
        } catch (IOException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        

            
        
    }
    
    
    
    
    
    
    
    
}
