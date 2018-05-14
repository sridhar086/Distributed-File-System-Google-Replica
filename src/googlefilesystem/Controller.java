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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Random;
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
                ArrayList<Integer> arr = new ArrayList<Integer>(hashtable.keySet());
                Collections.shuffle(arr);
                String str1 = hashtable.get(arr.get(0));
                String str2 = hashtable.get(arr.get(1));
                String str3 = hashtable.get(arr.get(2));
                String write_str = "OK "+str1+" "+str2+" "+str3;
                return write_str;
                //break;
            case "MAJORHEARTBEAT":
                    break;
            case "MINORHEARTBEAT":
                    break;
            case "NEWCHUNKSERVER":
                String ns_str = args[1]+"/"+args[2];
                hashtable.put(Integer.parseInt(args[3]), ns_str);
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
            Childsoc.close();
            
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
