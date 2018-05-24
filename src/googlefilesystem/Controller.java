/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import static googlefilesystem.Listener.hashtable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
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
    public static Hashtable<String,Hashtable<Integer,ArrayList<Integer>>> filemap = new Hashtable<String,Hashtable<Integer,ArrayList<Integer>>>();
    
    
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }
    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }   
    
    
    public static void answer(Socket soc)
    {
        try{
        InputStream inp = soc.getInputStream();
        OutputStream outp = soc.getOutputStream();
        DataInputStream in = new DataInputStream(inp);
        DataOutputStream out = new DataOutputStream(outp);
               
        String message = in.readUTF();     
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
                out.writeUTF(write_str);
                //return write_str;
                break;
                
            case "READREQUEST":
                System.out.println("THE FILENAME FOUND OUT IS  "+args[1]);
                if (filemap.containsKey(args[1]))
                {
                    Hashtable<Integer,ArrayList<Integer>> chunktochunkserverID = new Hashtable<Integer,ArrayList<Integer>>();
                    chunktochunkserverID = filemap.get(args[1]);                    
                    byte[] readrequest = serialize(chunktochunkserverID);
                    System.out.println("length is "+readrequest.length);
                    out.writeInt(readrequest.length);
                    out.write(readrequest);
                }                
                break;
            case "MAJORHEARTBEAT":
                System.out.println("Major heartbeat Chunk server ID "+args[1]+" length: "+Integer.parseInt(args[2]));
                byte[] majorheartbeatbyte = new byte[Integer.parseInt(args[2])];
                
                in.readFully(majorheartbeatbyte);
                String majorheartbeatstring = new String(majorheartbeatbyte, "ISO-8859-1");
                System.out.println(majorheartbeatstring);                
                out.writeUTF("OK");
                break;
            case "MINORHEARTBEAT":   
                System.out.println("Minor hearbeat Chunk server ID "+args[1]+" length: "+Integer.parseInt(args[2]));
                int chunkserverID = Integer.parseInt(args[1]);
                if(Integer.parseInt(args[2]) != 0)
                {
                byte[] minorheartbeatbyte = new byte[Integer.parseInt(args[2])];
                in.readFully(minorheartbeatbyte);
                String minorheartbeatstring = new String(minorheartbeatbyte, "ISO-8859-1");
                System.out.println(minorheartbeatstring);
                String[] minorheartbeats = minorheartbeatstring.split("__*__");
                //System.out.println(minorheartbeats);
                List<String> filelist = new ArrayList<String>(Arrays.asList(minorheartbeats));
                
                for(String file:filelist)
                {
                    String f = file.split(" ",2)[0];
                    String chunklist = minorheartbeatstring.split(" ",2)[1].trim();
                    chunklist = chunklist.replace("__*__", "");
                    System.out.println(chunklist);
                    //System.out.println(chunklist.substring(1, chunklist.length()-1));
                    List<String> myList = new ArrayList<String>(Arrays.asList(chunklist.substring(1, chunklist.length()-1).split(",")));
                    addtofilemap(f,myList,chunkserverID);
                    System.out.println(myList);
                }
                out.writeUTF("OK");
                //printfilemap();
                }
                
                break;
            case "NEWCHUNKSERVER":
                String ns_str = args[1]+"/"+args[2];
                hashtable.put(Integer.parseInt(args[3]), ns_str);
                out.writeUTF("OK");
                break;                               
            default:
                System.out.println("");            
        }           
        }catch(Exception e){System.out.println(e.toString());}
    }

    private static void printfilemap()
    {
        for(String str: filemap.keySet())
        {
            Hashtable<Integer,ArrayList<Integer>> chunktochunkserverID = new Hashtable<Integer,ArrayList<Integer>>();
            chunktochunkserverID = filemap.get(str);
            for(int chunk:chunktochunkserverID.keySet())
            {
                ArrayList<Integer> chunkserverIDs = new ArrayList<Integer>();
                chunkserverIDs = chunktochunkserverID.get(chunk);
                for (int ID: chunkserverIDs)
                {
                    System.out.println("File Name: "+str+" Chunk No: "+chunk+" Chunkserver ID:"+ID);
                }
            }
        }    
    }    
    
    private static void addtofilemap(String f, List<String> myList, int chunkserverID) 
    {
        if (filemap.containsKey(f))
        {
            Hashtable<Integer,ArrayList<Integer>> chunktochunkserverID = new Hashtable<Integer,ArrayList<Integer>>();
            chunktochunkserverID = filemap.get(f);
            
            for (String rawstr: myList)
            {
                String str = new String();
                str = rawstr.trim();
                if(chunktochunkserverID.containsKey(Integer.parseInt(str)))
                {
                    ArrayList<Integer> chunkserverIDlist = new ArrayList<Integer>();
                    chunkserverIDlist = chunktochunkserverID.get(Integer.parseInt(str));
                    chunkserverIDlist.add(chunkserverID);
                    chunktochunkserverID.put(Integer.parseInt(str), chunkserverIDlist);
                }
                else
                {
                    ArrayList<Integer> chunkserverIDlist = new ArrayList<Integer>();
                    chunkserverIDlist.add(chunkserverID);
                    chunktochunkserverID.put(Integer.parseInt(str), chunkserverIDlist);
                }
            }
        }
        else{
        
        Hashtable<Integer,ArrayList<Integer>> chunktochunkserverID = new Hashtable<Integer,ArrayList<Integer>>();
        for(String rawstr:myList)
        {
            String str = new String();
            str = rawstr.trim();
            ArrayList<Integer> chunkserverIDlist = new ArrayList<Integer>();
            chunkserverIDlist.add(chunkserverID);
            chunktochunkserverID.put(Integer.parseInt(str),chunkserverIDlist);
        }
        filemap.put(f, chunktochunkserverID);
        }
        
    }
    
    
    @Override
    public void run() {
    
        try {
            while(true)
            {
            
            Socket Childsoc = Serversocket.accept();
            answer(Childsoc);
            Childsoc.close(); 
            
            //new DataOutputStream(Childsoc.getOutputStream()).writeUTF(response);
            //Childsoc.close();
            
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
