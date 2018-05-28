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
import java.io.File;
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
    public static Hashtable<String,Hashtable<Integer,ArrayList<String>>> filemap = new Hashtable<String,Hashtable<Integer,ArrayList<String>>>();
    
    
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
                    Hashtable<Integer,ArrayList<String>> chunktochunkserverID = new Hashtable<Integer,ArrayList<String>>();
                    chunktochunkserverID = filemap.get(args[1]);                    
                    byte[] readrequest = serialize(chunktochunkserverID);
                    System.out.println("length is "+readrequest.length);
                    out.writeInt(readrequest.length);
                    out.write(readrequest);
                }
                break;
            case "MAJORHEARTBEAT":
                //System.out.println("Major heartbeat Chunk server ID "+args[1]+" length: "+Integer.parseInt(args[2]));
                byte[] majorheartbeatbyte = new byte[Integer.parseInt(args[2])];                
                in.readFully(majorheartbeatbyte);
                String majorheartbeatstring = new String(majorheartbeatbyte, "ISO-8859-1");
                //System.out.println(majorheartbeatstring);
                out.writeUTF("OK");
                break;
            case "MINORHEARTBEAT":   
                //System.out.println("Minor hearbeat Chunk server ID "+args[1]+" length: "+Integer.parseInt(args[2]));
                int chunkserverID = Integer.parseInt(args[1]);
                if(Integer.parseInt(args[2]) != 0)
                {
                byte[] minorheartbeatbyte = new byte[Integer.parseInt(args[2])];
                in.readFully(minorheartbeatbyte);
                String minorheartbeatstring = new String(minorheartbeatbyte, "ISO-8859-1");
                //System.out.println(minorheartbeatstring);
                String[] minorheartbeats = minorheartbeatstring.split("__*__");
                //System.out.println(minorheartbeats);
                List<String> filelist = new ArrayList<String>(Arrays.asList(minorheartbeats));
                
                for(String file:filelist)
                {
                    String f = file.split(" ",2)[0];
                    String chunklist = minorheartbeatstring.split(" ",2)[1].trim();
                    chunklist = chunklist.replace("__*__", "");
                    //System.out.println(chunklist);
                    //System.out.println(chunklist.substring(1, chunklist.length()-1));
                    List<String> myList = new ArrayList<String>(Arrays.asList(chunklist.substring(1, chunklist.length()-1).split(",")));
                    addtofilemap(f,myList,chunkserverID);
                    //System.out.println(myList);
                }
                
                //printfilemap();
                }
                out.writeUTF("OK");
                
                break;
            case "NEWCHUNKSERVER":
                String ns_str = args[1]+"/"+args[2];
                hashtable.put(Integer.parseInt(args[3]), ns_str);
                out.writeUTF("OK");
                break; 
            case "CHUNKRETRIEVAL":
                //System.out.println(args[1]+" "+args[2]);
                String checkfile= args[1]; //args[1] starts with this filename
                int chunkserverIDcheck = Integer.parseInt(args[2]);
                
                String checkfilename = checkfile.split("_")[0];
                int checkfilechunk = Integer.parseInt(checkfile.split("_")[1]);
                //System.out.println("the filename is "+checkfilename+" chunk is "+checkfilechunk);
                ArrayList<String> hostnames = new ArrayList<String>();
                
                hostnames = filemap.get(checkfilename).get(checkfilechunk);
                //System.out.println("the hostnames are "+hostnames);
                String chunkmissinghost = hashtable.get(chunkserverIDcheck);
                //System.out.println("missing chunk host is "+chunkmissinghost);
                hostnames.remove(chunkmissinghost);
                String missingchunkreply = "CHUNKRETRIEVALRESPONSE ";
                for(String host: hostnames)
                {
                    missingchunkreply += host +" ";
                }
                out.writeUTF(missingchunkreply);
                //System.out.println("the return string is "+missingchunkreply);
                
                //System.out.println("the server requested the corrupted chunk is "+chunkmissinghost);
                //System.out.println("The remaining hosts that contains the chunks are "+hostnames);
                  
                
                /*File dir = new File("Chunks/");
                File[] directoryListing = dir.listFiles();                
                if (directoryListing != null)
                {
                    for (File child : directoryListing) 
                    {                       
                        if(child.toString().startsWith(checkfile) && !child.toString().endsWith(chunkserverIDcheck) && !child.toString().endsWith(chunkserverIDcheck+".xml"))
                        {
                            System.out.println("The files found out are "+child.toString());
                        }                       
                    }
                }*/
                break;
            default:
                System.out.println("");            
        }           
        }catch(Exception e){System.out.println("The exception in answer(message) in controller is "+e.toString());}
    }

    private static void printfilemap()
    {
        for(String str: filemap.keySet())
        {
            Hashtable<Integer,ArrayList<String>> chunktochunkserverID = new Hashtable<Integer,ArrayList<String>>();
            chunktochunkserverID = filemap.get(str);
            for(int chunk:chunktochunkserverID.keySet())
            {
                ArrayList<String> chunkserverIDs = new ArrayList<String>();
                chunkserverIDs = chunktochunkserverID.get(chunk);
                for (String ID: chunkserverIDs)
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
            Hashtable<Integer,ArrayList<String>> chunktochunkserverID = new Hashtable<Integer,ArrayList<String>>();
            chunktochunkserverID = filemap.get(f);
            
            for (String rawstr: myList)
            {
                String str = new String();
                str = rawstr.trim();
                if(chunktochunkserverID.containsKey(Integer.parseInt(str)))
                {
                    ArrayList<String> chunkserverIDlist = new ArrayList<String>();
                    chunkserverIDlist = chunktochunkserverID.get(Integer.parseInt(str));
                    String chunkserverhost = hashtable.get(chunkserverID);
                    chunkserverIDlist.add(chunkserverhost);
                    chunktochunkserverID.put(Integer.parseInt(str), chunkserverIDlist);
                }
                else
                {
                    ArrayList<String> chunkserverIDlist = new ArrayList<String>();
                    String chunkserverhost = hashtable.get(chunkserverID);
                    chunkserverIDlist.add(chunkserverhost);
                    chunktochunkserverID.put(Integer.parseInt(str), chunkserverIDlist);
                }
            }
        }
        else{
        
        Hashtable<Integer,ArrayList<String>> chunktochunkserverID = new Hashtable<Integer,ArrayList<String>>();
        for(String rawstr:myList)
        {
            String str = new String();
            str = rawstr.trim();
            ArrayList<String> chunkserverIDlist = new ArrayList<String>();
            String chunkserverhost = hashtable.get(chunkserverID);
            chunkserverIDlist.add(chunkserverhost);
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
            System.out.println("The exception in controller is  "+ex.toString());
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
            System.out.println("the exception in controller in controller is "+ex.toString());
        }
        
        

            
        
    }
    
    
    
    
    
    
    
    
}
