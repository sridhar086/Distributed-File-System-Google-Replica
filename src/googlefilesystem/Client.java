/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sridhar
 */



public class Client {
    
    
    //private Socket soc;
    public ArrayList<String> WriteRequest(String ControllerHost, int ControllerPort)
    {

        try {
        Socket socket = new Socket();
        InetAddress addr = InetAddress.getByName(ControllerHost);
        SocketAddress sockaddr = new InetSocketAddress(addr, ControllerPort);
        socket.connect(sockaddr);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        out.writeUTF("WRITEREQUEST");
        String response = in.readUTF();
        //System.out.println("response "+response);
        ArrayList<String> Arr = new ArrayList<String>();
        String[] arg;
        if (!response.isEmpty())
        {
            arg = response.split(" ");
            Arr.add(arg[1]);Arr.add(arg[2]);Arr.add(arg[3]);
            socket.close();
            return Arr;
        }
        socket.close();
        return Arr;
            
            
        } catch (Exception ex) {
            System.out.println("Exception in Client.java WriteRequest");           
            return new ArrayList<String>();
                        
        } 
        
    }
    
    public byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }
    public Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }   

    
    
    
    public void ReadRequest(String ControllerHost, int ControllerPort, String filename)
    {
        try {
        Socket socket = new Socket();
        InetAddress addr = InetAddress.getByName(ControllerHost);
        SocketAddress sockaddr = new InetSocketAddress(addr, ControllerPort);
        socket.connect(sockaddr);
        InputStream inp = socket.getInputStream();
        OutputStream outp = socket.getOutputStream();
        DataOutputStream out = new DataOutputStream(outp);
        DataInputStream in = new DataInputStream(inp);        
        
        out.writeUTF("READREQUEST "+filename);
        int length = in.readInt();
        byte[] readrequestresponse = new byte[length];
        in.readFully(readrequestresponse);
        Hashtable<Integer,ArrayList<String>> chunktochunkserverID = new Hashtable<Integer,ArrayList<String>>();
        chunktochunkserverID = (Hashtable<Integer, ArrayList<String>>) this.deserialize(readrequestresponse);
        
        List<Integer> chunklist = new ArrayList(chunktochunkserverID.keySet());
        Collections.sort(chunklist);
        //ArrayList<byte[]> bytearray = new ArrayList<byte[]>();
        File returnedfile = new File("TestFiles/"+"DFS"+filename);
        FileOutputStream returnedfout = new FileOutputStream(returnedfile);
        for(int chunkseq:chunklist)
            {
                ArrayList<String> chunkserverIDs = new ArrayList<String>();
                chunkserverIDs = chunktochunkserverID.get(chunkseq);
                
                Collections.shuffle(chunkserverIDs);
                String chunkserverhost = chunkserverIDs.get(0);
                //System.out.println("The chunk "+chunk+" will be obtained from "+chunkserverhost);
                byte[] bytes = Read(filename,chunkseq,chunkserverhost);
                System.out.println("the size of bytes us "+bytes.length);
                //bytearray.add(bytes);
                returnedfout.write(bytes);
                
            }
        returnedfout.close();
        
        } catch (Exception ex) {
            System.out.println("Exception in Client.java "+ex.toString());                        
        }        
    }
    
    private byte[] Read(String filename, int chunkseq, String chunkserverhost)         
    {            
        try{
            String chunkhost = chunkserverhost.split("/")[0];
            int chunkport = Integer.parseInt(chunkserverhost.split("/")[1]);
            Socket readsocket = new Socket(chunkhost, chunkport);
            DataInputStream din = new DataInputStream( readsocket.getInputStream());
            DataOutputStream dout = new DataOutputStream(readsocket.getOutputStream());            
            dout.writeUTF("READ "+filename+"_"+chunkseq);            
            byte[] readbyte = new byte[din.readInt()];
            din.readFully(readbyte);
            return readbyte;
            //din.readInt();
                
        }catch(Exception ex){
            return new byte[0];
                
        }       
    }

    
    
    private void Write(String c_write, String cserver1, String FileName, int chunkseq)//, int NumChunks, boolean flag)
    {
        
        try {
            String[] arg = cserver1.split("/");
            System.out.println("write from client to "+arg[0]+" "+arg[1]);
            Socket writesocket = new Socket(arg[0], Integer.parseInt(arg[1]));
            DataOutputStream out = new DataOutputStream(writesocket.getOutputStream());            
            DataInputStream in = new DataInputStream(writesocket.getInputStream());
            byte[] sent_byte  = c_write.getBytes("ISO-8859-1");
            out.writeUTF("WRITE 2 "+FileName+"_"+chunkseq);
            /*
            if (flag == false){
            out.writeUTF("WRITE "+NumChunks);
            //int length = sent_byte.length;
            //out.writeInt(length);            
            }*/
            out.writeInt(sent_byte.length);
            out.write(sent_byte);
            //in.readUTF();
            writesocket.close();

        } catch (IOException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
    }
    
    public void WriteFileToDS(String ControllerHost, int ControllerPort, String filename)
    {      
        this.ReadFile(ControllerHost, ControllerPort, filename);      
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
    
    
    
    public void ReadFile(String ControllerHost, int ControllerPort, String filename)
    {       
        try {
            File file = new File(filename);
            FileInputStream is = new FileInputStream(file);
            
            //System.out.println("The size of file is "+);
            double FileSize = is.getChannel().size();
            int NumChunks = (int) Math.ceil(FileSize/(double)65536);
            System.out.println("Number of chunks "+NumChunks);
            byte[] chunk = new byte[65536];
            int chunkLen = 0;
            int chunkseq = 0;
            //boolean flag = false;
            while ((chunkLen = is.read(chunk)) != -1) 
            {
                ArrayList<String> Arr = new ArrayList<String>();
                Arr = this.WriteRequest(ControllerHost, ControllerPort);
                String cserver1 = new String();
                String cserver2 = new String();
                String cserver3 = new String();
                cserver1 = Arr.get(0);
                cserver2 = Arr.get(1);
                cserver3 = Arr.get(2);
                System.out.println(cserver1+" "+cserver2+" "+cserver3);
                //this.soc = new Socket(cserver1.split("/")[0],Integer.parseInt(cserver1.split("/")[1]));
                //System.out.println("The chunk size is "+chunk.length+" "+chunkLen);
                byte[] chunk_sent = deepcopy(chunk, chunkLen);
                //System.out.println("The chunk size is "+chunk_sent.length+" "+chunkLen);
                String sent_string = new String(chunk_sent,"ISO-8859-1");
                String c_write = cserver2+" "+cserver3+" "+sent_string;              
                this.Write(c_write, cserver1,file.getName(),chunkseq);//, NumChunks, flag);             
                //System.out.println("hashcode "+SHA1FromBytes(chunk_sent));
                chunkseq +=1;
                //flag = true;
                //break;
            }
            //soc.close();            
            } catch (Exception e) {
            // file not found, handle case
            }
        
    }
    
    public void ReadFileFromDS(String ControllerHost, int ControllerPort, String FileName)
    {
        this.ReadRequest(ControllerHost, ControllerPort, FileName);       
        
    }
    
    
    
    public Client()
    {
        
              
    }


    
    
    
}
