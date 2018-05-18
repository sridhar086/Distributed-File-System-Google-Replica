/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
    
    public void WriteFileToDS(String ControllerHost, int ControllerPort)
    {      
        this.ReadFile(ControllerHost, ControllerPort);      
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
    
    
    
    public void ReadFile(String ControllerHost, int ControllerPort)
    {       
        try {
            File file = new File("TestFiles/image.jpg");
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
        
    }
    
    
    
    public Client()
    {
        
              
    }
    
    
    
}
