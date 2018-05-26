/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sridhar
 */
public class GoogleFileSystem {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        
        Controller cont = new Controller();
        String[] cs_args1 = {"localhost","9091","localhost","9092","1"};
        String[] cs_args2 = {"localhost","9091","localhost","9093","2"};
        String[] cs_args3 = {"localhost","9091","localhost","9094","3"};
        try {
        ChunkServer cs1 = new ChunkServer(cs_args1);
        Thread.sleep(500);
        ChunkServer cs2 = new ChunkServer(cs_args2);
        Thread.sleep(500);
        ChunkServer cs3 = new ChunkServer(cs_args3);
        
        Thread.sleep(2500);      
            
        Client c = new Client();
        //ArrayList<String> Arr = new ArrayList<String>();
        //Arr = c.WriteRequest("localhost",9091);
        //if (Arr.size() != 0)
        //{c.WriteFileToDS("localhost",9091,Arr);}       
        c.WriteFileToDS("localhost",9091,"TestFiles/image.jpg");
        
        Thread.sleep(10000);
        //c.ReadFileFromDS("localhost",9091,"image.jpg");
        } catch (InterruptedException ex) {
            Logger.getLogger(GoogleFileSystem.class.getName()).log(Level.SEVERE, null, ex);
        }
    }   
}
