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
        String[] cs_args1 = {"127.0.0.1","9091","127.0.0.1","9092","1"};
        String[] cs_args2 = {"127.0.0.1","9091","127.0.0.1","9093","2"};
        String[] cs_args3 = {"127.0.0.1","9091","127.0.0.1","9094","3"};
        ChunkServer cs1 = new ChunkServer(cs_args1);
        ChunkServer cs2 = new ChunkServer(cs_args2);
        ChunkServer cs3 = new ChunkServer(cs_args3);
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Logger.getLogger(GoogleFileSystem.class.getName()).log(Level.SEVERE, null, ex);
        }        
        
        Client c = new Client();
        //ArrayList<String> Arr = new ArrayList<String>();
        //Arr = c.WriteRequest("127.0.0.1",9091);
        //if (Arr.size() != 0)
        //{c.WriteFileToDS("127.0.0.1",9091,Arr);}       
        c.WriteFileToDS("127.0.0.1",9091);
    }
    
}
