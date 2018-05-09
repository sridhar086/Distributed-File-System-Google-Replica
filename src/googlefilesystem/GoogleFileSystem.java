/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package googlefilesystem;

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
        String[] str_args = {"127.0.0.1","9091","127.0.0.1","9092","1"};
        ChunkServer cs = new ChunkServer(str_args);
    }
    
}
