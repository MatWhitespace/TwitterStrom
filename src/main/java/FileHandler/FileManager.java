package FileHandler;

import java.io.*;
import java.util.concurrent.Semaphore;

public class FileManager implements Serializable {
    private String fileName;
    private Semaphore mutex;

    public FileManager(String fileName){
        this.fileName = fileName;
        this.mutex = new Semaphore(1,true);
    }

    public PrintWriter getWrite() throws InterruptedException, FileNotFoundException {
        mutex.acquire();
        return new PrintWriter(new File(fileName));
    }

    public boolean stopWrite(PrintWriter pw) {
        pw.close();
        mutex.release();
        return true;
    }

    public BufferedReader getRead() throws FileNotFoundException, InterruptedException {
        mutex.acquire();
        return new BufferedReader(new FileReader(fileName));
    }

    public Boolean stopRead(BufferedReader br) throws IOException {
        br.close();
        mutex.release();
        return true;
    }

}
