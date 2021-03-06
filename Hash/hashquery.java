import java.nio.ByteBuffer;
import java.io.*;

/**
 *  Database Systems - HEAP IMPLEMENTATION
 */

public class hashquery implements dbimpl
{
   // initialize
   public static void main(String args[])
   {
      hashquery load = new hashquery();

      // calculate query time
      long startTime = System.currentTimeMillis();
      load.readArguments(args);
      long endTime = System.currentTimeMillis();

      System.out.println("Query time: " + (endTime - startTime) + "ms");
   }


   // reading command line arguments
   public void readArguments(String args[])
   {
      if (args.length == 2)
      {
         if (isInteger(args[1]))
         {
            readHeap(args[0], Integer.parseInt(args[1]));
         }
      }
      else
      {
          System.out.println("Error: only pass in two arguments");
      }
   }

   // check if pagesize is a valid integer
   public boolean isInteger(String s)
   {
      boolean isValidInt = false;
      try
      {
         Integer.parseInt(s);
         isValidInt = true;
      }
      catch (NumberFormatException e)
      {
         e.printStackTrace();
      }
      return isValidInt;
   }

   // read heapfile by page
   public void readHeap(String name, int pagesize)
   {
        File heapfile = new File(HEAP_FNAME + pagesize);
        File hashfile = new File(HASH_FNAME + pagesize);
        int intSize = 4;
        int offset = 0;
        int key = Math.abs(name.hashCode() % 13107);
        boolean isNextRecord = true;
        RandomAccessFile raf = null;
        FileInputStream fis = null;
        Hash hash = null;
        boolean loop = true;
        int index = 0;
        try
        {
             // reading page by page
            isNextRecord = true;
            Object obj = null;
            try{
                fis = new FileInputStream(hashfile);
                ObjectInputStream input = new ObjectInputStream(fis);
                while(loop){
                    obj = input.readObject();
                    if(obj!=null){
                        if (obj instanceof Hash) {
                            hash = (Hash) obj;
                            if(key == hash.hashVal){
                                loop = false;
                            }
                        }
                    }
                }
            }catch (ClassNotFoundException cnfe){}
            catch (EOFException eof){
                System.out.println("End of file, record not found");
            }

            raf = new RandomAccessFile(heapfile,"r");

            while (isNextRecord)
            {
                offset = hash.offset;
                fis = new FileInputStream(hashfile);
                byte[] bRecord = new byte[RECORD_SIZE];
                byte[] bRid = new byte[intSize];
                raf.seek(offset);
                raf.read(bRecord, 0, RECORD_SIZE);
                isNextRecord = printRecord(bRecord, name);
                if(hash.next != null){
                    hash = hash.next;
                    // System.out.println("Next");
                }else{
                    break;
                }
            }
        }
        catch (FileNotFoundException e)
        {
           System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
        }
        catch (IOException e)
        {
           e.printStackTrace();
        }
    }

   // returns records containing the argument text from shell
    public boolean printRecord(byte[] rec, String name)
    {
        String record = new String(rec);
        String BN_NAME = record.substring(RID_SIZE+REGISTER_NAME_SIZE,
                                        RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE);
        if (BN_NAME.toLowerCase().contains(name.toLowerCase()))
        {
            System.out.println(record);
            return false;
        }
        return true;
   }
}
