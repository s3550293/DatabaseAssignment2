import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.EOFException;

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
   /**
    * Skips pages untill we find the one that is located in the heap index
    * Once it is found we print then end the search
    */
   public void readHeap(String name, int pagesize)
   {
      File heapfile = new File(HEAP_FNAME + pagesize);
      File hashfile = new File(HASH_FNAME + pagesize);
      int intSize = 4;
      int pageCount = 0;
      int page = 0;
      int recCount = 0;
      int recordLen = 0;
      int rid = 0;
      int key = name.hashCode() % 3940;
      boolean isNextPage = true;
      boolean isNextRecord = true;
      boolean loop = true;
      try
      {
        FileInputStream fis = null;
        try{
            /**
             * #### New Code
             * Reads the hash file loads in hash buckets one at a time
             * if the hash index matches the then the function will proceed checking the
             * name and set the offset.
             * if it doesn't then the hash bucket will be dropped and a new one will
             * be read from file
             */
            fis = new FileInputStream(hashfile);
            Object obj = null;
            ObjectInputStream input = new ObjectInputStream(fis);
            while (loop){
                obj = input.readObject();
                if (obj instanceof Hash) {
                    Hash hash = (Hash) obj;
                    if(key == hash.hashVal){
                        while(loop){
                            if(hash.name.equals(name)){
                                page = hash.offset;
                                loop = false;
                            }else if(hash.next == null){
                                loop = false;
                            }else{
                                hash = hash.next;
                            }
                        }
                    }
                }
            }
        }catch (ClassNotFoundException cnfe){}
        catch (EOFException eof){
            System.out.println("End of file, record not found");
        }
        
        fis = new FileInputStream(heapfile);
         // reading page by page
         boolean exit = false;
         while (isNextPage)
         {
            byte[] bPage = new byte[pagesize];
            byte[] bPageNum = new byte[intSize];
            fis.read(bPage, 0, pagesize);
            if(pageCount == page){
                System.arraycopy(bPage, bPage.length-intSize, bPageNum, 0, intSize);

                // reading by record, return true to read the next record
                isNextRecord = true;
                while (isNextRecord)
                {
                    byte[] bRecord = new byte[RECORD_SIZE];
                    byte[] bRid = new byte[intSize];
                    try
                    {
                        System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
                        System.arraycopy(bRecord, 0, bRid, 0, intSize);
                        rid = ByteBuffer.wrap(bRid).getInt();
                        if (rid != recCount)
                        {
                            isNextRecord = false;
                        }
                        else
                        {
                            exit = printRecord(bRecord, name);
                            recordLen += RECORD_SIZE;
                            if(exit){
                                return;
                            }
                        }
                        recCount++;
                        // if recordLen exceeds pagesize, catch this to reset to next page
                    }
                    catch (ArrayIndexOutOfBoundsException e)
                    {
                        isNextRecord = false;
                        recordLen = 0;
                        recCount = 0;
                        rid = 0;
                    }
                }
            }
            // check to complete all pages
            if (ByteBuffer.wrap(bPageNum).getInt() != pageCount)
            {
               isNextPage = false;
            }
            pageCount++;
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
   public boolean printRecord(byte[] rec, String input)
   {
      String record = new String(rec);
      String BN_NAME = record
                         .substring(RID_SIZE+REGISTER_NAME_SIZE,
                          RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE);
      if (BN_NAME.toLowerCase().contains(input.toLowerCase()))
      {
         System.out.println(record);
         return true;
      }
      return false;
   }
}
