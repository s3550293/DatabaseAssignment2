import java.nio.ByteBuffer;
import java.io.*;

/**
 *  Database Systems - HEAP IMPLEMENTATION
 */

public class hashload implements dbimpl
{
    /**
     * Hash Table that holds the buckets
     */
    private Hash[] hashtable = new Hash[13107];

   // initialize
   public static void main(String args[])
   {
      hashload load = new hashload();

      // calculate query time
      long startTime = System.currentTimeMillis();
      load.readArguments(args);
      long endTime = System.currentTimeMillis();

      System.out.println("Load time: " + (endTime - startTime) + "ms");
   }


   // reading command line arguments
   public void readArguments(String args[])
   {
        if (isInteger(args[0]))
        {
        	readHeap(Integer.parseInt(args[0]));
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
	public void readHeap(int pagesize)
	{
		File heapfile = new File(HEAP_FNAME + pagesize);
		File hashfile = new File(HASH_FNAME + pagesize);
		FileOutputStream fos_Hash = null;
    	int intSize = 4;
    	int pageCount = 0;
    	int recCount = 0;
    	int recordLen = 0;
		int rid = 0;
		int offset = 0;
    	boolean isNextPage = true;
		boolean isNextRecord = true;
		FileInputStream fis = null;
    	try
    	{
    		fis = new FileInputStream(heapfile);
    		// reading page by page
    		while (isNextPage)
    		{
    	    	byte[] bPage = new byte[pagesize];
    	    	byte[] bPageNum = new byte[intSize];
    	    	fis.read(bPage, 0, pagesize);
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
					//    printRecord(bRecord, name);
						offset = ( recCount * RECORD_SIZE )+( pageCount * pagesize );
						addHash(bRecord,offset);
                		recordLen += RECORD_SIZE;
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
		/**
          * CODE
          * this saves each indavitual bucket in the array to a file
		  */
		try{
		  	fos_Hash = new FileOutputStream(hashfile);
		  	ObjectOutputStream oos = new ObjectOutputStream(fos_Hash);
		  	for(int i=0;i<hashtable.length;i++){
				oos.writeObject(hashtable[i]);
			}
			oos.close();
		}catch (FileNotFoundException e){
			System.out.println("File: " + hashfile + " not found.");
		}
		catch (IOException e)
    	{
        	e.printStackTrace();
    	}
	}

   /**
    * Adds a hash index for the given business name and
    * saves the name and pagecount to the hash key
    * then saves the new hash index to the array
    */
    public void addHash(byte[] rec, int offset){

		String record = new String(rec);
    	String BN_NAME = record.substring(RID_SIZE+REGISTER_NAME_SIZE,
							RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE);
		String new_Val = BN_NAME.replaceAll("\0+$", "");
		int hashval = Math.abs(new_Val.hashCode() % 13107);
		// System.out.println(hashval);
		if(hashtable[hashval] == null){
			hashtable[hashval] = new Hash(hashval, offset);
		}
		else {
			hashtable[hashval].put(hashtable[hashval], new Hash(hashval ,offset));
		}
    }
}
