import java.io.Serializable;

public class Hash implements Serializable{
    private static final long serialVersionUID = -5302010108271068350L;
    // Holds the next Hash Index of the same value
    Hash next = null;
    // Holds the Hash value and the page number as an offset
    int hashVal,offset;
    // Holds the buisness name
    String name;

    /**
     * Constructor for the hash object/bucket
     */
    public Hash(int hashVal, String name, int offset){
        this.hashVal = hashVal;
        this.name = name;
        this.offset = offset;
    }

    /**
     * Places new Hash linked list that acts as a bucket
     * this results in bottomless buckets a limit can be added
     */
    public void put(Hash current, Hash nextHash){
        while(true){
            if(current.next == null){
                // current.prev = nextHash;
                nextHash.next = nextHash;
                break;
            }else{
                current = current.next;
            }
        }
        
    }
}