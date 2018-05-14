import java.io.Serializable;

public class Hash implements Serializable{
    // Hash prev = null;
    Hash next = null;
    int hashVal,offset;
    String name;

    public Hash(String name, int offset){
        hashVal = name.hashCode() % 3940;
        this.name = name;
        this.offset = offset;
    }

    /**
     * Places new Hash in bucket
     * this results in bottomless buckets
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