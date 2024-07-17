package index.bplusTree;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    public void updateNumKeys(int numKeys) {
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((numKeys >> 8) & 0xFF);
        numKeysBytes[1] = (byte) numKeys;
        this.write_data(0, numKeysBytes);
    }

    public void setPrev(int prev) {
        // set prev leaf node
        byte[] prevBytes = new byte[2];
        prevBytes[0] = (byte) ((prev >> 8) & 0xFF);
        prevBytes[1] = (byte) prev;
        this.write_data(2, prevBytes);
    }

    public int getPrev()
    {
        byte[] prevBytes = this.get_data(2, 2);
        int prev = (prevBytes[0] << 8) | (prevBytes[1] & 0xFF);
        return prev;
    }

    public void setNext(int next) {
        // set next leaf node
        byte[] nextBytes = new byte[2];
        nextBytes[0] = (byte) ((next >> 8) & 0xFF);
        nextBytes[1] = (byte) next;
        this.write_data(4, nextBytes);
    }

    public int getNext()
    {
        byte[] nextBytes = this.get_data(4, 2);
        int next = (nextBytes[0] << 8) | (nextBytes[1] & 0xFF);
        return next;
    }

    public void updateNextFreeOffset(int nextFreeOffset) {
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) ((nextFreeOffset >> 8) & 0xFF);
        nextFreeOffsetBytes[1] = (byte) nextFreeOffset;
        this.write_data(6, nextFreeOffsetBytes);
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int addr = 8;
        byte[] lenKey = new byte[2];

        for (int i = 0; i < numKeys; i++) {
            lenKey = this.get_data(addr+2, 2);
            int length = ((lenKey[0] & 0xFF)<< 8) | (lenKey[1] & 0xFF);
            keys[i] = this.convertBytesToT(this.get_data(addr + 4, length), this.typeClass);
            addr += 4 + length;
        }
        return keys;

    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int addr = 8;
        byte[] lenKey = new byte[2];
        byte[] id = new byte[2];
        
        for (int i = 0; i < numKeys; i++) {
            id = this.get_data(addr, 2);
            block_ids[i] = (id[0] << 8) | (id[1] & 0xFF);
            lenKey = this.get_data(addr+2, 2);
            int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
            addr += 4 + length;
        }
        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {


        /* Write your code here */
        byte[] lenKeyBytes = new byte[2];
        byte[] blockIdBytes = new byte[2];
        byte[] keyBytes = convertTToBytes(key, this.typeClass);
        lenKeyBytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
        lenKeyBytes[1] = (byte) (keyBytes.length & 0xFF);
        blockIdBytes[0] = (byte) ((block_id >> 8) & 0xFF);
        blockIdBytes[1] = (byte) (block_id & 0xFF);

        byte[] nextFreeOffsetBytes = this.get_data(6, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);
        int numKeys = this.getNumKeys();

        int addr = 8;
        byte[] lenKey = new byte[2];
        T[] keys = this.getKeys();
        for(int i=0;i<numKeys;i++) {
            lenKey = this.get_data(addr+2, 2);
            int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
            keys[i] = this.convertBytesToT(this.get_data(addr + 4, length), this.typeClass);
            if(this.compare(keys[i], key, this.typeClass)>0) break;
            addr += 4 + length;
        }

        if(addr<nextFreeOffset)
        {
            byte[] data_to_write = this.get_data(addr,nextFreeOffset-addr);
            this.write_data(addr+keyBytes.length+4, data_to_write);
        }
        this.write_data(addr, blockIdBytes);
        this.write_data(addr+2, lenKeyBytes);
        this.write_data(addr+4, keyBytes);

        numKeys++;
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((numKeys >> 8) & 0xFF);
        numKeysBytes[1] = (byte) (numKeys & 0xFF);
        this.write_data(0, numKeysBytes);

        
        nextFreeOffset += 4 + keyBytes.length;
        nextFreeOffsetBytes[0] = (byte) ((nextFreeOffset >> 8) & 0xFF);
        nextFreeOffsetBytes[1] = (byte) (nextFreeOffset & 0xFF);
        this.write_data(6, nextFreeOffsetBytes);

        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */
        int numKeys = this.getNumKeys();
        T[] keys = this.getKeys();
        int[] block_ids = this.getBlockIds();

        for (int i = 0; i < numKeys; i++) {
            if (this.compare(keys[i], key, this.typeClass)==0) {
                return block_ids[i];
            }
        }
        return -1;
    }

}
