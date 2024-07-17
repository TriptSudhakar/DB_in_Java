package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int addr = 4;
        byte[] lenKey = new byte[2];

        for (int i = 0; i < numKeys; i++) {
            lenKey = this.get_data(addr+2, 2);
            int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
            keys[i] = this.convertBytesToT(this.get_data(addr + 4, length), this.typeClass);
            addr += 4 + length;
        }
        return keys;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        byte[] lenKeyBytes = new byte[2];
        byte[] rightBlockIdBytes = new byte[2];
        byte[] keyBytes = convertTToBytes(key, this.typeClass);
        lenKeyBytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
        lenKeyBytes[1] = (byte) (keyBytes.length & 0xFF);
        rightBlockIdBytes[0] = (byte) ((right_block_id >> 8) & 0xFF);
        rightBlockIdBytes[1] = (byte) (right_block_id & 0xFF);

        byte[] nextFreeOffsetBytes = this.get_data(2, 2);
        int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);
        int numKeys = this.getNumKeys();

        int addr = 6;
        byte[] lenKey = new byte[2];
        T[] keys = getKeys();
        for(int i=0;i<numKeys;i++) {
            lenKey = this.get_data(addr, 2);
            int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
            keys[i] = this.convertBytesToT(this.get_data(addr + 2, length), this.typeClass);
            if(this.compare(keys[i], key, this.typeClass)>0) break;
            addr += 4 + length;
        }

        if(addr<nextFreeOffset)
        {
            byte[] data_to_write = this.get_data(addr,nextFreeOffset-addr);
            this.write_data(addr+keyBytes.length+4, data_to_write);
        }
        this.write_data(addr, lenKeyBytes);
        this.write_data(addr+2, keyBytes);
        this.write_data(addr+2+keyBytes.length, rightBlockIdBytes);

        numKeys++;
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((numKeys >> 8) & 0xFF);
        numKeysBytes[1] = (byte) (numKeys & 0xFF);
        this.write_data(0, numKeysBytes);

        
        nextFreeOffset += 4 + keyBytes.length;
        nextFreeOffsetBytes[0] = (byte) ((nextFreeOffset >> 8) & 0xFF);
        nextFreeOffsetBytes[1] = (byte) (nextFreeOffset & 0xFF);
        this.write_data(2, nextFreeOffsetBytes);

        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */
        int numKeys = getNumKeys();
        int[] children = this.getChildren();
        T[] keys = this.getKeys();

        for (int i = 0; i < numKeys; i++) {
            if (this.compare(keys[i], key, this.typeClass) > 0) {
                return children[i];
            }
        }
        return children[keys.length];
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int addr = 4;
        byte[] child = new byte[2];
        byte[] lenKey = new byte[2];

        for (int i = 0; i < numKeys; i++) {
            child = this.get_data(addr, 2);
            children[i] = ((child[0] << 8) | (child[1] & 0xFF));
            lenKey = this.get_data(addr+2, 2);
            int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
            addr += 4 + length;
        }
        child = this.get_data(addr, 2);
        children[numKeys] = ((child[0] << 8) | (child[1] & 0xFF));
        return children;

    }

}
