package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */
        ArrayList<Integer> parent = new ArrayList<>();
        int id = getRootId();

        while(!isLeaf(id)) {
            parent.add(id);
            InternalNode node = (InternalNode) blocks.get(id);
            id = node.search(key);
        }

        if(!isFull(id))
        { 
            LeafNode node = (LeafNode) blocks.get(id);
            node.insert(key,block_id);
            blocks.set(id,node);
            return;
        }
        
        if(id == getRootId())
        {
            LeafNode leaf = (LeafNode) blocks.get(id);
            int node_id = blocks.size();
            LeafNode node = new LeafNode<>(this.typeClass);
            int new_root_id = blocks.size()+1;

            int numKeys = getOrder()-1;
            int mid = (numKeys+1) / 2;

            T[] keys = (T[]) leaf.getKeys();
            int addr = 8;
            byte[] lenKey = new byte[2];

            if(leaf.compare(keys[mid-1],key,this.typeClass)>0)
            {
                for (int i = 0; i < mid-1; i++) {
                    lenKey = leaf.get_data(addr+2, 2);
                    int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
                    keys[i] = (T) leaf.convertBytesToT(leaf.get_data(addr + 4, length), this.typeClass);
                    addr += 4 + length;
                }
    
                byte[] nextFreeOffsetBytes = leaf.get_data(6, 2);
                int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

                node.write_data(8,leaf.get_data(addr,nextFreeOffset-addr));
                node.updateNumKeys(numKeys-mid+1);
                node.updateNextFreeOffset(8+nextFreeOffset-addr);
                
                leaf.updateNumKeys(mid-1);
                leaf.updateNextFreeOffset(addr);
                leaf.insert(key,block_id);
            }
            else
            {
                for (int i = 0; i < mid; i++) {
                    lenKey = leaf.get_data(addr+2, 2);
                    int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
                    keys[i] = (T) leaf.convertBytesToT(leaf.get_data(addr + 4, length), this.typeClass);
                    addr += 4 + length;
                }
    
                byte[] nextFreeOffsetBytes = leaf.get_data(6, 2);
                int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

                node.write_data(8,leaf.get_data(addr,nextFreeOffset-addr));
                node.updateNumKeys(numKeys-mid);
                node.updateNextFreeOffset(8+nextFreeOffset-addr);
                node.insert(key,block_id);
                
                leaf.updateNumKeys(mid);
                leaf.updateNextFreeOffset(addr);
            }

            leaf.setNext(node_id);
            node.setPrev(id);
            blocks.set(id,leaf);
            blocks.add(node);
            InternalNode newRoot = new InternalNode(((LeafNode) blocks.get(node_id)).getKeys()[0],id,node_id,this.typeClass);
            blocks.add(newRoot);
            byte[] rootIdBytes = new byte[2];
            rootIdBytes[0] = (byte) ((new_root_id >> 8) & 0xFF);
            rootIdBytes[1] = (byte) (new_root_id & 0xFF);
            BlockNode meta = blocks.get(0);
            meta.write_data(2, rootIdBytes);
            blocks.set(0,meta);
            return;
        }
        
        LeafNode leaf = (LeafNode) blocks.get(id);
        int node_id = blocks.size();
        LeafNode node = new LeafNode<>(this.typeClass);

        int numKeys = getOrder()-1;
        int mid = (numKeys+1) / 2;

        T[] keys = (T[]) leaf.getKeys();
        int addr = 8;
        byte[] lenKey = new byte[2];

        if(leaf.compare(keys[mid-1],key,this.typeClass)>0)
        {
            for (int i = 0; i < mid-1; i++) {
                lenKey = leaf.get_data(addr+2, 2);
                int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
                keys[i] = (T) leaf.convertBytesToT(leaf.get_data(addr + 4, length), this.typeClass);
                addr += 4 + length;
            }

            byte[] nextFreeOffsetBytes = leaf.get_data(6, 2);
            int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

            node.write_data(8,leaf.get_data(addr,nextFreeOffset-addr));
            node.updateNumKeys(numKeys-mid+1);
            node.updateNextFreeOffset(8+nextFreeOffset-addr);
            
            leaf.updateNumKeys(mid-1);
            leaf.updateNextFreeOffset(addr);
            leaf.insert(key,block_id);
        }
        else
        {
            for (int i = 0; i < mid; i++) {
                lenKey = leaf.get_data(addr+2, 2);
                int length = (lenKey[0] << 8) | (lenKey[1] & 0xFF);
                keys[i] = (T) leaf.convertBytesToT(leaf.get_data(addr + 4, length), this.typeClass);
                addr += 4 + length;
            }

            byte[] nextFreeOffsetBytes = leaf.get_data(6, 2);
            int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);

            node.write_data(8,leaf.get_data(addr,nextFreeOffset-addr));
            node.updateNumKeys(numKeys-mid);
            node.updateNextFreeOffset(8+nextFreeOffset-addr);
            node.insert(key,block_id);
            
            leaf.updateNumKeys(mid);
            leaf.updateNextFreeOffset(addr);
        }
        
        byte[] next_id = leaf.get_data(4,2);
        int id_next = (next_id[0] << 8) | (next_id[1] & 0xFF);

        leaf.setNext(node_id);
        node.setPrev(id);
        if(id_next!=0)
        {
            LeafNode next = (LeafNode) blocks.get(id_next);
            node.setNext(id_next);
            next.setPrev(node_id);
            blocks.set(id_next,next);
        }

        blocks.set(id,leaf);
        blocks.add(node);

        T key_to_insert = ((T[]) ((LeafNode) blocks.get(node_id)).getKeys())[0];
        int block_id_to_insert = node_id;
        while(parent.size()>0)
        {
            int pid = parent.remove(parent.size()-1);
            if(!isFull(pid))
            {
                InternalNode pnode = (InternalNode) blocks.get(pid);
                pnode.insert(key_to_insert,block_id_to_insert);
                blocks.set(pid,pnode);
                break;
            }
            else if(pid!=getRootId())
            {
                InternalNode pnode = (InternalNode) blocks.get(pid);
                keys = (T[]) pnode.getKeys();
                int[] children = pnode.getChildren();
                T[] allKeys = (T[]) new Object[keys.length+1];
                int[] allChildren = new int[children.length+1];
                allChildren[0] = children[0];

                int i = 0;
                while(i<numKeys){
                    if(leaf.compare(keys[i],key_to_insert,this.typeClass)>=0) break;
                    allKeys[i] = keys[i];
                    allChildren[i+1] = children[i+1];
                    i++;
                }

                allKeys[i] = key_to_insert;
                allChildren[i+1] = block_id_to_insert;
                while(i<numKeys){
                    allKeys[i+1] = keys[i];
                    allChildren[i+2] = children[i+1];
                    i++;
                }

                InternalNode left = new InternalNode(allKeys[0],allChildren[0],allChildren[1],this.typeClass);
                mid = allKeys.length/2;
                for(int j=1;j<mid;j++) left.insert(allKeys[j],allChildren[j+1]);
    
                InternalNode right = new InternalNode(allKeys[mid+1],allChildren[mid+1],allChildren[mid+2],this.typeClass);
                for(int j=mid+2;j<allKeys.length;j++) right.insert(allKeys[j],allChildren[j+1]);

                key_to_insert = allKeys[mid];
                block_id_to_insert = blocks.size();
                blocks.set(pid,left);
                blocks.add(right);
            }
            else
            {
                InternalNode pnode = (InternalNode) blocks.get(pid);
                keys = (T[]) pnode.getKeys();
                int[] children = pnode.getChildren();
                T[] allKeys = (T[]) new Object[keys.length+1];
                int[] allChildren = new int[children.length+1];
                allChildren[0] = children[0];

                int i = 0;
                while(i<numKeys){
                    if(leaf.compare(keys[i],key_to_insert,this.typeClass)>=0) break;
                    allKeys[i] = keys[i];
                    allChildren[i+1] = children[i+1];
                    i++;
                }

                allKeys[i] = key_to_insert;
                allChildren[i+1] = block_id_to_insert;
                while(i<numKeys){
                    allKeys[i+1] = keys[i];
                    allChildren[i+2] = children[i+1];
                    i++;
                }

                InternalNode left = new InternalNode(allKeys[0],allChildren[0],allChildren[1],this.typeClass);
                mid = allKeys.length/2;
                for(int j=1;j<mid;j++) left.insert(allKeys[j],allChildren[j+1]);
    
                InternalNode right = new InternalNode(allKeys[mid+1],allChildren[mid+1],allChildren[mid+2],this.typeClass);
                for(int j=mid+2;j<allKeys.length;j++) right.insert(allKeys[j],allChildren[j+1]);

                InternalNode newRoot = new InternalNode(allKeys[mid],pid,blocks.size(),this.typeClass);
                key_to_insert = allKeys[mid];
                block_id_to_insert = blocks.size();
                blocks.set(pid,left);
                blocks.add(right);
                blocks.add(newRoot);
                int new_root_id = blocks.size()-1;
                byte[] rootIdBytes = new byte[2];
                rootIdBytes[0] = (byte) ((new_root_id >> 8) & 0xFF);
                rootIdBytes[1] = (byte) (new_root_id & 0xFF);
                BlockNode meta = blocks.get(0);
                meta.write_data(2, rootIdBytes);
                blocks.set(0,meta);
            }
        }
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        int id = getRootId();
        while(!isLeaf(id)) {
            InternalNode node = (InternalNode) blocks.get(id);
            id = node.search(key);
        }
        
        LeafNode node = (LeafNode) blocks.get(id);
        if(node.search(key) == -1)
        {
            T[] keys = (T[]) node.getKeys();
            if(node.compare(key, keys[keys.length-1],this.typeClass) > 0)
            {
                int next = node.getNext();
                if(next == 0) return -1;
                return next;
            }
            return id;
        }
        
        int prev = node.getPrev();
        while(prev != 0) {
            LeafNode prevNode = (LeafNode) blocks.get(prev);
            if(prevNode.search(key)==-1) break;
            node = prevNode;
            id = prev;
            prev = node.getPrev();
        }
        return id;
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}