package index.bplusTree;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */
        if (typeClass == Integer.class) {
            return typeClass.cast(((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF));
        } 
        else if (typeClass == String.class) {
            return typeClass.cast(new String(bytes));
        } 
        else if (typeClass == Float.class) {
            int intValue = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
            float floatValue = Float.intBitsToFloat(intValue);
            return typeClass.cast(floatValue);
        }
        else if (typeClass == Double.class) {
            long longValue = (((long)(bytes[0] & 0xFF)) << 56) | (((long)(bytes[1] & 0xFF)) << 48) | (((long)(bytes[2] & 0xFF)) << 40) | (((long)(bytes[3] & 0xFF)) << 32) | (((long)(bytes[4] & 0xFF)) << 24) | (((long)(bytes[5] & 0xFF)) << 16) | (((long)(bytes[6] & 0xFF)) << 8) | ((long)(bytes[7] & 0xFF));
            double doubleValue = Double.longBitsToDouble(longValue);
            return typeClass.cast(doubleValue);
        } 
        else if (typeClass == Boolean.class) {
            return typeClass.cast(bytes[0] != 0);
        }  
        else {
            throw new IllegalArgumentException("Invalid type class: " + typeClass);
        }
    }
    
    default public byte[] convertTToBytes(T key, Class<T> typeClass){
        
        /* Write your code here */
        if (typeClass == Integer.class) {
            Integer val = (Integer) key;
            byte[] bytes = new byte[4];
            bytes[0] = (byte) (val >> 24);
            bytes[1] = (byte) (val >> 16);
            bytes[2] = (byte) (val >> 8);
            bytes[3] = (byte) (val & 0xFF);
            return bytes;
        } 
        else if (typeClass == String.class) {
            return ((String) key).getBytes();
        } 
        else if (typeClass == Float.class) {
            Float floatValue = (Float) key;
            int intValue = Float.floatToIntBits(floatValue);
            byte[] bytes = new byte[4];
            bytes[0] = (byte) (intValue >> 24);
            bytes[1] = (byte) (intValue >> 16);
            bytes[2] = (byte) (intValue >> 8);
            bytes[3] = (byte) intValue;
            return bytes;
        }
        else if (typeClass == Double.class) {
            Double doubleValue = (Double) key;
            long longValue = Double.doubleToLongBits(doubleValue);
            byte[] bytes = new byte[8];
            bytes[0] = (byte) (longValue >> 56);
            bytes[1] = (byte) (longValue >> 48);
            bytes[2] = (byte) (longValue >> 40);
            bytes[3] = (byte) (longValue >> 32);
            bytes[4] = (byte) (longValue >> 24);
            bytes[5] = (byte) (longValue >> 16);
            bytes[6] = (byte) (longValue >> 8);
            bytes[7] = (byte) longValue;
            return bytes;
        } 
        else if (typeClass == Boolean.class) {
            Boolean boolValue = (Boolean) key;
            byte[] bytes = new byte[1];
            bytes[0] = (byte) (boolValue ? 1 : 0);
            return bytes;
        }  
        else {
            throw new IllegalArgumentException("Invalid type class: " + typeClass);
        }
    }

    default public int compare(T key, T otherKey,Class<T> typeClass){
        if (typeClass == Integer.class) {
            Integer intKey = (Integer) key;
            Integer otherIntKey = (Integer) otherKey;
            return intKey.compareTo(otherIntKey);
        } 
        else if (typeClass == String.class) {
            String strKey = (String) key;
            String otherStrKey = (String) otherKey;
            return strKey.compareTo(otherStrKey);
        } 
        else if (typeClass == Float.class) {
            Float floatKey = (Float) key;
            Float otherFloatKey = (Float) otherKey;
            return floatKey.compareTo(otherFloatKey);
        } 
        else if (typeClass == Double.class) {
            Double doubleKey = (Double) key;
            Double otherDoubleKey = (Double) otherKey;
            return doubleKey.compareTo(otherDoubleKey);
        } 
        else if (typeClass == Boolean.class) {
            Boolean doubleKey = (Boolean) key;
            Boolean otherDoubleKey = (Boolean) otherKey;
            return doubleKey.compareTo(otherDoubleKey);
        }  
        else {
            throw new IllegalArgumentException("Invalid type class: " + typeClass);
        }
    }
}