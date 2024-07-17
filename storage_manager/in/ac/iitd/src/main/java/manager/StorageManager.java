package manager;

import index.bplusTree.BPlusTreeIndexFile;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    public int getFileId(String table_name) {
        return file_to_fileid.get(table_name);
    }
    
    public int getIndex(String table_name, String column_name) {
        int id = file_to_fileid.get(table_name);
        byte[] numColumnsBytes = db.get_data(id,0,0,2);
        int numColumns = (numColumnsBytes[1] << 8) | (numColumnsBytes[0] & 0xFF);

        int index = 0;
        for(int i = 0; i < numColumns; i++) {
            byte[] offsetBytes = db.get_data(id,0,2*i+2,2);
            int offset = (offsetBytes[1] << 8) | (offsetBytes[0] & 0xFF);            
            int length = (int) db.get_data(id,0,offset+1,1)[0];

            byte[] data = db.get_data(id,0,offset+2,length);
            String name = String.class.cast(new String(data));
            if(name.equals(column_name)) break;
            index++;
        }
        return index;
    }

    public int getType(String table_name, String column_name) {
        int id = file_to_fileid.get(table_name);
        byte[] numColumnsBytes = db.get_data(id,0,0,2);
        int numColumns = (numColumnsBytes[1] << 8) | (numColumnsBytes[0] & 0xFF);

        int type = 0;
        for(int i = 0; i < numColumns; i++) {
            byte[] offsetBytes = db.get_data(id,0,2*i+2,2);
            int offset = (offsetBytes[1] << 8) | (offsetBytes[0] & 0xFF);
            
            type = db.get_data(id,0,offset,1)[0];
            int length = (int) db.get_data(id,0,offset+1,1)[0];

            byte[] data = db.get_data(id,0,offset+2,length);
            String name = String.class.cast(new String(data));
            if(name.equals(column_name)) break;
        }
        return type;
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise
        if(!check_file_exists(table_name)) return null;

        int file_id = file_to_fileid.get(table_name);
        
        byte[] numRecordsBytes = db.get_data(file_id,block_id,0,2);
        int numRecords = (numRecordsBytes[0] << 8) | (numRecordsBytes[1] & 0xFF);
        
        int[] offsets = new int[numRecords];

        for(int i = 0; i < numRecords; i++) {
            byte[] offset = db.get_data(file_id,block_id,2*i+2,2);
            offsets[i] = (offset[0] << 8) | (offset[1] & 0xFF);
        }

        List<byte[]> recordBytes = new ArrayList<byte[]>();
        int last = db.get_data(file_id,block_id).length;
        for(int i = 0; i < numRecords; i++) {
            int offset = offsets[i];
            byte[] record = db.get_data(file_id,block_id,offset,last-offset);
            last = offset;
            recordBytes.add(record);
        }

        byte[] numColumnsBytes = db.get_data(file_id,0,0,2);
        int numColumns = (numColumnsBytes[1] << 8) | (numColumnsBytes[0] & 0xFF);

        int variable = 0;
        ArrayList<Integer> fixColType = new ArrayList<Integer>();
        for(int i = 0; i < numColumns; i++) {
            byte[] offsetBytes = db.get_data(file_id,0,2*i+2,2);
            int offset = (offsetBytes[1] << 8) | (offsetBytes[0] & 0xFF);

            int type = db.get_data(file_id,0,offset,1)[0];
            if(type>0) fixColType.add(type);
            else variable++;
        }

        int varp = 0;
        int fixp = 4*variable;
        List<Object[]> records = new ArrayList<Object[]>();
        for(int i=0;i<numRecords;i++)
        {
            Object[] record = new Object[numColumns];
            byte[] bytes = recordBytes.get(i);

            varp = 0;
            fixp = 4*variable;

            for(int j=0;j<numColumns-variable;j++)
            {
                int t = fixColType.get(j);
                switch(t)
                {
                    case 1:
                        record[j] = Integer.class.cast(((bytes[fixp+3] & 0xFF) << 24) | ((bytes[fixp+2] & 0xFF) << 16) | ((bytes[fixp+1] & 0xFF) << 8) | (bytes[fixp] & 0xFF));
                        fixp += 4;
                        break;
                    case 2:
                        record[j] = Boolean.class.cast(bytes[fixp]!=0);
                        fixp++;
                        break;
                    case 3:
                        int intValue = ((bytes[fixp+3] & 0xFF) << 24) | ((bytes[fixp+2] & 0xFF) << 16) | ((bytes[fixp+1] & 0xFF) << 8) | (bytes[fixp] & 0xFF);
                        record[j] = Float.class.cast(Float.intBitsToFloat(intValue));
                        fixp+=4;
                        break;
                    case 4:
                        long longValue = (((long)(bytes[fixp+7] & 0xFF)) << 56) | (((long)(bytes[fixp+6] & 0xFF)) << 48) | (((long)(bytes[fixp+5] & 0xFF)) << 40) | (((long)(bytes[fixp+4] & 0xFF)) << 32) | (((long)(bytes[fixp+3] & 0xFF)) << 24) | (((long)(bytes[fixp+2] & 0xFF)) << 16) | (((long)(bytes[fixp+1] & 0xFF)) << 8) | ((long)(bytes[fixp] & 0xFF));
                        record[j] = Double.class.cast(Double.longBitsToDouble(longValue));
                        fixp+=8;
                        break;
                    default:
                        throw new RuntimeException("Unsupported type");
                }
            }

            for(int j=0;j<variable;j++)
            {
                int offset = (bytes[varp+1] << 8) | (bytes[varp] & 0xFF);
                int length = (bytes[varp+3] << 8) | (bytes[varp+2] & 0xFF);
                
                byte[] data = new byte[length];
                for(int k=0;k<length;k++) data[k] = bytes[k+offset];
                
                record[j+numColumns-variable] = String.class.cast(new String(data));
                varp+=4;
            }
            
            int bitmapLength = (numColumns+7)/8;
            byte[] nullBitmap = new byte[bitmapLength];
            for(int k=0;k<bitmapLength;k++) nullBitmap[k] = bytes[k+fixp];
            List<Boolean> bitmap = new ArrayList<Boolean>();
            for(int j=0;j<numColumns;j++)
            {
                Boolean val = Boolean.class.cast((nullBitmap[j/8] & (1 << (7-(j%8)))) != 0);
                bitmap.add(val);
            }
            for(int j=0;j<numColumns;j++)
            {
                if(bitmap.get(j)) record[j] = null;
            }
            
            records.add(record);
        }
        return records;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        if(!check_file_exists(table_name)) return false;

        int id = getFileId(table_name);
        int index = getIndex(table_name, column_name);
        int type = getType(table_name, column_name);

        BPlusTreeIndexFile<String>  index_file_0 = null;
        BPlusTreeIndexFile<Integer> index_file_1 = null;
        BPlusTreeIndexFile<Boolean> index_file_2 = null;
        BPlusTreeIndexFile<Float>   index_file_3 = null;
        BPlusTreeIndexFile<Double>  index_file_4 = null;
        switch(type) {
            case 0:
                index_file_0 = new BPlusTreeIndexFile<String>(order, String.class);
                break;
            case 1:
                index_file_1 = new BPlusTreeIndexFile<Integer>(order, Integer.class);
                break;
            case 2:
                index_file_2 = new BPlusTreeIndexFile<Boolean>(order, Boolean.class);
                break;
            case 3:
                index_file_3 = new BPlusTreeIndexFile<Float>(order, Float.class);
                break;
            case 4:
                index_file_4 = new BPlusTreeIndexFile<Double>(order, Double.class);
                break;
            default:
                throw new RuntimeException("Unsupported type");
        }

        int num_blocks = 1;
        while (db.get_data(id, num_blocks) != null) {
            List<Object[]> records = get_records_from_block(table_name, num_blocks);
            for (Object[] record : records) {
                Object value = record[index];
                if(value == null) continue;
                switch (type) {
                    case 0:
                        index_file_0.insert((String) value, num_blocks);
                        break;
                    case 1:
                        index_file_1.insert((Integer) value, num_blocks);
                        break;
                    case 2:
                        index_file_2.insert((Boolean) value, num_blocks);
                        break;
                    case 3:
                        index_file_3.insert((Float) value, num_blocks);
                        break;
                    case 4:
                        index_file_4.insert((Double) value, num_blocks);
                        break;
                    default:
                        throw new RuntimeException("Unsupported type");
                }
            }
            num_blocks++;
        }


        String index_file_name = table_name + "_" + column_name + "_index";
        int file_id = -1;
        switch (type) {
            case 0:
                file_id = db.addFile(index_file_0);
                break;
            case 1:
                file_id = db.addFile(index_file_1);
                break;
            case 2:
                file_id = db.addFile(index_file_2);
                break;
            case 3:
                file_id = db.addFile(index_file_3);
                break;
            case 4:
                file_id = db.addFile(index_file_4);
                break;
            default:
                throw new RuntimeException("Unsupported type");
        }
        file_to_fileid.put(index_file_name, file_id);
        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        if(!check_index_exists(table_name,column_name)) return -1;
        int index_file = file_to_fileid.get(table_name + "_" + column_name + "_index");
        return db.search_index(index_file,value);
    }

    public <T> int search_by_value(String table_name,String column_name,T key)
    {
        if(!check_index_exists(table_name,column_name)) return -1;
        int index_file = file_to_fileid.get(table_name + "_" + column_name + "_index");
        return db.search_index(index_file,key);
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}