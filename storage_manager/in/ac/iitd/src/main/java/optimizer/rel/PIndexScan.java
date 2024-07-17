package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;

import manager.StorageManager;
import org.apache.calcite.sql.SqlKind;
import storage.DB;
import index.bplusTree.LeafNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {
    
        private final List<RexNode> projects;
        private final RelDataType rowType;
        private final RelOptTable table;
        private final RexNode filter;
    
        public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
            super(cluster, traitSet, table);
            this.table = table;
            this.rowType = deriveRowType();
            this.filter = filter;
            this.projects = projects;
        }
    
        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PIndexScan(getCluster(), traitSet, table, filter, projects);
        }
    
        @Override
        public RelOptTable getTable() {
            return table;
        }

        @Override
        public String toString() {
            return "PIndexScan";
        }

        public String getTableName() {
            return table.getQualifiedName().get(1);
        }

        @Override
        public List<Object[]> evaluate(StorageManager storage_manager) {
            String tableName = getTableName();
            System.out.println("Evaluating PIndexScan for table: " + tableName);

            /* Write your code here */
            RexLiteral check = (RexLiteral) ((RexCall) filter).getOperands().get(1);
            int columnIndex = ((RexInputRef) (((RexCall) filter).getOperands().get(0))).getIndex();
            String columnName = table.getRowType().getFieldNames().get(columnIndex);
            RelDataType colType = table.getRowType().getFieldList().get(columnIndex).getType();
            DB db = storage_manager.getDb();
            int actualIndex = storage_manager.getIndex(tableName,columnName);

            Class<?> colClass = null;
            Object val = null;
            switch (colType.getSqlTypeName()) {
                case INTEGER:
                    colClass = Integer.class;
                    val = check.getValueAs(Integer.class);
                    break;
                case CHAR:
                case VARCHAR:
                    colClass = String.class;
                    val = check.getValueAs(String.class);
                    break;
                case BOOLEAN:
                    colClass = Boolean.class;
                    val = check.getValueAs(Boolean.class);
                    break;
                case FLOAT:
                    colClass = Float.class;
                    val = check.getValueAs(Float.class);
                    break;
                case DOUBLE:
                case DECIMAL:
                    colClass = Double.class;
                    val = check.getValueAs(Double.class);
                    break;
                default:
                    // Handle default case (unknown or unsupported type)
                    System.out.println("Unknown or unsupported column type");
                    // Add your logic for handling unknown or unsupported types
                    break;
            }

            int file_id = storage_manager.getFileId(tableName);
            int index_file_id = storage_manager.getFileId(tableName + "_" + columnName + "_index");
            int leaf_id = storage_manager.search_by_value(tableName, columnName, colClass.cast(val));

            List<Object[]> result = new ArrayList<Object[]>();
            Set<Integer> blocks = new HashSet<>();

            if(filter.isA(SqlKind.EQUALS))
            {
                if(leaf_id == -1) return result;

                while(leaf_id!=0)
                {
                    LeafNode leaf = new LeafNode(colClass);
                    leaf.write_data(0, db.get_data(index_file_id, leaf_id));
                    if(leaf.search(val)==-1) break;

                    Object[] keys = leaf.getKeys(); 
                    int[] block_ids = leaf.getBlockIds();
                    for(int i=0;i<keys.length;i++)
                    {
                        int cmp = leaf.compare(colClass.cast(keys[i]), val, colClass);
                        if(cmp==0) blocks.add(block_ids[i]);
                    }
                    leaf_id = leaf.getNext();
                }

                LeafNode leaf = new LeafNode(colClass);
                for(int block_id: blocks)
                {
                    List<Object[]> records = storage_manager.get_records_from_block(tableName, block_id);
                    for(int i=0;i<records.size();i++)
                    {
                        Object[] rec = records.get(i);
                        if(rec[actualIndex]==null) continue;
                        if(leaf.compare(colClass.cast(rec[actualIndex]), val, colClass)==0) result.add(records.get(i));
                    }
                }
            }
            else if(filter.isA(SqlKind.GREATER_THAN))
            {
                if(leaf_id == -1) return result;

                while(leaf_id!=0)
                {
                    LeafNode leaf = new LeafNode(colClass);
                    leaf.write_data(0, db.get_data(index_file_id, leaf_id));

                    Object[] keys = leaf.getKeys(); 
                    int[] block_ids = leaf.getBlockIds();
                    for(int i=0;i<keys.length;i++)
                    {
                        int cmp = leaf.compare(colClass.cast(keys[i]), val, colClass);
                        if(cmp>0) blocks.add(block_ids[i]);
                    }

                    leaf_id = leaf.getNext();
                }

                LeafNode leaf = new LeafNode(colClass);
                for(int block_id: blocks)
                {
                    List<Object[]> records = storage_manager.get_records_from_block(tableName, block_id);
                    for(int i=0;i<records.size();i++)
                    {
                        Object[] rec = records.get(i);
                        if(rec[actualIndex]==null) continue;
                        if(leaf.compare(colClass.cast(rec[actualIndex]), val, colClass)>0) result.add(records.get(i));
                    }
                }
            }
            else if(filter.isA(SqlKind.GREATER_THAN_OR_EQUAL))
            {
                if(leaf_id == -1) return result;

                while(leaf_id!=0)
                {
                    LeafNode leaf = new LeafNode(colClass);
                    leaf.write_data(0, db.get_data(index_file_id, leaf_id));

                    Object[] keys = leaf.getKeys(); 
                    int[] block_ids = leaf.getBlockIds();
                    for(int i=0;i<keys.length;i++)
                    {
                        int cmp = leaf.compare(colClass.cast(keys[i]), val, colClass);
                        if(cmp>=0) blocks.add(block_ids[i]);
                    }

                    leaf_id = leaf.getNext();
                }

                LeafNode leaf = new LeafNode(colClass);
                for(int block_id: blocks)
                {
                    List<Object[]> records = storage_manager.get_records_from_block(tableName, block_id);
                    for(int i=0;i<records.size();i++)
                    {
                        Object[] rec = records.get(i);
                        if(rec[actualIndex]==null) continue;
                        if(leaf.compare(colClass.cast(rec[actualIndex]), val, colClass)>=0) result.add(records.get(i));
                    }
                }
            }
            else if(filter.isA(SqlKind.LESS_THAN))
            {
                if(leaf_id==-1)
                {
                    int num_blocks = 1;
                    while(db.get_data(file_id, num_blocks)!=null)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, num_blocks);
                        for(Object[] rec: records) result.add(rec);
                        num_blocks++;
                    }
                    return result;
                }

                while(leaf_id!=0)
                {
                    LeafNode leaf = new LeafNode(colClass);
                    leaf.write_data(0, db.get_data(index_file_id, leaf_id));

                    Object[] keys = leaf.getKeys(); 
                    int[] block_ids = leaf.getBlockIds();
                    for(int i=0;i<keys.length;i++)
                    {
                        int cmp = leaf.compare(colClass.cast(keys[i]), val, colClass);
                        if(cmp<0) blocks.add(block_ids[i]);
                    }

                    leaf_id = leaf.getPrev();
                }

                LeafNode leaf = new LeafNode(colClass);
                for(int block_id: blocks)
                {
                    List<Object[]> records = storage_manager.get_records_from_block(tableName, block_id);
                    for(int i=0;i<records.size();i++)
                    {
                        Object[] rec = records.get(i);
                        if(rec[actualIndex]==null) continue;
                        if(leaf.compare(colClass.cast(rec[actualIndex]), val, colClass)<0) result.add(records.get(i));
                    }
                }
            }
            else
            {
                if(leaf_id==-1)
                {
                    int num_blocks = 1;
                    while(db.get_data(file_id, num_blocks)!=null)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, num_blocks);
                        for(Object[] rec: records) result.add(rec);
                        num_blocks++;
                    }
                    return result;
                }

                int temp = leaf_id;
                while(leaf_id!=0)
                {
                    LeafNode leaf = new LeafNode(colClass);
                    leaf.write_data(0, db.get_data(index_file_id, leaf_id));

                    Object[] keys = leaf.getKeys(); 
                    int[] block_ids = leaf.getBlockIds();
                    for(int i=0;i<keys.length;i++)
                    {
                        int cmp = leaf.compare(colClass.cast(keys[i]), val, colClass);
                        if(cmp<0) blocks.add(block_ids[i]);
                    }

                    leaf_id = leaf.getPrev();
                }

                leaf_id = temp;
                while(leaf_id!=0)
                {
                    LeafNode leaf = new LeafNode(colClass);
                    leaf.write_data(0, db.get_data(index_file_id, leaf_id));
                    if(leaf.search(val)==-1) break;

                    Object[] keys = leaf.getKeys(); 
                    int[] block_ids = leaf.getBlockIds();
                    for(int i=0;i<keys.length;i++)
                    {
                        int cmp = leaf.compare(colClass.cast(keys[i]), val, colClass);
                        if(cmp==0) blocks.add(block_ids[i]);
                    }
                    leaf_id = leaf.getNext();
                }

                LeafNode leaf = new LeafNode(colClass);
                for(int block_id: blocks)
                {
                    List<Object[]> records = storage_manager.get_records_from_block(tableName, block_id);
                    for(int i=0;i<records.size();i++)
                    {
                        Object[] rec = records.get(i);
                        if(rec[actualIndex]==null) continue;
                        if(leaf.compare(colClass.cast(rec[actualIndex]), val, colClass)<=0) result.add(records.get(i));
                    }
                }
            }
            return result;
        }
}