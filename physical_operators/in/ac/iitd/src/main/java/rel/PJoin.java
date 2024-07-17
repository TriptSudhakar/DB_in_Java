package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

import java.util.*;

/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
    }
    List<Object[]> inputList = new ArrayList<>(); // contains the rows of the outer relation
    List<HashMap> mapList = new ArrayList<>(); // contains the list of hashmaps which contain the list of indices according to a particular key
    List<Boolean> found = new ArrayList<>(); // which relations have been used in the inner join
    boolean empty = false;
    boolean leftJoin = false;
    int counter = 0;
    int outerColumnSize = 0;
    int innerColumnSize = 0;
    List<Integer> outerIndices = new ArrayList<>(); // indices of the columns of the outer relation corresponding to the keys used
    List<Integer> innerIndices = new ArrayList<>(); // indices of the columns of the inner relation corresponding to the keys used
    Object[] innerRow = null;
    Deque<Integer> hashIndices = new LinkedList<>();
    boolean crossProduct = false;

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */
        if(!(((PRel) getRight()).open())) return false;
        if(!(((PRel) getLeft()).open())) return false;

        while(((PRel) getLeft()).hasNext())
        {
            Object[] row = ((PRel) getLeft()).next();
            inputList.add(row);
            found.add(false);
        }

        outerColumnSize = ((PRel) getLeft()).getRowType().getFieldCount();
        innerColumnSize = ((PRel) getRight()).getRowType().getFieldCount();
        outerIndices = joinInfo.leftKeys;
        innerIndices = joinInfo.rightKeys;

        if(outerIndices.size()==0 && condition instanceof RexLiteral)
        {
            boolean cond = ((RexLiteral) condition).getValueAs(Boolean.class);
            if(cond) crossProduct = true;
            return true;
        }

        for(int i=0;i<outerIndices.size();i++)
        {
            int outerIndex = outerIndices.get(i);
            RelDataType type = getLeft().getRowType().getFieldList().get(outerIndex).getType();

            switch (type.getSqlTypeName()) {
                case BIGINT:
                case INTEGER:
                    HashMap<Integer, Set<Integer>> imap = new HashMap<>();
                    for(int j=0;j<inputList.size();j++)
                    {
                        Object[] row = inputList.get(j);
                        Integer key = (Integer) row[outerIndex];
                        if(imap.containsKey(key))
                        {
                            Set<Integer> list = imap.get(key);
                            list.add(j);
                            imap.put(key,list);
                        }
                        else imap.put(key, new HashSet<>(Arrays.asList(j)));
                    }
                    mapList.add(imap);
                    break;
                case CHAR:
                case VARCHAR:
                    HashMap<String, Set<Integer>> smap = new HashMap<>();
                    for(int j=0;j<inputList.size();j++)
                    {
                        Object[] row = inputList.get(j);
                        String key = (String) row[outerIndex];
                        if(smap.containsKey(key))
                        {
                            Set<Integer> list = smap.get(key);
                            list.add(j);
                            smap.put(key,list);
                        }
                        else smap.put(key, new HashSet<>(Arrays.asList(j)));
                    }
                    mapList.add(smap);
                    break;
                case BOOLEAN:
                    HashMap<Boolean, Set<Integer>> bmap = new HashMap<>();
                    for(int j=0;j<inputList.size();j++)
                    {
                        Object[] row = inputList.get(j);
                        Boolean key = (Boolean) row[outerIndex];
                        if(bmap.containsKey(key))
                        {
                            Set<Integer> list = bmap.get(key);
                            list.add(j);
                            bmap.put(key,list);
                        }
                        else bmap.put(key, new HashSet<>(Arrays.asList(j)));
                    }
                    mapList.add(bmap);
                    break;
                case REAL:
                case FLOAT:
                    HashMap<Float, Set<Integer>> fmap = new HashMap<>();
                    for(int j=0;j<inputList.size();j++)
                    {
                        Object[] row = inputList.get(j);
                        Float key = (Float) row[outerIndex];
                        if(fmap.containsKey(key))
                        {
                            Set<Integer> list = fmap.get(key);
                            list.add(j);
                            fmap.put(key,list);
                        }
                        else fmap.put(key, new HashSet<>(Arrays.asList(j)));
                    }
                    mapList.add(fmap);
                    break;
                case DOUBLE:
                case DECIMAL:
                    HashMap<Double, Set<Integer>> dmap = new HashMap<>();
                    for(int j=0;j<inputList.size();j++)
                    {
                        Object[] row = inputList.get(j);
                        Double key = (Double) row[outerIndex];
                        if(dmap.containsKey(key))
                        {
                            Set<Integer> list = dmap.get(key);
                            list.add(j);
                            dmap.put(key,list);
                        }
                        else dmap.put(key, new HashSet<>(Arrays.asList(j)));
                    }
                    mapList.add(dmap);
                    break;
                default:
                    System.out.println("Unknown or unsupported column type");
                    break;
            }
        }
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        ((PRel) getLeft()).close();
        ((PRel) getRight()).close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        if(!leftJoin)
        {
            if(hashIndices.size()>0 || empty) return true;
            while(((PRel) getRight()).hasNext())
            {
                innerRow = ((PRel) getRight()).next();
                if(crossProduct)
                {
                    for(int i=0;i<inputList.size();i++)
                    {
                        hashIndices.add(i);
                        found.set(i, true);
                    }
                    return true;
                }

                Set<Integer> indices = new HashSet<>();
                for(int i=0;i<mapList.size();i++)
                {
                    RelDataType type = getLeft().getRowType().getFieldList().get(outerIndices.get(i)).getType();

                    switch (type.getSqlTypeName()) {
                        case BIGINT:
                        case INTEGER:
                            if(mapList.get(i).containsKey((Integer) innerRow[innerIndices.get(i)]))
                            {
                                if(i == 0) indices = ((HashMap<Integer,Set<Integer>>) mapList.get(i)).get((Integer) innerRow[innerIndices.get(i)]);
                                else indices.retainAll(((HashMap<Integer,Set<Integer>>) mapList.get(i)).get((Integer) innerRow[innerIndices.get(i)]));
                            }
                            else indices = new HashSet<>();
                            break;
                        case CHAR:
                        case VARCHAR:
                            if(mapList.get(i).containsKey((String) innerRow[innerIndices.get(i)]))
                            {
                                if(i == 0) indices = ((HashMap<String,Set<Integer>>) mapList.get(i)).get((String) innerRow[innerIndices.get(i)]);
                                else indices.retainAll(((HashMap<String,Set<Integer>>) mapList.get(i)).get((String) innerRow[innerIndices.get(i)]));
                            }
                            else indices = new HashSet<>();
                            break;
                        case BOOLEAN:
                            if(mapList.get(i).containsKey((Boolean) innerRow[innerIndices.get(i)]))
                            {
                                if(i == 0) indices = ((HashMap<Boolean,Set<Integer>>) mapList.get(i)).get((Boolean) innerRow[innerIndices.get(i)]);
                                else indices.retainAll(((HashMap<Boolean,Set<Integer>>) mapList.get(i)).get((Boolean) innerRow[innerIndices.get(i)]));
                            }
                            else indices = new HashSet<>();
                            break;
                        case REAL:
                        case FLOAT:
                            if(mapList.get(i).containsKey((Float) innerRow[innerIndices.get(i)]))
                            {
                                if(i == 0) indices = ((HashMap<Float,Set<Integer>>) mapList.get(i)).get((Float) innerRow[innerIndices.get(i)]);
                                else indices.retainAll(((HashMap<Float,Set<Integer>>) mapList.get(i)).get((Float) innerRow[innerIndices.get(i)]));
                            }
                            else indices = new HashSet<>();
                            break;
                        case DOUBLE:
                        case DECIMAL:
                            if(mapList.get(i).containsKey((Double) innerRow[innerIndices.get(i)]))
                            {
                                if(i == 0) indices = ((HashMap<Double,Set<Integer>>) mapList.get(i)).get((Double) innerRow[innerIndices.get(i)]);
                                else indices.retainAll(((HashMap<Double,Set<Integer>>) mapList.get(i)).get((Double) innerRow[innerIndices.get(i)]));
                            }
                            else indices = new HashSet<>();
                            break;
                        default:
                            System.out.println("Unknown or unsupported column type");
                            break;
                    }
                }

                if(indices.size()>0)
                {
                    for(int index : indices)
                    {
                        hashIndices.add(index);
                        found.set(index, true);
                    }
                    return true;
                }
                if(joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL)
                {
                    empty = true;
                    return true;
                }
            }

            leftJoin = true;
        }

        if(joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL)
        {
            while(counter < inputList.size())
            {
                if(found.get(counter)) counter++;
                else return true;
            }
            return false;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        if(hashIndices.size()>0)
        {
            int rowIndex = hashIndices.poll();
            Object[] outerRow = inputList.get(rowIndex);
            Object[] joinRow = new Object[outerColumnSize + innerColumnSize];
            for(int i=0;i<outerColumnSize;i++)
            {
                joinRow[i] = outerRow[i];
            }
            for(int i=0;i<innerColumnSize;i++)
            {
                joinRow[i+outerColumnSize] = innerRow[i];
            }
            return joinRow;
        }
        if(empty)
        {
            Object[] joinRow = new Object[outerColumnSize + innerColumnSize];
            for(int i=0;i<innerColumnSize;i++)
            {
                joinRow[i+outerColumnSize] = innerRow[i];
            }

            empty = false;
            return joinRow;
        }
        if(leftJoin)
        {
            Object[] joinRow = new Object[outerColumnSize + innerColumnSize];
            Object[] outerRow = inputList.get(counter++);

            for(int i=0;i<outerColumnSize;i++)
            {
                joinRow[i] = outerRow[i];
            }
            return joinRow;
        }
        return null;
    }
}
