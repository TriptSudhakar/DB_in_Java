package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;

import java.util.*;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }
    List<Object[]> inputList = new ArrayList<>();
    HashMap<List<Object>, List<Object[]>> keyMap = new HashMap<>();
    List<List<Object>> outputList = new ArrayList<>();
    int outputCounter = 0;

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    public <T> int compare(T key, T otherKey,Class<T> typeClass){
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

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        /* Write your code here */
        if(!(((PRel) getInput()).open())) return false;
        while(((PRel) getInput()).hasNext())
        {
            Object[] row = ((PRel) getInput()).next();
            inputList.add(row);
        }

        List<Integer> groupList = getGroupSet().asList();
        List<AggregateCall> aggCallsList = getAggCallList();

        if(groupList.size()==0)
        {
            List<Object> output = new ArrayList<>();
            for(int i=0;i<aggCallsList.size();i++)
            {
                AggregateCall call = aggCallsList.get(i);
                int index = -1;
                RelDataType type = null;

                switch (call.getAggregation().getName())
                {
                    case "COUNT" :
                        List<Integer> indexList = call.getArgList();
                        if(indexList.size() == 0)
                        {
                            List<List<Object>> rowList = new ArrayList<>();
                            for(Object[] row:inputList)
                            {
                                List<Object> listRow = new ArrayList<>();
                                boolean isNull = true;
                                for(Object obj : row) {
                                    if(obj != null) isNull = false;
                                    listRow.add(obj);
                                }
                                if(!isNull) rowList.add(listRow);
                            }
                            output.add(rowList.size());
                        }
                        else
                        {
                            if(call.isDistinct())
                            {
                                Set<List<Object>> set = new HashSet<>();
                                for(Object[] row:inputList)
                                {
                                    List<Object> listRow = new ArrayList<>();
                                    boolean isNull = true;
                                    for(int keyIndex : indexList) {
                                         if(row[keyIndex] != null) isNull = false;
                                        listRow.add(row[keyIndex]);
                                    }
                                    if(!isNull) set.add(listRow);
                                }
                                output.add(set.size());
                            }
                            else
                            {
                                List<List<Object>> rowList = new ArrayList<>();
                                for(Object[] row:inputList)
                                {
                                    List<Object> listRow = new ArrayList<>();
                                    boolean isNull = true;
                                    for(int keyIndex : indexList) {
                                         if(row[keyIndex] != null) isNull = false;
                                        listRow.add(row[keyIndex]);
                                    }
                                    if(!isNull) rowList.add(listRow);
                                }
                                output.add(rowList.size());
                            }
                        }
                        break;
                    case "MAX" :
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        switch (type.getSqlTypeName()) {
                            case BIGINT:
                            case INTEGER:
                                Integer maxVal = (Integer) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        maxVal = null;
                                        break;
                                    }
                                    Integer val = (Integer) row[index];
                                    if(compare(val,maxVal,Integer.class)>0) maxVal = val;
                                }
                                output.add(maxVal);
                                break;
                            case CHAR:
                            case VARCHAR:
                                String maxStr = (String) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        maxStr = null;
                                        break;
                                    }
                                    String val = (String) row[index];
                                    if(compare(val,maxStr,String.class)>0) maxStr = val;
                                }
                                output.add(maxStr);
                                break;
                            case BOOLEAN:
                                Boolean maxBool = (Boolean) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        maxBool = null;
                                        break;
                                    }
                                    Boolean val = (Boolean) row[index];
                                    if(compare(val,maxBool,Boolean.class)>0) maxBool = val;
                                }
                                output.add(maxBool);
                                break;
                            case REAL:
                            case FLOAT:
                                Float maxFloat = (Float) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        maxFloat = null;
                                        break;
                                    }
                                    Float val = (Float) row[index];
                                    if(compare(val,maxFloat,Float.class)>0) maxFloat = val;
                                }
                                output.add(maxFloat);
                                break;
                            case DOUBLE:
                            case DECIMAL:
                                Double maxDouble = (Double) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        maxDouble = null;
                                        break;
                                    }
                                    Double val = (Double) row[index];
                                    if(compare(val,maxDouble,Double.class)>0) maxDouble = val;
                                }
                                output.add(maxDouble);
                                break;
                            default:
                                System.out.println("Unknown or unsupported column type");
                                break;
                        }
                        break;
                    case "MIN" :
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        switch (type.getSqlTypeName()) {
                            case BIGINT:
                            case INTEGER:
                                Integer minVal = (Integer) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        minVal = null;
                                        break;
                                    }
                                    Integer val = (Integer) row[index];
                                    if(compare(val,minVal,Integer.class)<0) minVal = val;
                                }
                                output.add(minVal);
                                break;
                            case CHAR:
                            case VARCHAR:
                                String minStr = (String) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        minStr = null;
                                        break;
                                    }
                                    String val = (String) row[index];
                                    if(compare(val,minStr,String.class)<0) minStr = val;
                                }
                                output.add(minStr);
                                break;
                            case BOOLEAN:
                                Boolean minBool = (Boolean) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        minBool = null;
                                        break;
                                    }
                                    Boolean val = (Boolean) row[index];
                                    if(compare(val,minBool,Boolean.class)<0) minBool = val;
                                }
                                output.add(minBool);
                                break;
                            case REAL:
                            case FLOAT:
                                Float minFloat = (Float) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        minFloat = null;
                                        break;
                                    }
                                    Float val = (Float) row[index];
                                    if(compare(val,minFloat,Float.class)<0) minFloat = val;
                                }
                                output.add(minFloat);
                                break;
                            case DOUBLE:
                            case DECIMAL:
                                Double minDouble = (Double) inputList.get(0)[index];
                                for(Object[] row : inputList)
                                {
                                    if(row[index] == null)
                                    {
                                        minDouble = null;
                                        break;
                                    }
                                    Double val = (Double) row[index];
                                    if(compare(val,minDouble,Double.class)<0) minDouble = val;
                                }
                                output.add(minDouble);
                                break;
                            default:
                                System.out.println("Unknown or unsupported column type");
                                break;
                        }
                        break;
                    case "SUM" :
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        if(call.isDistinct())
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Set<Integer> sumSet = new HashSet<>();
                                    Integer sumVal = 0;

                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        if(sumSet.contains(val)) continue;
                                        sumSet.add(val);
                                        sumVal += val;
                                    }
                                    output.add(sumVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Set<Float> sumFloatSet = new HashSet<>();
                                    Float sumFloat = 0.0f;

                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        if(sumFloatSet.contains(val)) continue;
                                        sumFloatSet.add(val);
                                        sumFloat += val;
                                    }
                                    output.add(sumFloat);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Set<Double> sumDoubleSet = new HashSet<>();
                                    Double sumDouble = 0.0;

                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        if(sumDoubleSet.contains(val)) continue;
                                        sumDoubleSet.add(val);
                                        sumDouble += val;
                                    }
                                    output.add(sumDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        else
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Integer sumVal = 0;
                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        sumVal += val;
                                    }
                                    output.add(sumVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Float sumFloat = 0.0f;
                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        sumFloat += val;
                                    }
                                    output.add(sumFloat);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Double sumDouble = 0.0;
                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        sumDouble += val;
                                    }
                                    output.add(sumDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        break;
                    case "AVG" :
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        if(call.isDistinct())
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Set<Integer> avgSet = new HashSet<>();
                                    Integer avgVal = 0;

                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        if(avgSet.contains(val)) continue;
                                        avgSet.add(val);
                                        avgVal += val;
                                    }

                                    Double avgDoubleVal = Double.valueOf(avgVal);
                                    avgDoubleVal /= avgSet.size();
                                    output.add(avgDoubleVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Set<Float> avgFloatSet = new HashSet<>();
                                    Float avgFloat = 0.0f;

                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        if(avgFloatSet.contains(val)) continue;
                                        avgFloatSet.add(val);
                                        avgFloat += val;
                                    }

                                    Double avgDoubleFloatVal = Double.valueOf(avgFloat);
                                    avgDoubleFloatVal /= avgFloatSet.size();
                                    output.add(avgDoubleFloatVal);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Set<Double> avgDoubleSet = new HashSet<>();
                                    Double avgDouble = 0.0;

                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        if(avgDoubleSet.contains(val)) continue;
                                        avgDoubleSet.add(val);
                                        avgDouble += val;
                                    }

                                    avgDouble /= avgDoubleSet.size();
                                    output.add(avgDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        else
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Integer avgVal = 0;
                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        avgVal += val;
                                    }
                                    
                                    Double avgDoubleVal = Double.valueOf(avgVal);
                                    avgDoubleVal /= inputList.size();
                                    output.add(avgDoubleVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Float avgFloat = 0.0f;
                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        avgFloat += val;
                                    }
                                    
                                    Double avgDoubleFloatVal = Double.valueOf(avgFloat);
                                    avgDoubleFloatVal /= inputList.size();
                                    output.add(avgDoubleFloatVal);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Double avgDouble = 0.0;
                                    for(Object[] row : inputList)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        avgDouble += val;
                                    }
                                    
                                    avgDouble /= inputList.size();
                                    output.add(avgDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        break;
                }
            }

            outputList.add(output);
            return true;
        }

        for(int i=0;i<inputList.size();i++)
        {
            Object[] row = inputList.get(i);
            List<Object> key = new ArrayList<>();
            for(int j=0;j<groupList.size();j++)
            {
                int index = groupList.get(j);
                key.add(row[index]);
            }

            if(keyMap.containsKey(key))
            {
                List<Object[]> entries = keyMap.get(key);
                entries.add(row);
                keyMap.put(key, entries);
            }
            else
            {
                List<Object[]> entries = new ArrayList<>();
                entries.add(row);
                keyMap.put(key,entries);
            }
        }

        for(Map.Entry<List<Object>, List<Object[]>> entry : keyMap.entrySet())
        {
            outputList.add(entry.getKey());
        }

        int counter = 0;
        for(Map.Entry<List<Object>, List<Object[]>> entry : keyMap.entrySet())
        {
            List<Object> output = outputList.get(counter);
            List<Object[]> rows = entry.getValue();

            for(int i=0;i<aggCallsList.size();i++)
            {
                AggregateCall call = aggCallsList.get(i);
                int index = -1;
                RelDataType type = null;

                switch (call.getAggregation().getName())
                {
                    case "COUNT" :
                        List<Integer> indexList = call.getArgList();

                        if(indexList.size() == 0)
                        {
                            List<List<Object>> rowList = new ArrayList<>();
                            for(Object[] row:rows)
                            {
                                List<Object> listRow = new ArrayList<>();
                                boolean isNull = true;
                                for(Object obj : row) {
                                    if(obj != null) isNull = false;
                                    listRow.add(obj);
                                }
                                if(!isNull) rowList.add(listRow);
                            }
                            output.add(rowList.size());
                        }
                        else
                        {
                            if(call.isDistinct())
                            {
                                Set<List<Object>> set = new HashSet<>();
                                for(Object[] row:rows)
                                {
                                    List<Object> listRow = new ArrayList<>();
                                    boolean isNull = true;
                                    for(int keyIndex : indexList) {
                                         if(row[keyIndex] != null) isNull = false;
                                        listRow.add(row[keyIndex]);
                                    }
                                    if(!isNull) set.add(listRow);
                                }
                                output.add(set.size());
                            }
                            else
                            {
                                List<List<Object>> rowList = new ArrayList<>();
                                for(Object[] row:rows)
                                {
                                    List<Object> listRow = new ArrayList<>();
                                     boolean isNull = true;
                                    for(int keyIndex : indexList) {
                                        if(row[keyIndex] != null) isNull = false;
                                        listRow.add(row[keyIndex]);
                                    }
                                    if(!isNull) rowList.add(listRow);
                                }
                                output.add(rowList.size());
                            }
                        }
                        break;
                    case "MAX" :
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        switch (type.getSqlTypeName()) {
                            case BIGINT:
                            case INTEGER:
                                Integer maxVal = (Integer) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        maxVal = null;
                                        break;
                                    }
                                    Integer val = (Integer) row[index];
                                    if(compare(val,maxVal,Integer.class)>0) maxVal = val;
                                }
                                output.add(maxVal);
                                break;
                            case CHAR:
                            case VARCHAR:
                                String maxStr = (String) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        maxStr = null;
                                        break;
                                    }
                                    String val = (String) row[index];
                                    if(compare(val,maxStr,String.class)>0) maxStr = val;
                                }
                                output.add(maxStr);
                                break;
                            case BOOLEAN:
                                Boolean maxBool = (Boolean) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        maxBool = null;
                                        break;
                                    }
                                    Boolean val = (Boolean) row[index];
                                    if(compare(val,maxBool,Boolean.class)>0) maxBool = val;
                                }
                                output.add(maxBool);
                                break;
                            case REAL:
                            case FLOAT:
                                Float maxFloat = (Float) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        maxFloat = null;
                                        break;
                                    }
                                    Float val = (Float) row[index];
                                    if(compare(val,maxFloat,Float.class)>0) maxFloat = val;
                                }
                                output.add(maxFloat);
                                break;
                            case DOUBLE:
                            case DECIMAL:
                                Double maxDouble = (Double) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        maxDouble = null;
                                        break;
                                    }
                                    Double val = (Double) row[index];
                                    if(compare(val,maxDouble,Double.class)>0) maxDouble = val;
                                }
                                output.add(maxDouble);
                                break;
                            default:
                                System.out.println("Unknown or unsupported column type");
                                break;
                        }
                        break;
                    case "MIN":
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        switch (type.getSqlTypeName()) {
                            case BIGINT:
                            case INTEGER:
                                Integer minVal = (Integer) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        minVal = null;
                                        break;
                                    }
                                    Integer val = (Integer) row[index];
                                    if(compare(val,minVal,Integer.class)<0) minVal = val;
                                }
                                output.add(minVal);
                                break;
                            case CHAR:
                            case VARCHAR:
                                String minStr = (String) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        minStr = null;
                                        break;
                                    }
                                    String val = (String) row[index];
                                    if(compare(val,minStr,String.class)<0) minStr = val;
                                }
                                output.add(minStr);
                                break;
                            case BOOLEAN:
                                Boolean minBool = (Boolean) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        minBool = null;
                                        break;
                                    }
                                    Boolean val = (Boolean) row[index];
                                    if(compare(val,minBool,Boolean.class)<0) minBool = val;
                                }
                                output.add(minBool);
                                break;
                            case REAL:
                            case FLOAT:
                                Float minFloat = (Float) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        minFloat = null;
                                        break;
                                    }
                                    Float val = (Float) row[index];
                                    if(compare(val,minFloat,Float.class)<0) minFloat = val;
                                }
                                output.add(minFloat);
                                break;
                            case DOUBLE:
                            case DECIMAL:
                                Double minDouble = (Double) rows.get(0)[index];
                                for(Object[] row : rows)
                                {
                                    if(row[index] == null)
                                    {
                                        minDouble = null;
                                        break;
                                    }
                                    Double val = (Double) row[index];
                                    if(compare(val,minDouble,Double.class)<0) minDouble = val;
                                }
                                output.add(minDouble);
                                break;
                            default:
                                System.out.println("Unknown or unsupported column type");
                                break;
                        }
                        break;
                    case "SUM":
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        if(call.isDistinct())
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Integer sumVal = 0;
                                    Set<Integer> setInt = new HashSet<>();
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        if(setInt.contains(val)) continue;
                                        setInt.add(val);
                                        sumVal += val;
                                    }
                                    output.add(sumVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Float sumFloat = 0.0f;
                                    Set<Float> setFloat = new HashSet<>();
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        if(setFloat.contains(val)) continue;
                                        setFloat.add(val);
                                        sumFloat += val;
                                    }
                                    output.add(sumFloat);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Double sumDouble = 0.0;
                                    Set<Double> setDouble = new HashSet<>();
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        if(setDouble.contains(val)) continue;
                                        setDouble.add(val);
                                        sumDouble += val;
                                    }
                                    output.add(sumDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        else
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Integer sumVal = 0;
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        sumVal += val;
                                    }
                                    output.add(sumVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Float sumFloat = 0.0f;
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        sumFloat += val;
                                    }
                                    output.add(sumFloat);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Double sumDouble = 0.0;
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        sumDouble += val;
                                    }
                                    output.add(sumDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        break;
                    case "AVG":
                        index = call.getArgList().get(0);
                        type = getInput().getRowType().getFieldList().get(index).getType();

                        if(call.isDistinct())
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Integer sum = 0;
                                    Set<Integer> setInt = new HashSet<>();
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        if(setInt.contains(val)) continue;
                                        setInt.add(val);
                                        sum += val;
                                    }
                                    Double avgVal = Double.valueOf(sum) / setInt.size();
                                    output.add(avgVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Float sumFloat = 0.0f;
                                    Set<Float> setFloat = new HashSet<>();
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        if(setFloat.contains(val)) continue;
                                        setFloat.add(val);
                                        sumFloat += val;
                                    }
                                    Double avgFloat = Double.valueOf(sumFloat) / setFloat.size();
                                    output.add(avgFloat);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Double sumDouble = 0.0;
                                    Set<Double> setDouble = new HashSet<>();
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        if(setDouble.contains(val)) continue;
                                        setDouble.add(val);
                                        sumDouble += val;
                                    }
                                    Double avgDouble = Double.valueOf(sumDouble) / setDouble.size();
                                    output.add(avgDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        else
                        {
                            switch (type.getSqlTypeName()) {
                                case BIGINT:
                                case INTEGER:
                                    Integer sumVal = 0;
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Integer val = (Integer) row[index];
                                        sumVal += val;
                                    }
                                    Double avgVal = Double.valueOf(sumVal) / rows.size();
                                    output.add(avgVal);
                                    break;
                                case REAL:
                                case FLOAT:
                                    Float sumFloat = 0.0f;
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Float val = (Float) row[index];
                                        sumFloat += val;
                                    }
                                    Double avgFloat = Double.valueOf(sumFloat) / rows.size();
                                    output.add(avgFloat);
                                    break;
                                case DOUBLE:
                                case DECIMAL:
                                    Double sumDouble = 0.0;
                                    for(Object[] row : rows)
                                    {
                                        if(row[index] == null) continue;
                                        Double val = (Double) row[index];
                                        sumDouble += val;
                                    }
                                    Double avgDouble = Double.valueOf(sumDouble) / rows.size();
                                    output.add(avgDouble);
                                    break;
                                default:
                                    System.out.println("Unknown or unsupported column type");
                                    break;
                            }
                        }
                        break;
                    default:
                        System.out.println("Invalid aggregation type");
                        break;
                }
            }

            outputList.set(counter++, output);
        }

        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
        ((PRel) getInput()).close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        /* Write your code here */
        return outputCounter < outputList.size();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        List<Object> outputRow = outputList.get(outputCounter++);
        return outputRow.toArray();
    }

}