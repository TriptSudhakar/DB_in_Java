package rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel{
    
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
            ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }
    List<Object[]> inputList = new ArrayList<>();
    List<RelFieldCollation> collations = new ArrayList<>();
    int counter = 0;
    int end = 0;

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    public <T> int compareValues(T key, T otherKey,Class<T> typeClass){
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
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        if(!(((PRel) getInput()).open())) return false;
        while(((PRel) getInput()).hasNext())
        {
            Object[] row = ((PRel) getInput()).next();
            inputList.add(row);
        }
        collations = collation.getFieldCollations();
        end = inputList.size();

        if(offset != null) counter = ((RexLiteral) offset).getValueAs(Integer.class);
        if(fetch != null) end = Math.min(end, counter + ((RexLiteral) fetch).getValueAs(Integer.class));

        Collections.sort(inputList, new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                for (RelFieldCollation collation : collations) {
                    int index = collation.getFieldIndex();
                    RelFieldCollation.Direction direction = collation.getDirection();

                    Object left = o1[index];
                    Object right = o2[index];

                    if(left == null && right == null) continue;
                    if(left == null) return 1;
                    if(right == null) return -1;

                    int result = 0;
                    RelDataType type = getInput().getRowType().getFieldList().get(index).getType();
                    switch (type.getSqlTypeName()) {
                        case BIGINT:
                        case INTEGER:
                            result = compareValues((Integer) o1[index],(Integer) o2[index], Integer.class);
                            break;
                        case CHAR:
                        case VARCHAR:
                            result = compareValues((String) o1[index],(String) o2[index], String.class);
                            break;
                        case BOOLEAN:
                            result = compareValues((Boolean) o1[index],(Boolean) o2[index], Boolean.class);
                            break;
                        case REAL:
                        case FLOAT:
                            result = compareValues((Float) o1[index],(Float) o2[index], Float.class);
                            break;
                        case DOUBLE:
                        case DECIMAL:
                            result = compareValues((Double) o1[index],(Double) o2[index], Double.class);
                            break;
                        default:
                            System.out.println("Invalid type: " + type.getSqlTypeName());
                            break;
                    }
                    if (result != 0) {
                        return direction == RelFieldCollation.Direction.ASCENDING ? result : -result;
                    }
                }
                return 0;
            }
        });
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        ((PRel) getInput()).close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
        return counter < end;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
        return inputList.get(counter++);
    }

}
