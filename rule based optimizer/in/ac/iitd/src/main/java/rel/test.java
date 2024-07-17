package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.type.BasicSqlType;

import java.util.List;


/*
 * PProjectFilter is a relational operator that represents a Project followed by a Filter.
 * You need to write the entire code in this file.
 * To implement PProjectFilter, you can extend either Project or Filter class.
 * Define the constructor accordinly and override the methods as required.
 */
public class PProjectFilter extends Project implements PRel {

    private final RexNode condition ;

    public PProjectFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RexNode cc ,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        this.condition = cc;
//        assert getConvention() instanceof PConvention;
    }
    public PProjectFilter copy(RelTraitSet traitSet, RelNode input,
                               List<RexNode> projects  ,RelDataType rowType) {
        return new PProjectFilter(getCluster(), traitSet, input, projects , condition ,rowType);
    }


    public String toString() {
        return "PProjectFilter";
    }

    Object[] buffer_row =null ;

    public boolean greater_equal_compare(Object left, Object right){
        if (left== null && right== null) {
            return true;
        }
        // if one key is true then for boolean check if it is true
        if (left == null && right instanceof Boolean) {
            return ! (Boolean) right;
        }
        if (left == null && right instanceof Boolean) {
            return (Boolean) left;
        }
        if (left == null ) {
            return true;
        }
        if (right == null) {
            return false;
        }
        if (left instanceof Integer){
            return (Integer) left >= (Integer) right ;
        }
        if (left instanceof Long){
            return (Long) left >= (Long) right ;
        }
        if (left instanceof Float){
            return (Float) left >= (Float) right ;
        }
        if (left instanceof Double){
            return (Double) left >= (Double) right ;
        }
        if (left instanceof Boolean){
            if ((Boolean) left) return true ;
            return (! (Boolean) right) ;
        }
        if (left instanceof  String){
            return ((String) left).compareTo((String) right) >= 0 ;
        }
        throw new IllegalArgumentException("Unsupported type: " + left.getClass().getName());
    }

    public Object literal_to_object(RexLiteral r){
        if (r.getType().getSqlTypeName().getName().equals("INTEGER")){
            return r.getValueAs(Integer.class);
        }
        if (r.getType().getSqlTypeName().getName().equals("BIGINT")){
            return r.getValueAs(Long.class);
        }
        if (r.getType().getSqlTypeName().getName().equals("FLOAT")){
            return r.getValueAs(Float.class);
        }
        if (r.getType().getSqlTypeName().getName().equals("DOUBLE")){
            return r.getValueAs(Double.class);
        }
        if (r.getType().getSqlTypeName().getName().equals("BOOLEAN")){
            return r.getValueAs(Boolean.class);
        }
        if (r.getType().getSqlTypeName().getName().equals("VARCHAR")){
            return r.getValueAs(String.class);
        }
        throw new IllegalArgumentException("Unsupported type: " + r.getType().getSqlTypeName().getName());
    }

    public Object evaluate_rexnode(Object[] row , RexNode r){
        if (r instanceof RexLiteral){
            // if r is null return null

            if (((RexLiteral) r).getValue() == null){
                return null ;
            }
            return literal_to_object((RexLiteral) r);


        }

        if (r instanceof RexInputRef){
            return row[((RexInputRef) r).getIndex()];
        }
        if (r instanceof RexCall){
            RexCall call = (RexCall) r ;
            SqlOperator operator = (SqlOperator) call.getOperator();
            List<RexNode> operands = call.getOperands();
            // if the operator is of type cast
//            if (operator.getKind() == SqlKind.CAST) {
//                // Handle CAST operator
//
//                Object expr = evaluate_rexnode(row, operands.get(0));
//                return convert_data_type(expr,call.getType()) ;
//                // Implement logic to determine target type from RexNode (might require additional methods)
////                Class targetType = determineTypeFromRexNode(targetTypeNode);
////                return castObject(expr, targetType);
//            }

            if (operator.getKind()== SqlKind.NOT){
                return ! (boolean) evaluate_rexnode(row, operands.get(0));
            }
            if (operator.getKind() == SqlKind.IS_NULL){
                return evaluate_rexnode(row, operands.get(0)) == null ;
            }
            if (operator.getKind() == SqlKind.IS_NOT_NULL){
                return evaluate_rexnode(row, operands.get(0)) != null ;
            }
            if (operator.getKind() == SqlKind.AND){
                boolean ans = true ;
                for (int i=0 ; i < operands.size() ; i++){
                    ans = ans & (boolean) evaluate_rexnode(row, operands.get(i));
                }
                return ans ;
            }
            if (operator.getKind() == SqlKind.OR){
                boolean ans = false ;
                for (int i=0 ; i < operands.size() ; i++){
                    ans = ans | (boolean) evaluate_rexnode(row, operands.get(i));
                }
                return ans ;
            }
            if (operator.getKind() == SqlKind.LESS_THAN){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                return !greater_equal_compare(left, right);
            }
            if (operator.getKind() == SqlKind.LESS_THAN_OR_EQUAL){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                return greater_equal_compare(right, left);
            }
            if (operator.getKind() == SqlKind.GREATER_THAN){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                return greater_equal_compare(left, right) & !left.equals(right);
            }
            if (operator.getKind() == SqlKind.GREATER_THAN_OR_EQUAL){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                return greater_equal_compare(left, right);
            }
            if (operator.getKind() == SqlKind.EQUALS){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                return left.equals(right);
            }
            if (operator.getKind() == SqlKind.PLUS){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                if (left instanceof Integer){
                    return (Integer) left + (Integer) right ;
                }
                if (left instanceof Long){
                    return (Long) left + (Long) right ;
                }
                if (left instanceof Float){
                    return (Float) left + (Float) right ;
                }
                if (left instanceof Double){
                    return (Double) left + (Double) right ;
                }
                throw new IllegalArgumentException("Unsupported type: " + left.getClass().getName());
            }
            if (operator.getKind() == SqlKind.MINUS){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                if (left instanceof Integer){
                    return (Integer) left - (Integer) right ;
                }
                if (left instanceof Long){
                    return (Long) left - (Long) right ;
                }
                if (left instanceof Float){
                    return (Float) left - (Float) right ;
                }
                if (left instanceof Double){
                    return (Double) left - (Double) right ;
                }
                throw new IllegalArgumentException("Unsupported type: " + left.getClass().getName());
            }
            if (operator.getKind() == SqlKind.TIMES){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                if (left instanceof Integer){
                    return (Integer) left * (Integer) right ;
                }
                if (left instanceof Long){
                    return (Long) left * (Long) right ;
                }
                if (left instanceof Float){
                    return (Float) left * (Float) right ;
                }
                if (left instanceof Double){
                    return (Double) left * (Double) right ;
                }
                throw new IllegalArgumentException("Unsupported type: " + left.getClass().getName());
            }
            if (operator.getKind() == SqlKind.DIVIDE){
                Object left = evaluate_rexnode(row, operands.get(0));
                Object right = evaluate_rexnode(row, operands.get(1));
                if (left instanceof Integer){
                    return (Integer) left / (Integer) right ;
                }
                if (left instanceof Long){
                    return (Long) left / (Long) right ;
                }
                if (left instanceof Float){
                    return (Float) left / (Float) right ;
                }
                if (left instanceof Double){
                    return (Double) left / (Double) right ;
                }
                throw new IllegalArgumentException("Unsupported type: " + left.getClass().getName());
            }
        }
        throw new IllegalArgumentException("Unsupported type: " + r.getClass().getName());
    }

    boolean matchesCondition(Object [] r, RexNode condition){
        // check if the row matches the condition

        boolean t = (boolean) evaluate_rexnode(r, condition);
        return t ;

    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        PRel p = (PRel) this.input ;
        return p.open() ;

        /* Write your code here */
    }

    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        PRel p = (PRel) this.input ;
        p.close() ;
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProjectFilter has next");
        PRel p = (PRel) this.getInput();
        if (!p.hasNext()) {
            return false;
        }
        Object[] row = p.next();
        if (matchesCondition(row , condition)){

            List<RexNode> projections = getProjects() ;
            Object[] result = new Object[projections.size()];
            for(int i=0; i<projections.size(); i++){
                // here projections could be RexNode : RexInputRef or RexCall
                result[i] = evaluate_rexnode(row, projections.get(i));
            }
            buffer_row = result ;

            return true ;
        }
        else{
            return hasNext() ;
        }
        /* Write your code here */
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        PRel p = (PRel) getInput();
        if (buffer_row != null){
            Object[] row = buffer_row ;
            buffer_row = null ;
            return row ;
        }
        /* Write your code here */
        return null;
    }
}
