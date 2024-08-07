package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        return ((PRel) getInput()).open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        ((PRel) getInput()).close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        return ((PRel) getInput()).hasNext();
    }

    Object evaluate(Object[] inputRow, RexNode project)
    {
        if(project instanceof RexLiteral)
        {
            RexLiteral literal = (RexLiteral) project;
            if(literal.isNull()) return null;

            switch (literal.getType().getSqlTypeName())
            {
                case BIGINT:
                case INTEGER:
                    return literal.getValueAs(Integer.class);
                case CHAR:
                case VARCHAR:
                    return literal.getValueAs(String.class);
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class);
                case REAL:
                case FLOAT:
                    return literal.getValueAs(Float.class);
                case DOUBLE:
                case DECIMAL:
                    return literal.getValueAs(Double.class);
            }
        }
        if(project instanceof RexInputRef)
        {
            RexInputRef col = (RexInputRef) project;
            return inputRow[col.getIndex()];
        }

        RexCall predicate = (RexCall) project;
        SqlOperator op = predicate.getOperator();
        if(op.getKind() == SqlKind.IS_NULL)
        {
            Object value = evaluate(inputRow, predicate.getOperands().get(0));
            return value == null;
        }
        if(op.getKind() == SqlKind.IS_NOT_NULL)
        {
            Object value = evaluate(inputRow, predicate.getOperands().get(0));
            return value != null;
        }
        if(op.getKind() == SqlKind.CAST)
        {
            RexNode obj = predicate.getOperands().get(0);
            return evaluate(inputRow, obj);
        }
        if(op.getKind() == SqlKind.NOT)
        {
            Object eval = evaluate(inputRow, predicate.getOperands().get(0));
            if(eval == null) return false;
            boolean lval = (boolean) eval;
            return !lval;
        }
        if(op.getKind() == SqlKind.AND)
        {
            List<RexNode> operands = predicate.getOperands();
            for(int i=0;i<operands.size();i++)
            {
                Object eval = evaluate(inputRow, operands.get(i));
                if(eval == null) return false;
                boolean lval = (boolean) eval;
                if(!lval) return false;
            }
            return true;
        }
        if(op.getKind() == SqlKind.OR)
        {
            List<RexNode> operands = predicate.getOperands();
            for(int i=0;i<operands.size();i++)
            {
                Object eval = evaluate(inputRow, operands.get(i));
                if(eval == null) continue;
                boolean lval = (boolean) eval;
                if(lval) return true;
            }
            return false;
        }

        RexNode left = predicate.getOperands().get(0);
        RexNode right = predicate.getOperands().get(1);

        Object lval = evaluate(inputRow, left);
        Object rval = evaluate(inputRow, right);

        if(lval == null || rval == null)
        {
            switch (op.getKind())
            {
                case TIMES:
                case DIVIDE:
                case PLUS:
                case MINUS:
                    return null;
                case EQUALS:
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return false;
            }
        }

        if(lval instanceof Double || rval instanceof Double)
        {
            Double leftValue = 0.0;
            Double rightValue = 0.0;

            if(lval instanceof Double) leftValue = (Double) lval;
            if(lval instanceof Float) leftValue = Double.valueOf((Float) lval);
            if(lval instanceof Integer) leftValue = Double.valueOf((Integer) lval);

            if(rval instanceof Double) rightValue = (Double) rval;
            if(rval instanceof Float) rightValue = Double.valueOf((Float) rval);
            if(rval instanceof Integer) rightValue = Double.valueOf((Integer) rval);

            switch(op.getKind())
            {
                case TIMES:
                    return leftValue * rightValue;
                case DIVIDE:
                    return leftValue / rightValue;
                case PLUS:
                    return leftValue + rightValue;
                case MINUS:
                    return leftValue - rightValue;
                case EQUALS:
                    return leftValue.compareTo(rightValue) == 0;
                case NOT_EQUALS:
                    return leftValue.compareTo(rightValue) != 0;
                case LESS_THAN:
                    return leftValue.compareTo(rightValue) < 0;
                case LESS_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) <= 0;
                case GREATER_THAN:
                    return leftValue.compareTo(rightValue) > 0;
                case GREATER_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) >= 0;
            }
        }
        if(lval instanceof Float || rval instanceof Float)
        {
            Float leftValue = 0.0f;
            Float rightValue = 0.0f;

            if(lval instanceof Float) leftValue = (Float) lval;
            if(lval instanceof Integer) leftValue = Float.valueOf((Integer) lval);

            if(rval instanceof Float) rightValue = (Float) rval;
            if(rval instanceof Integer) rightValue = Float.valueOf((Integer) rval);

            switch(op.getKind())
            {
                case TIMES:
                    return leftValue * rightValue;
                case DIVIDE:
                    return leftValue / rightValue;
                case PLUS:
                    return leftValue + rightValue;
                case MINUS:
                    return leftValue - rightValue;
                case EQUALS:
                    return leftValue.compareTo(rightValue) == 0;
                case NOT_EQUALS:
                    return leftValue.compareTo(rightValue) != 0;
                case LESS_THAN:
                    return leftValue.compareTo(rightValue) < 0;
                case LESS_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) <= 0;
                case GREATER_THAN:
                    return leftValue.compareTo(rightValue) > 0;
                case GREATER_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) >= 0;
            }
        }
        if(lval instanceof Integer || rval instanceof Integer)
        {
            Integer leftValue = (Integer) lval;
            Integer rightValue = (Integer) rval;

            switch(op.getKind())
            {
                case TIMES:
                    return leftValue * rightValue;
                case DIVIDE:
                    return leftValue / rightValue;
                case PLUS:
                    return leftValue + rightValue;
                case MINUS:
                    return leftValue - rightValue;
                case EQUALS:
                    return leftValue.compareTo(rightValue) == 0;
                case NOT_EQUALS:
                    return leftValue.compareTo(rightValue) != 0;
                case LESS_THAN:
                    return leftValue.compareTo(rightValue) < 0;
                case LESS_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) <= 0;
                case GREATER_THAN:
                    return leftValue.compareTo(rightValue) > 0;
                case GREATER_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) >= 0;
            }
        }
        if(lval instanceof String || rval instanceof String)
        {
            String leftValue = (String) lval;
            String rightValue = (String) rval;

            switch(op.getKind())
            {
                case EQUALS:
                    return leftValue.compareTo(rightValue) == 0;
                case NOT_EQUALS:
                    return leftValue.compareTo(rightValue) != 0;
                case LESS_THAN:
                    return leftValue.compareTo(rightValue) < 0;
                case LESS_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) <= 0;
                case GREATER_THAN:
                    return leftValue.compareTo(rightValue) > 0;
                case GREATER_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) >= 0;
            }
        }
        if(lval instanceof Boolean || rval instanceof Boolean)
        {
            Boolean leftValue = (Boolean) lval;
            Boolean rightValue = (Boolean) rval;

            switch(op.getKind())
            {
                case EQUALS:
                    return leftValue.compareTo(rightValue) == 0;
                case NOT_EQUALS:
                    return leftValue.compareTo(rightValue) != 0;
                case LESS_THAN:
                    return leftValue.compareTo(rightValue) < 0;
                case LESS_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) <= 0;
                case GREATER_THAN:
                    return leftValue.compareTo(rightValue) > 0;
                case GREATER_THAN_OR_EQUAL:
                    return leftValue.compareTo(rightValue) >= 0;
            }
        }
        return null;
    }
    
    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        /* Write your code here */
        Object[] inputRow = ((PRel) getInput()).next();
        Object[] outputRow = new Object[getProjects().size()];
        for(int i=0;i<getProjects().size();i++)
        {
            RexNode project = getProjects().get(i);
            outputRow[i] = evaluate(inputRow,project);
        }
        return outputRow;
    }
}
