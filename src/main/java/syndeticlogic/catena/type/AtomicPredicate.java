package syndeticlogic.catena.type;

public class AtomicPredicate implements Predicate {
    private Operator operator;
    private Operand operand;
    
    public AtomicPredicate(Operator operator, Operand operand) {
        this.operator = operator;
        this.operand = operand;
    }

    public boolean satisfies(byte[] raw, int offset, int length) {
        return operator.satisfies(operand, raw, offset, length);
    }
}
