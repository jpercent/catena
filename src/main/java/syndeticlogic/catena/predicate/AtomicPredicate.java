package syndeticlogic.catena.predicate;

import syndeticlogic.catena.type.Value;

public class AtomicPredicate implements Predicate {
    private Operator operator;
    private Value value;
    
    public AtomicPredicate(Operator operator, Value value) {
        this.operator = operator;
        this.value = value;
    }

    public boolean satisfies(byte[] raw, int offset, int length) {
        return operator.satisfies(value, raw, offset, length);
    }
}
