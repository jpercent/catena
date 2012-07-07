package syndeticlogic.catena.predicate;

public class CompoundPredicate implements Predicate {
    private BinaryConnector operator;
    private Predicate lhs;
    private Predicate rhs;

    public CompoundPredicate(BinaryConnector operator, Predicate p, Predicate q) {
        this.operator = operator;
        this.lhs = p;
        this.rhs = q;
    }

    @Override
    public boolean satisfies(byte[] raw, int offset, int length) {
        return operator.satisfies(lhs, rhs, raw, offset, length);
    }
}
