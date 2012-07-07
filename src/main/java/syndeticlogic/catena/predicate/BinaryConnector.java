package syndeticlogic.catena.predicate;

public enum BinaryConnector {
    Conjunction {
        @Override
        public boolean satisfies(Predicate p, Predicate q, byte[] raw,
                                 int offset, int length) {
            boolean ret = false;
            if (p.satisfies(raw, offset, length)
                    && q.satisfies(raw, offset, length)) {
                ret = true;
            }
            return ret;
        }
    },
    Disjunction {
        @Override
        public boolean satisfies(Predicate p, Predicate q, byte[] raw,
                                 int offset, int length) {
            boolean ret = false;
            if (p.satisfies(raw, offset, length)
                    || q.satisfies(raw, offset, length)) {
                ret = true;
            }
            return ret;
        }
    };
    public abstract boolean satisfies(Predicate p, Predicate q, byte[] raw,
                               int offset, int length);
}
