package syndeticlogic.catena.type;


public enum Operator {
    LESS_THAN {
        @Override
        public boolean satisfies(Value op, byte[] rawdata, int offset, int length) {
            boolean ret = false;
            if (op.compareTo(rawdata, offset, length) == -1) {
                ret = true;
            }
            return ret;
        }
    },
    
    LESS_THAN_EQUAL {
        @Override
        public boolean satisfies(Value op, byte[] rawdata, int offset, int length) {
            boolean ret = false;
            if (op.compareTo(rawdata, offset, length) == 0) {
                ret = true;
            } else if (op.compareTo(rawdata, offset, length) == -1) {
                ret = true;
            }
            return ret;
        }
    },
    
    EQUALS {
        @Override
        public boolean satisfies(Value op, byte[] rawdata, int offset, int length) {
            boolean ret = false;
            if (op.compareTo(rawdata, offset, length) == 0) {
                ret = true;
            }
            return ret;
        }
    },
    
    NOT_EQUALS {
        @Override
        public boolean satisfies(Value op, byte[] rawdata, int offset, int length) {
            boolean ret = false;
            if (op.compareTo(rawdata, offset, length) != 0) {
                ret = true;
            }
            return ret;
        }
    },
    
    GREATER_THAN_EQUAL {
        @Override
        public boolean satisfies(Value op, byte[] rawdata, int offset, int length) {
            boolean ret = false;
            if (op.compareTo(rawdata, offset, length) == 0) {
                ret = true;
            } else if (op.compareTo(rawdata, offset, length) == 1) {
                ret = true;
            }
            return ret;
        }
    },
    
    GREATER_THAN {
        @Override
        public boolean satisfies(Value op, byte[] rawdata, int offset, int length) {
            boolean ret = false;
            if (op.compareTo(rawdata, offset, length) == 1) {
                ret = true;
            }
            return ret;
        }
    };

    abstract boolean satisfies(Value op, byte[] rawData, int offset, int length);
}
