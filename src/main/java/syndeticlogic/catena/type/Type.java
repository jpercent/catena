package syndeticlogic.catena.type;

public enum Type {
    
    TYPE {
        @Override
        public int length() {
            return TYPE_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }      
    },
    
    BOOLEAN {
        @Override 
        public int length() {
            return BOOLEAN_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    }, 
    
    BYTE{
        @Override 
        public int length() {
            return BYTE_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    },
    
    CHAR {
        @Override 
        public int length() {
            return CHAR_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    },
    
    SHORT { 
        @Override 
        public int length() {
            return SHORT_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    },
    
    INTEGER {
        @Override 
        public int length() {
            return INTEGER_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    },
    
    LONG {
        @Override 
        public int length() {
            return LONG_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    },
    
    FLOAT {
        @Override 
        public int length() {
            return FLOAT_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    },
    
    DOUBLE {
        @Override 
        public int length() {
            return DOUBLE_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return true;
        }
    },
    
    STRING {
        @Override 
        public int length() {
            return SHORT_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return false;
        }
    },
    
    BINARY {
        @Override 
        public int length() {
            return INTEGER_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return false;
        }
    },
    
    CODEABLE() {
        @Override 
        public int length() {
            return BYTE_BYTES;
        }
        
        @Override
        public boolean isFixedLength() {
            return false;
        }
    };
    
    private static final int TYPE_BYTES = 1;
    private static final int BOOLEAN_BYTES = 1;
    private static final int BYTE_BYTES = 1;
    private static final int CHAR_BYTES = 2; 
    private static final int SHORT_BYTES = 2;
    private static final int INTEGER_BYTES = 4;
    private static final int LONG_BYTES = 8;
    private static final int FLOAT_BYTES = 4;
    private static final int DOUBLE_BYTES = 8;

    public abstract int length();
    public abstract boolean isFixedLength();
}
