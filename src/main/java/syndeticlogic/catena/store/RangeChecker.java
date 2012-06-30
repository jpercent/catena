package syndeticlogic.catena.store;

import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;

public class RangeChecker {
    private Object smallest;
    private Object largest;
    private Type type;

    public RangeChecker(Type t, Object s, Object l) {
        type = t;
        smallest = s;
        largest = l;
    }

    public Object getSmallest() {
        return smallest;
    }

    public Object getLargest() {
        return largest;
    }

    public void adjustRange(byte[] buffer, int bufferOffset, int length) {

        switch (type) {
        case TYPE:
            break;
        case BOOLEAN:
            break;
        case BYTE:
            break;
        case CHAR: {
            Character newChar = Codec.getCodec().decodeChar(buffer,
                    bufferOffset);
            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newChar;
                return;
            }

            assert newChar != null;
            if (newChar < (Character) smallest)
                smallest = newChar;

            if (newChar > ((Character) largest))
                largest = newChar;

            break;
        }
        case SHORT: {
            Short newShort = Codec.getCodec().decodeShort(buffer,
                    bufferOffset);

            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newShort;
                return;
            }

            assert newShort != null && smallest != null && largest != null;
            if (newShort < (Character) smallest)
                smallest = newShort;

            if (newShort > ((Character) largest))
                largest = newShort;

            break;
        }
        case INTEGER: {
            Integer newInteger = Codec.getCodec().decodeInteger(buffer,
                    bufferOffset);

            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newInteger;
                return;
            }

            assert newInteger != null && smallest != null && largest != null;
            if (newInteger < (Integer) smallest)
                smallest = newInteger;

            if (newInteger > ((Integer) largest))
                largest = newInteger;

            break;
        }
        case LONG: {
            Long newLong = Codec.getCodec().decodeLong(buffer,
                    bufferOffset);

            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newLong;
                return;
            }

            assert newLong != null && smallest != null && largest != null;
            if (newLong < (Long) smallest)
                smallest = newLong;

            if (newLong > ((Character) largest))
                largest = newLong;

            break;
        }
        case FLOAT: {
            Float newFloat = Codec.getCodec().decodeFloat(buffer,
                    bufferOffset);

            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newFloat;
                return;
            }

            assert newFloat != null && smallest != null && largest != null;
            if (newFloat < (Float) smallest)
                smallest = newFloat;

            if (newFloat > ((Float) largest))
                largest = newFloat;

            break;
        }

        case DOUBLE: {
            Double newDouble = Codec.getCodec().decodeDouble(buffer,
                    bufferOffset);

            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newDouble;
                return;
            }

            assert newDouble != null && smallest != null && largest != null;
            if (newDouble < (Double) smallest)
                smallest = newDouble;

            if (newDouble > ((Double) largest))
                largest = newDouble;

            break;
        }

        case STRING: {
            String newString = Codec.getCodec().decodeString(buffer,
                    bufferOffset);

            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newString;
                return;
            }

            assert newString != null && smallest != null && largest != null;
            int result = ((String) smallest).compareTo(newString);
            if (result < 0)
                smallest = newString;

            result = ((String) largest).compareTo(newString);
            if (result > 0)
                largest = newString;

            break;
        }

        case BINARY: {
            if (largest == null) {
                assert smallest == null;
                byte[] newBuff = new byte[length];
                System.arraycopy(buffer, bufferOffset, newBuff, 0, length);
                smallest = largest = (newBuff);
                return;
            }
            int size = buffer.length - bufferOffset;
            boolean bufferSmaller = false;
            boolean reset = false;

            if ((buffer.length - bufferOffset) < ((byte[]) smallest).length) {
                size = ((byte[]) smallest).length;
                bufferSmaller = true;
            }

            for (int i = 0, j = bufferOffset; i < size; i++, j++) {
                if (buffer[j] < ((byte[]) smallest)[i]) {
                    smallest = buffer;
                    reset = true;
                    break;
                }
            }

            // lexigraphic collation..
            if (!reset && bufferSmaller)
                smallest = buffer;

            size = buffer.length - bufferOffset;
            reset = false;
            bufferSmaller = false;

            if ((buffer.length - bufferOffset) < ((byte[]) largest).length) {
                size = ((byte[]) largest).length;
                bufferSmaller = true;
            }

            for (int i = 0, j = bufferOffset; i < size; i++, j++) {
                if (buffer[j] > ((byte[]) largest)[i]) {
                    largest = buffer;
                    reset = true;
                    break;
                }
            }

            if (!reset && !bufferSmaller)
                largest = buffer;

            break;
        }

        case CODEABLE: {
            Codeable newCodeable = Codec.getCodec().decodeCodeable(
                    buffer, bufferOffset);
            assert newCodeable != null && smallest != null && largest != null;
            assert newCodeable.getTypeId() == ((Codeable) smallest).getTypeId();
            assert newCodeable.getTypeId() == ((Codeable) largest).getTypeId();

            if (largest == null) {
                assert smallest == null;
                smallest = largest = (Object) newCodeable;
                return;
            }

            if (((Codeable) smallest).compareTo(newCodeable) < 0)
                smallest = newCodeable;

            if (((Codeable) largest).compareTo(newCodeable) > 0)
                largest = newCodeable;

            break;
        }
        default:
            throw new RuntimeException("Unknown type");
        }
    }
}
