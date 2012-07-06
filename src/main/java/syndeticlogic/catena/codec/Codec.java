/*
 *   Author == James Percent (james@empty-set.net)
 *   Copyright 2010, 2011 James Percent
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package syndeticlogic.catena.codec;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.type.TypeFactory;


public class Codec {

    private static final byte TYPE_TYPE = 0;
    private static final byte BOOLEAN_TYPE = 1;
    private static final byte BYTE_TYPE = 2;
    private static final byte CHAR_TYPE = 3;
    private static final byte SHORT_TYPE = 4;
    private static final byte INTEGER_TYPE = 5;
    private static final byte LONG_TYPE = 6;
    private static final byte FLOAT_TYPE = 7;
    private static final byte DOUBLE_TYPE = 8;
    private static final byte STRING_TYPE = 9;
    private static final byte BINARY_TYPE = 10;
    private static final byte CODEABLE_TYPE = 11;
    
    private static final byte BITS_PER_TYPE = 4;
    private static final byte TYPES_PER_BYTE = 2;
    
    private static Codec singleton;
    private TypeFactory factory;

    public synchronized static void configureCodec(TypeFactory factory) {
        singleton = new Codec(factory);
    }

    public static synchronized Codec getCodec() {
        if (singleton == null) {
            throw new RuntimeException("Codec has not been injected");
        }
        return singleton;
    }

    public Codec(TypeFactory factory) {
        this.factory = factory;
    }

    public synchronized CodeHelper coder() {
        return new CodeHelper(this);
    }

    public int getBitsPerType() {
        return BITS_PER_TYPE;
    }

    public int encodeTypes(byte[] dest, int offset, List<Type> types) {
        int bytes = 0;
        int shift = 0;
        int size = (types.size() % TYPES_PER_BYTE == 0 ? types.size()
                / TYPES_PER_BYTE : types.size() / TYPES_PER_BYTE + 1);

        for (int i = offset; i < size; i++)
            dest[i] = 0;

        boolean set = false;
        byte current = dest[offset];

        for (Type t : types) {
            set = false;
            current = encodeAndPackType(t, current, shift);
            shift += 4;
            if (shift == 8) {
                shift = 0;
                bytes++;
                set = true;
                dest[offset++] = current;
                current = dest[offset];
            }
        }

        if (!set) {
            bytes++;
            dest[offset] = current;
        }
        return bytes;
    }

    public int encodeTypes(ByteBuffer dest, List<Type> types) {
        int bytes = 0;
        int shift = 0;

        int mark = dest.position();
        dest.mark();

        int size = 0;
        if (types.size() % TYPES_PER_BYTE == 0)
            size = types.size() / TYPES_PER_BYTE;
        else
            size = types.size() / TYPES_PER_BYTE + 1;

        for (int i = dest.limit(); i < mark + size; i++)
            dest.put((byte) 0);

        dest.reset();

        byte current = dest.get(dest.position());
        boolean set = false;
        for (Type t : types) {
            set = false;
            current = encodeAndPackType(t, current, shift);
            shift += 4;
            if (shift == 8) {
                shift = 0;
                dest.put(current);
                current = dest.get(dest.position());
                bytes++;
                set = true;
            }
        }
        if (!set)
            dest.put(current);

        return bytes;
    }

    public List<Type> decodeTypes(byte[] source, int offset, int numTypes) {
        assert (source.length - offset) >= numTypes;
        int shift = 0;
        List<Type> types = new ArrayList<Type>(numTypes);

        for (int i = 0; i < numTypes; i++) {
            byte t = (byte) ((source[offset] >>> shift) & 0xf);
            types.add(decodeType(t));
            shift += 4;
            if (shift == 8) {
                shift = 0;
                offset++;
            }
        }
        if (shift == 4)
            offset++;

        return types;
    }

    public List<Type> decodeTypes(ByteBuffer source, int numTypes) {
        int shift = 0;
        List<Type> types = new ArrayList<Type>(numTypes);
        source.mark();
        for (int i = 0; i < numTypes; i++) {
            byte t = (byte) ((source.get(source.position()) >>> shift) & 0xf);
            types.add(decodeType(t));
            shift += 4;
            if (shift == 8) {
                shift = 0;
                source.get();
            }
        }

        if (shift == 4)
            source.get();

        return types;
    }

    private byte encodeAndPackType(Type t, byte current, int shift) {
        switch (t) {
        case TYPE:
            current |= TYPE_TYPE << shift;
            break;
        case BOOLEAN:
            current |= BOOLEAN_TYPE << shift;
            break;
        case BYTE:
            current |= BYTE_TYPE << shift;
            break;
        case CHAR:
            current |= CHAR_TYPE << shift;
            break;
        case SHORT:
            current |= SHORT_TYPE << shift;
            break;
        case INTEGER:
            current |= INTEGER_TYPE << shift;
            break;
        case LONG:
            current |= LONG_TYPE << shift;
            break;
        case FLOAT:
            current |= FLOAT_TYPE << shift;
            break;
        case DOUBLE:
            current |= DOUBLE_TYPE << shift;
            break;
        case STRING:
            current |= STRING_TYPE << shift;
            break;
        case BINARY:
            current |= BINARY_TYPE << shift;
            break;
        case CODEABLE:
            current |= CODEABLE_TYPE << shift;
            break;
        default:
            throw new RuntimeException("Undefined type");
        }
        return current;
    }

    private byte encodeType(Type t) {
        byte ret = 0;
        switch (t) {
        case TYPE:
            ret = TYPE_TYPE;
            break;
        case BOOLEAN:
            ret = BOOLEAN_TYPE;
            break;
        case BYTE:
            ret = BYTE_TYPE;
            break;
        case CHAR:
            ret = CHAR_TYPE;
            break;
        case SHORT:
            ret = SHORT_TYPE;
            break;
        case INTEGER:
            ret = INTEGER_TYPE;
            break;
        case LONG:
            ret = LONG_TYPE;
            break;
        case FLOAT:
            ret = FLOAT_TYPE;
            break;
        case DOUBLE:
            ret = DOUBLE_TYPE;
            break;
        case STRING:
            ret = STRING_TYPE;
            break;
        case BINARY:
            ret = BINARY_TYPE;
            break;
        case CODEABLE:
            ret = CODEABLE_TYPE;
            break;
        default:
            throw new RuntimeException("Undefined type");
        }
        return ret;
    }

    private Type decodeType(byte t) {

        Type ret = Type.BINARY;
        switch (t) {
        case TYPE_TYPE:
            ret = Type.TYPE;
            // assert false;
            break;
        case BOOLEAN_TYPE:
            ret = Type.BOOLEAN;
            break;
        case BYTE_TYPE:
            ret = Type.BYTE;
            break;
        case CHAR_TYPE:
            ret = Type.CHAR;
            break;
        case SHORT_TYPE:
            ret = Type.SHORT;
            break;
        case INTEGER_TYPE:
            ret = Type.INTEGER;
            break;
        case LONG_TYPE:
            ret = Type.LONG;
            break;
        case FLOAT_TYPE:
            ret = Type.FLOAT;
            break;
        case DOUBLE_TYPE:
            ret = Type.DOUBLE;
            break;
        case STRING_TYPE:
            ret = Type.STRING;
            break;
        case BINARY_TYPE:
            ret = Type.BINARY;
            break;
        case CODEABLE_TYPE:
            ret = Type.CODEABLE;
            break;
        default:
            throw new RuntimeException("Undefined type: " + t);
        }
        return ret;
    }

    public int encode(Type t, byte[] dest, int offset) {
        dest[offset] = encodeType(t);
        return Type.TYPE.length();
    }

    public int encode(boolean b, byte[] dest, int offset) {
        assert (dest.length - offset) >= Type.BOOLEAN.length();
        dest[offset] = (b ? (byte) 1 : (byte) 0);
        return Type.BOOLEAN.length();
    }

    public int encode(byte b, byte[] dest, int offset) {
        assert (dest.length - offset) >= Type.BYTE.length();
        dest[offset] = b;
        return Type.BYTE.length();
    }

    public int encode(char c, byte[] dest, int offset) {
        String temp = new String(new char[] { c });
        byte[] temp1 = null;
        try {
            // String characterEncoding = getProperties("character.encoding");
            temp1 = temp.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        assert (dest.length - offset) >= temp1.length;

        for (int i = 0; i < temp1.length; i++)
            dest[offset + i] = temp1[i];
        return Type.CHAR.length();
    }

    public int encode(short b, byte[] dest, int offset) {
        assert dest.length > offset
                && dest.length - offset >= Type.SHORT.length();
        for (int i = 0, j = 8; i < 2; i++, j -= 8) {
            dest[offset + i] = (byte) (b >>> j);
        }
        return Type.SHORT.length();
    }

    public int encode(int i, byte[] dest, int offset) {
        assert dest.length > offset
                && dest.length - offset >= Type.INTEGER.length();
        for (int k = 0, j = 24; k < 4; k++, j -= 8) {
            dest[offset + k] = (byte) (i >>> j);
        }
        return Type.INTEGER.length();
    }

    public int encode(long l, byte[] dest, int offset) {
        assert dest.length > offset
                && dest.length - offset >= Type.LONG.length();
        for (int i = 0, j = 56; i < 8; i++, j -= 8) {
            dest[offset + i] = (byte) (l >>> j);
        }
        return Type.LONG.length();
    }

    public int encode(float f, byte[] dest, int offset) {
        int i = Float.floatToRawIntBits(f);
        return encode(i, dest, offset);
    }

    public int encode(double d, byte[] dest, int offset) {
        long l = Double.doubleToRawLongBits(d);
        return encode(l, dest, offset);
    }

    public int encode(String s, byte[] dest, int offset) {
        byte[] bytes = s.getBytes();
        int size = bytes.length;
        assert size <= 65536
                && (dest.length - (offset + Type.SHORT.length())) >= size;
        offset += encode((short) size, dest, offset);
        for (int i = 0; i < (short) size; i++) {
            dest[offset + i] = bytes[i];
        }
        return Type.SHORT.length() + bytes.length;
    }

    public int encode(byte[] b, byte[] dest, int offset) {
        // Note that encode(byte[] b gives you Type.INTEGER.length() length, whereas
        // string
        // and Codeable have only Type.SHORT.length() lengths.
        assert (dest.length - offset) >= b.length;
        offset += encode((int) b.length, dest, offset);
        //System.arraycopy(b, 0, dest, offset, b.length);

        for (int i = 0; i < b.length; i++)
            dest[offset + i] = b[i];

        return Type.INTEGER.length() + b.length;
    }

    public int encode(Codeable c, byte[] dest, int offset) {
        int size = c.computeSize();
        assert size <= 65536;
        byte type = c.getTypeId();
        encode(type, dest, offset);
        offset += Type.BYTE.length();
        c.encode(dest, offset);
        return Type.BYTE.length() + size;
    }

    public int encode(Type t, ByteBuffer dest) {
        dest.put(encodeType(t));
        return Type.TYPE.length();
    }

    public int encode(boolean b, ByteBuffer dest) {
        assert dest.remaining() >= Type.BOOLEAN.length();
        dest.put((b ? (byte) 1 : (byte) 0));
        return Type.BOOLEAN.length();
    }

    public int encode(byte b, ByteBuffer dest) {
        assert dest.remaining() >= Type.BYTE.length();
        dest.put(b);
        return Type.BYTE.length();
    }

    public int encode(char c, ByteBuffer dest) {
        String temp = new String(new char[] { c });
        byte[] temp1 = null;
        try {
            // String characterEncoding = getProperties("character.encoding");
            temp1 = temp.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        assert dest.remaining() >= temp1.length;

        for (int i = 0; i < temp1.length; i++)
            dest.put(temp1[i]);
        return Type.CHAR.length();
    }

    public int encode(short s, ByteBuffer dest) {
        assert dest.remaining() >= Type.SHORT.length();
        for (int i = 0, j = 8; i < 2; i++, j -= 8) {
            dest.put((byte) (s >>> j));
        }
        return Type.SHORT.length();
    }

    public int encode(int i, ByteBuffer dest) {
        assert dest.remaining() >= Type.INTEGER.length();
        for (int k = 0, j = 24; k < 4; k++, j -= 8) {
            dest.put((byte) (i >>> j));
        }
        return Type.INTEGER.length();
    }

    public int encode(long l, ByteBuffer dest) {
        assert dest.remaining() >= Type.LONG.length();
        for (int i = 0, j = 56; i < 8; i++, j -= 8) {
            dest.put((byte) (l >>> j));
        }
        return Type.LONG.length();
    }

    public int encode(float f, ByteBuffer dest) {
        int i = Float.floatToRawIntBits(f);
        return encode(i, dest);
    }

    public int encode(double d, ByteBuffer dest) {
        long l = Double.doubleToRawLongBits(d);
        return encode(l, dest);
    }

    public int encode(String s, ByteBuffer dest) {
        byte[] bytes = s.getBytes();
        int size = bytes.length;

        assert size <= 65536;
        assert dest.remaining() >= size + Type.SHORT.length();

        encode((short) size, dest);
        for (int i = 0; i < (short) size; i++)
            dest.put(bytes[i]);

        return Type.SHORT.length() + bytes.length;
    }

    public int encode(byte[] source, ByteBuffer dest) {
        // Note that encode(byte[] b gives you Type.INTEGER.length() length, whereas
        // string
        // and Codeable have only Type.SHORT.length() lengths.
        assert source.length <= dest.remaining();
        encode((int) (source.length), dest);
        dest.put(source);
        return Type.INTEGER.length() + source.length;
    }

    public int encode(Codeable c, ByteBuffer dest) {
        int size = c.computeSize();
        assert size <= 65536;
        // byte type = c.getTypeId();
        throw new RuntimeException("Not implemented");
        // encode(type, dest);
        // offset += Type.BYTE.length();
        // c.encode(dest, dest);
        // return Type.BYTE.length()+size;
    }

    /*
     * public int getCodeSize(Type t) { int ret = 0; switch(t) { case TYPE: ret
     * = Type.BYTE.length(); break; case BOOLEAN: ret = Type.BOOLEAN.length(); break; case BYTE:
     * ret = Type.BYTE.length(); break; case CHAR: ret = Type.CHAR.length(); break; case SHORT:
     * ret = Type.SHORT.length(); break; case INTEGER: ret = Type.INTEGER.length(); break; case
     * LONG: ret = Type.LONG.length(); break; case FLOAT: ret = Type.FLOAT.length(); break; case
     * DOUBLE: ret = Type.DOUBLE.length(); break; case STRING: ret = Type.SHORT.length(); break;
     * case BINARY: ret = Type.INTEGER.length(); break; case CODEABLE: ret = Type.BYTE.length();
     * break; default: throw new RuntimeException("Undefined type"); } return
     * ret; }
     */

    /*
     * public boolean isTypeFixedLength(Type t) { switch(t) { case TYPE: case
     * BOOLEAN: case BYTE: case CHAR: case SHORT: case INTEGER: case LONG: case
     * FLOAT: case DOUBLE: return true; case STRING: case BINARY: case CODEABLE:
     * return false; default: throw new RuntimeException("Undefined type"); } }
     */
    // XXX - perhaps this should be deprecated - isTypeFixedLength can be used
    // in place
    /*
     * public int getTypeSize(Type t) {
     * 
     * int ret = 0; switch(t) { case TYPE: ret = Type.BYTE.length(); break; case BOOLEAN:
     * ret = Type.BOOLEAN.length(); break; case BYTE: ret = Type.BYTE.length(); break; case CHAR:
     * ret = Type.CHAR.length(); break; case SHORT: ret = Type.SHORT.length(); break; case
     * INTEGER: ret = Type.INTEGER.length(); break; case LONG: ret = Type.LONG.length(); break;
     * case FLOAT: ret = Type.FLOAT.length(); break; case DOUBLE: ret = Type.DOUBLE.length();
     * break; case STRING: ret = -1; break; case BINARY: ret = -1; break; case
     * CODEABLE: ret = -1; break; default: throw new
     * RuntimeException("Undefined type"); } return ret; }
     */

    public Type decodeCodecType(byte[] source, int offset) {
        assert source.length - offset >= 1;
        return decodeType(source[offset]);
    }

    public boolean decodeBoolean(byte[] source, int offset) {
        assert source.length - offset >= 1;
        if (source[offset] == 0) {
            return false;
        } else {
            assert source[offset] == 1;
            return true;
        }
    }

    public byte decodeByte(byte[] source, int offset) {
        assert source.length - offset >= 1;
        byte b = source[offset];
        return b;
    }

    public char decodeChar(byte[] source, int offset) {
        assert source.length - offset >= Type.CHAR.length();
        String temp;
        try {
            temp = new String(source, offset, 1, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        assert temp.toCharArray().length == 1;
        char c = temp.toCharArray()[0];
        return c;
    }

    public short decodeShort(byte[] source, int offset) {
        assert source.length > offset && source.length - offset >= Type.SHORT.length();
        short ret = 0;
        for (int i = 0, j = 8; i < 2; i++, j -= 8) {
            ret |= ((source[offset + i] & 0xff) << j);
        }
        return ret;
    }

    public int decodeInteger(byte[] source, int offset) {
        assert source.length > offset && source.length - offset >= Type.INTEGER.length();
        int ret = 0;
        for (int k = 0, j = 24; k < 4; k++, j -= 8) {
            ret |= ((source[offset + k] & 0xff) << j);
        }
        return ret;
    }

    public long decodeLong(byte[] source, int offset) {
        assert source.length - offset >= Type.LONG.length();
        assert source.length > offset && source.length - offset >= 8;
        long ret = 0;
        for (int i = 0, j = 56; i < 8; i++, j -= 8) {
            ret |= (long) (source[offset + i] & 0xff) << j;
        }
        return ret;
    }

    public float decodeFloat(byte[] source, int offset) {
        assert source.length - offset >= Type.FLOAT.length();
        int i = decodeInteger(source, offset);
        float f = Float.intBitsToFloat(i);
        return f;
    }

    public double decodeDouble(byte[] source, int offset) {
        assert source.length - offset >= Type.DOUBLE.length();
        long l = decodeLong(source, offset);
        double d = Double.longBitsToDouble(l);
        return d;
    }

    public String decodeString(byte[] source, int offset) {
        short size = decodeShort(source, offset);
        offset += Type.SHORT.length();
        String s = new String(source, offset, size);// , "ISO-8859-1");
        return s;
    }

    public byte[] decodeBinary(byte[] source, int offset) {
        int size = decodeInteger(source, offset);
        offset += Type.INTEGER.length();
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = source[offset + i];
        }
        return b;
    }

    public Codeable decodeCodeable(byte[] source, int offset) {
        byte type = decodeByte(source, offset);
        offset += Type.BYTE.length();
        Codeable c = (Codeable) factory.create(type);
        c.decode(source, offset);
        return c;
    }

    public Type decodeCodecType(ByteBuffer source) {
        return decodeType(source.get());
    }

    public boolean decodeBoolean(ByteBuffer source) {
        assert source.remaining() >= 1;
        byte data = source.get();
        if (data == 0) {
            return false;
        } else {
            assert data == 1;
            return true;
        }
    }

    public byte decodeByte(ByteBuffer source) {
        assert source.remaining() >= 1;
        byte b = source.get();
        return b;
    }

    public char decodeChar(ByteBuffer source) {
        assert source.remaining() >= Type.CHAR.length();
        String temp;
        byte[] data = new byte[Type.CHAR.length()];
        source.get(data);
        try {
            temp = new String(data, 0, 1, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        assert temp.toCharArray().length == 1;
        char c = temp.toCharArray()[0];
        return c;
    }

    public short decodeShort(ByteBuffer source) {
        assert source.remaining() >= Type.SHORT.length();
        short ret = 0;
        for (int i = 0, j = 8; i < 2; i++, j -= 8) {
            ret |= ((source.get() & 0xff) << j);
        }
        return ret;
    }

    public int decodeInteger(ByteBuffer source) {
        assert source.remaining() >= Type.INTEGER.length();
        int ret = 0;
        for (int k = 0, j = 24; k < 4; k++, j -= 8) {
            ret |= ((source.get() & 0xff) << j);
        }
        return ret;
    }

    public long decodeLong(ByteBuffer source) {
        assert source.remaining() >= Type.LONG.length();
        long ret = 0;
        for (int i = 0, j = 56; i < 8; i++, j -= 8) {
            ret |= (long) (source.get() & 0xff) << j;
        }
        return ret;
    }

    public float decodeFloat(ByteBuffer source) {
        assert source.remaining() >= Type.FLOAT.length();
        int i = decodeInteger(source);
        float f = Float.intBitsToFloat(i);
        return f;
    }

    public double decodeDouble(ByteBuffer source) {
        assert source.remaining() >= Type.DOUBLE.length();
        long l = decodeLong(source);
        double d = Double.longBitsToDouble(l);
        return d;
    }

    public String decodeString(ByteBuffer source) {
        short size = decodeShort(source);
        byte[] data = new byte[size];
        source.get(data);
        String s = new String(data, 0, size);
        return s;
    }

    public byte[] decodeBinary(ByteBuffer source) {
        int size = decodeInteger(source);
        byte[] b = new byte[size];
        source.get(b);
        return b;
    }

    public Codeable decodeCodeable(ByteBuffer source) {
        // byte type = decodeByte(source);
        throw new RuntimeException("Not implemented");
        // Codeable c = (Codeable)codecManager.create(type);
        // c.decode(source);
        // return c; // TODO Auto-generated method stub
    }
}