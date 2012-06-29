package syndeticlogic.catena.type;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CodeHelper {
	private static final Log log = LogFactory.getLog(CodeHelper.class);
	private List<Object> components;
	private List<Type> types;
	private Codec coder;
	
	public CodeHelper(Codec coder) {
		components = new LinkedList<Object>();
		types = new LinkedList<Type>();
		this.coder = coder;
	}
	
	public void reset() {
		components = new LinkedList<Object>();
		types = new LinkedList<Type>();
	}
	
	public void append(Type t) {
		components.add(t);
		types.add(Type.TYPE);
	}
	
	public void append(boolean b) {
		components.add(b);
		types.add(Type.BOOLEAN);
	}
	
	public void append(byte b) {
		components.add(new Byte(b));
		types.add(Type.BYTE);
	}

	public void append(char c) {
		components.add(c);
		types.add(Type.CHAR);
	}
	
	public void append(short s) {
		components.add(new Short(s));
		types.add(Type.SHORT);
	}
	
	public void append(int i) {
		components.add(i);
		types.add(Type.INTEGER);
	}
	
	public void append(long l) {
		components.add(l);
		types.add(Type.LONG);
	}
	
	public void append(float f) {
		components.add(new Float(f));
		types.add(Type.FLOAT);
	}
	
	public void append(double d) {
		components.add(new Double(d));
		types.add(Type.DOUBLE);
	}
	
	public void append(String s) {
		components.add(s);
		types.add(Type.STRING);
	}
	
	public void append(byte[] b) {
		components.add(b);
		types.add(Type.BINARY);
	}
	
	public void append(Codeable c) {
		components.add(c);
		types.add(Type.CODEABLE);
	}
	
	public int encode(byte[] buffer, int offset) {
		offset += coder.encodeTypes(buffer, offset, types);
		for(Object obj : components) {
			if(log.isTraceEnabled()) log.trace(obj+" offset "+ offset);

			if(obj instanceof Type) 
				offset += coder.encode((Type)obj, buffer, offset);
			else if(obj instanceof Boolean) 
				offset += coder.encode(((Boolean)obj).booleanValue(), buffer, offset);
			else if(obj instanceof Character) 
				offset += coder.encode(((Character)obj), buffer, offset);
			else if(obj instanceof Byte) 
				offset += coder.encode(((Byte)obj).byteValue(), buffer, offset);
			else if(obj instanceof Short) 
				offset += coder.encode(((Short)obj).shortValue(), buffer, offset);
			else if(obj instanceof Integer) 
				offset += coder.encode(((Integer)obj).intValue(), buffer, offset);
			else if(obj instanceof Long) 
				offset += coder.encode(((Long)obj).longValue(), buffer, offset);
			else if(obj instanceof Float) 
				offset += coder.encode(((Float)obj).floatValue(), buffer, offset);
			else if(obj instanceof Double) 
				offset += coder.encode(((Double)obj).doubleValue(), buffer, offset);
			else if(obj instanceof String)
				offset += coder.encode((String)obj, buffer, offset);
			else if(obj instanceof byte[]) 
				offset += coder.encode((byte[])obj, buffer, offset);
			else if(obj instanceof Codeable) 
				offset += coder.encode((Codeable)obj, buffer, offset);
			else
				assert false;
		}
		return offset;
	}
	
	public int encode(ByteBuffer buffer) {
		coder.encodeTypes(buffer, types);
		for(Object obj : components) {
			if(log.isTraceEnabled()) log.trace(obj+" offset "+ buffer.position());
			
			if(obj instanceof Type) 
				coder.encode((Type)obj, buffer);
			else if(obj instanceof Boolean) 
				coder.encode(((Boolean)obj).booleanValue(), buffer);
			else if(obj instanceof Character) 
				coder.encode(((Character)obj), buffer);
			else if(obj instanceof Byte) 
				coder.encode(((Byte)obj).byteValue(), buffer);
			else if(obj instanceof Short) 
				coder.encode(((Short)obj).shortValue(), buffer);
			else if(obj instanceof Integer) 
				coder.encode(((Integer)obj).intValue(), buffer);
			else if(obj instanceof Long) 
				coder.encode(((Long)obj).longValue(), buffer);
			else if(obj instanceof Float) 
				coder.encode(((Float)obj).floatValue(), buffer);
			else if(obj instanceof Double) 
				coder.encode(((Double)obj).doubleValue(), buffer);
			else if(obj instanceof String)
				coder.encode((String)obj, buffer);
			else if(obj instanceof byte[]) 
				coder.encode((byte[])obj, buffer);
			else if(obj instanceof Codeable) 
				coder.encode((Codeable)obj, buffer);
			else
				assert false;
		}
		return -1;
	}
	
	public byte[] encodeByteArray() {
		int size = computeSize();
		byte[] buffer = new byte[size];
		encode(buffer, 0);
		return buffer;
	}
	
	public ByteBuffer encodeByteBuffer() {
		int size = computeSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		encode(buffer);		
		return buffer;
	}
	
	public List<Object> decode(byte[] rawData, int offset, int elements) {
		types = coder.decodeTypes(rawData, offset, elements);
		int typescodesize = coder.getBitsPerType()*elements;
		
		if(typescodesize % 8 == 0)
			typescodesize /= 8;
		else 
			typescodesize = typescodesize/8+1;
		
		offset += typescodesize;
		if(log.isTraceEnabled()) log.trace(" offset "+ offset);

		for(Type t : types) {
			switch(t) {
			case TYPE:
				components.add(coder.decodeCodecType(rawData, offset));
				offset += Type.TYPE.length();
				break;
			case BOOLEAN:
				components.add(coder.decodeBoolean(rawData, offset));
				offset += Type.BOOLEAN.length();
				break;				
			case BYTE:
				components.add(coder.decodeByte(rawData, offset));
				offset += Type.BYTE.length();
				break;
			case CHAR:
				components.add(coder.decodeChar(rawData, offset));
				offset += Type.CHAR.length();
				break;
			case SHORT:
				components.add(coder.decodeShort(rawData, offset));
				offset += Type.SHORT.length();
				break;
			case INTEGER:
				components.add(coder.decodeInteger(rawData, offset));
				offset += Type.INTEGER.length();
				break;
			case LONG:
				components.add(coder.decodeLong(rawData, offset));
				offset += Type.LONG.length();
				break;
			case FLOAT:
				components.add(coder.decodeFloat(rawData, offset));
				offset += Type.FLOAT.length();
				break;
			case DOUBLE:
				components.add(coder.decodeDouble(rawData, offset));
				offset += Type.DOUBLE.length();
				break;
			case STRING:
				String str = coder.decodeString(rawData, offset);
				components.add(str);
				offset += Type.STRING.length()+str.getBytes().length;
				break;
			case BINARY:
				byte[] bytes = coder.decodeBinary(rawData, offset);
				components.add(bytes);
				offset += Type.BINARY.length()+bytes.length;
				break;
			case CODEABLE:
				Codeable c = coder.decodeCodeable(rawData, offset);
				components.add(c);
				offset += Type.CODEABLE.length()+c.computeSize();
				break;
			default:	
				assert false;
			}
		}
		return components;
	}

	public List<Object> decode(ByteBuffer rawData, int elements) {
		types = coder.decodeTypes(rawData, elements);

		for(Type t : types) {
			switch(t) {
			case TYPE:
				components.add(coder.decodeCodecType(rawData));
						break;
			case BOOLEAN:
				components.add(coder.decodeBoolean(rawData));
				break;				
			case BYTE:
				components.add(coder.decodeByte(rawData));
				break;
			case CHAR:
				components.add(coder.decodeChar(rawData));
				break;
			case SHORT:
				components.add(coder.decodeShort(rawData));
				break;
			case INTEGER:
				components.add(coder.decodeInteger(rawData));
				break;
			case LONG:
				components.add(coder.decodeLong(rawData));
				break;
			case FLOAT:
				components.add(coder.decodeFloat(rawData));
				break;
			case DOUBLE:
				components.add(coder.decodeDouble(rawData));
				break;
			case STRING:
				String str = coder.decodeString(rawData);
				components.add(str);
				break;
			case BINARY:
				byte[] bytes = coder.decodeBinary(rawData);
				components.add(bytes);
				break;
			case CODEABLE:
				Codeable c = coder.decodeCodeable(rawData);
				components.add(c);
				break;
			default:	
				assert false;
			}
		}
		return components;
	}
	
	private int computeSize() {
		int size = coder.getBitsPerType()*types.size();
		if(size % 8 == 0) 
			size = size/8;
		else
			size = size/8 +1;
		
		int index = 0;
		for(Type t : types){
			size += t.length();
			switch(t){
			case STRING:
				size += ((String)components.get(index)).getBytes().length;  
				break;
			case BINARY:
				size += ((byte[])components.get(index)).length;  
				break;
			case CODEABLE:
				size += ((Codeable)components.get(index)).computeSize();  
				break;
			}
			index++;
		}
		return size;
	}
}
