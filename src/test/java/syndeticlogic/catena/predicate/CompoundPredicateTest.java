package syndeticlogic.catena.predicate;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.type.IntegerValue;
import syndeticlogic.catena.type.Value;
import syndeticlogic.catena.utility.Codec;

public class CompoundPredicateTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	public byte[] encodeToByteArray(Codec coder, int value) {
		byte[] encoded = new byte[4];
		coder.encode(value,encoded, 0);
		return encoded;
	}
	
	public Value encodeToValue(Codec coder, int value) {
		return new IntegerValue(encodeToByteArray(coder, value), 0);
	}
	
	@Test
	public void test() {
		Codec coder = new Codec(null);
		
		AtomicPredicate predicate = new AtomicPredicate(Operator.LESS_THAN, encodeToValue(coder, 41));
		AtomicPredicate predicate1 = new AtomicPredicate(Operator.GREATER_THAN, encodeToValue(coder, 43));
		CompoundPredicate cp = new CompoundPredicate(BinaryConnector.Conjunction, predicate, predicate1);
		boolean sat = cp.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertTrue(sat);
		
		AtomicPredicate predicate2 = new AtomicPredicate(Operator.LESS_THAN, encodeToValue(coder, 1617));
		sat = predicate.satisfies(encodeToByteArray(coder,1618), 0, 4);
		AtomicPredicate predicate3 = new AtomicPredicate(Operator.GREATER_THAN, encodeToValue(coder, 1619));
		CompoundPredicate cp1 = new CompoundPredicate(BinaryConnector.Conjunction, predicate2, predicate3);
		sat = cp1.satisfies(encodeToByteArray(coder, 1618), 0, 4);
		assertTrue(sat);
		

		CompoundPredicate cp2 = new CompoundPredicate(BinaryConnector.Disjunction, cp, cp1);
		sat = cp2.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertTrue(sat);
		sat = cp2.satisfies(encodeToByteArray(coder, 1618), 0, 4);
		assertTrue(sat);
		sat = cp2.satisfies(encodeToByteArray(coder, 1619), 0, 4);
		assertFalse(sat);
		sat = cp2.satisfies(encodeToByteArray(coder, 41), 0, 4);
		assertFalse(sat);
		sat = cp2.satisfies(encodeToByteArray(coder, 43), 0, 4);
		assertFalse(sat);
		sat = cp2.satisfies(encodeToByteArray(coder, 1617), 0, 4);
		assertFalse(sat);
	}
}
