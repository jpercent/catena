package syndeticlogic.catena.predicate;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.type.IntegerValue;
import syndeticlogic.catena.type.Value;
import syndeticlogic.catena.utility.Codec;

public class AtomicPredicateTest {

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
		AtomicPredicate predicate = new AtomicPredicate(Operator.GREATER_THAN, encodeToValue(coder, 42));
		boolean sat = predicate.satisfies(encodeToByteArray(coder, 43), 0, 4);
		assertFalse(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 41), 0, 4);
		assertTrue(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertFalse(sat);

		predicate = new AtomicPredicate(Operator.GREATER_THAN_EQUAL, encodeToValue(coder, 42));
		sat = predicate.satisfies(encodeToByteArray(coder, 43), 0, 4);
		assertFalse(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 41), 0, 4);
		assertTrue(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertTrue(sat);

		
		predicate = new AtomicPredicate(Operator.EQUALS, encodeToValue(coder, 42));
		sat = predicate.satisfies(encodeToByteArray(coder, 43), 0, 4);
		assertFalse(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 41), 0, 4);
		assertFalse(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertTrue(sat);

		
		predicate = new AtomicPredicate(Operator.LESS_THAN, encodeToValue(coder, 42));
		sat = predicate.satisfies(encodeToByteArray(coder, 43), 0, 4);
		assertTrue(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 41), 0, 4);
		assertFalse(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertFalse(sat);


		predicate = new AtomicPredicate(Operator.LESS_THAN_EQUAL, encodeToValue(coder, 42));
		sat = predicate.satisfies(encodeToByteArray(coder, 43), 0, 4);
		assertTrue(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 41), 0, 4);
		assertFalse(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertTrue(sat);
		

		predicate = new AtomicPredicate(Operator.NOT_EQUALS, encodeToValue(coder, 42));
		sat = predicate.satisfies(encodeToByteArray(coder, 43), 0, 4);
		assertTrue(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 41), 0, 4);
		assertTrue(sat);
		sat = predicate.satisfies(encodeToByteArray(coder, 42), 0, 4);
		assertFalse(sat);

	
	
	
	}
}
