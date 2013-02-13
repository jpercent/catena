package syndeticlogic.catena.relation;

public class TupleMaterializer {
	private RelationDescriptor relationDescriptor;
	private BufferPool buffers;
	
	public TupleMaterializer(RelationDescriptor relationDescriptor) {
		this.relationDescriptor = relationDescriptor;
	}
	
	/**
	 * Gets the next X rows.  Always returns full rows.
	 * @return
	 */
	public boolean getRows() {
		
	}
	
	// scan up to a mb at a time and send it back
	//
	// get a bunch of 1 MB buffers and fill them up
	
}
