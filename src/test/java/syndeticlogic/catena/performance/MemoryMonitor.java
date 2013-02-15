package syndeticlogic.catena.performance;

import java.util.LinkedList;

public interface MemoryMonitor {
	
	public void start();
	public void finish();
	
	// Pages free: the total number of free pages in the system
	public long getAverageFreePages();	
	public LinkedList<Long> getRawFreePageMeasurements();

	// Pages active: the total number of pages currently in use and pageable.
	public long getAverageActivePages();
	public LinkedList<Long> getRawActivePageMeasurements();

	//Pages inactive:the total number of pages on the inactive list
	public long getAverageInactivePages();
	public LinkedList<Long> getRawInactivePagesMeasurements();

	// Pages wired down:the total number of pages wired down.  That is, pages that cannot be paged out.
    public long getAverageWiredPages();
	public LinkedList<Long> getRawWiredPagesMeasurements();
	
	// Translation faults: the number of times the "vm_fault" routine has been called
    public long getAverageNumberOfFaultRoutineCalls();
	public LinkedList<Long> getRawNumberOfFaultRoutineCallMeasurements();
   
	// Pages copy-on-write :the number of faults that caused a page to be copied (generally caused by copy-on-write faults).
    public long getAverageCopyOnWriteFaults();
	public LinkedList<Long> getRawCopyOnWriteFaultsMeasurements();

	// Pages zero filled: the total number of pages that have been zero-filled on demand.
    public long getAverageZeroFilledPages();
	public LinkedList<Long> getRawZeroFilledPageMeasurements();

	// Pages reactivated: the total number of pages that have been moved from the inactive list to the active list (reactivated).
    public long getAverageReactivatedPages();
	public LinkedList<Long> getRawReactivatedPagesMeasurements();
    
	// Pageins: the number of requests for pages from a pager (such as the inode pager).
    public long getAveragePageIns();
    public LinkedList<Long> getRawPageInMeasurements();
    
    // Pageouts:  the number of pages that have been paged out.
    public long getAveragePageOuts();
	public LinkedList<Long> getRawPageOutsMeasurements();

}
