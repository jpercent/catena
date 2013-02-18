package syndeticlogic.catena.performance;

import java.util.LinkedList;

public interface MemoryMonitor extends Monitor {
	
	// Pages free: the total number of free pages in the system
	public double getAverageFreePages();	
	public LinkedList<Long> getRawFreePageMeasurements();

	// Pages active: the total number of pages currently in use and pageable.
	public double getAverageActivePages();
	public LinkedList<Long> getRawActivePageMeasurements();

	//Pages inactive:the total number of pages on the inactive list
	public double getAverageInactivePages();
	public LinkedList<Long> getRawInactivePagesMeasurements();

	// Pages wired down:the total number of pages wired down.  That is, pages that cannot be paged out.
    public double getAverageWiredPages();
	public LinkedList<Long> getRawWiredPagesMeasurements();
	
	// Translation faults: the number of times the "vm_fault" routine has been called
    public double getAverageNumberOfFaultRoutineCalls();
	public LinkedList<Long> getRawNumberOfFaultRoutineCallMeasurements();
   
	// Pages copy-on-write :the number of faults that caused a page to be copied (generally caused by copy-on-write faults).
    public double getAverageCopyOnWriteFaults();
	public LinkedList<Long> getRawCopyOnWriteFaultsMeasurements();

	// Pages zero filled: the total number of pages that have been zero-filled on demand.
    public double getAverageZeroFilledPages();
	public LinkedList<Long> getRawZeroFilledPageMeasurements();

	// Pages reactivated: the total number of pages that have been moved from the inactive list to the active list (reactivated).
    public double getAverageReactivatedPages();
	public LinkedList<Long> getRawReactivatedPagesMeasurements();
    
	// Pageins: the number of requests for pages from a pager (such as the inode pager).
    public double getAveragePageIns();
    public LinkedList<Long> getRawPageInMeasurements();
    
    // Pageouts:  the number of pages that have been paged out.
    public double getAveragePageOuts();
	public LinkedList<Long> getRawPageOutsMeasurements();

}
