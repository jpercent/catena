package syndeticlogic.catena.utility;

import java.util.ArrayList;

/**
 * We will keep creating and storing various data types until an OutOfMemory error is thrown ... logging how much memory is free and used as we go.
 * <p>
 * This will be used to profile different JVMs and on different operating systems.
 * 
 * @author benjc
 * 
 */
public class MemoryTest {


    public static void main(String args[]) {

        if (args.length < 1) {
            exitAndPrintUsage();
        }

        String argument = args[0];
        if (argument.trim().equalsIgnoreCase("int")) {
            System.out.println("Running Test: int");
            runTest(1);
        } else if (argument.trim().equalsIgnoreCase("long")) {
            System.out.println("Running Test: long");
            runTest(2);
        } else if (argument.trim().equalsIgnoreCase("string")) {
            System.out.println("Running Test: string");
            runTest(3);
        } else {
            exitAndPrintUsage();
        }

    }

    private static void exitAndPrintUsage() {
        System.out.println("Usage: java MemoryTest <int, long, or string>");
        System.exit(0);
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void runTest(int type) {

        System.out.println("Total Memory: " + Runtime.getRuntime().totalMemory() / 1024 / 1024);
        System.out.println("Max Memory: " + Runtime.getRuntime().maxMemory() / 1024 / 1024);
        System.out.println("Free Memory: " + Runtime.getRuntime().freeMemory() / 1024 / 1024);


        /* a collection to hold all the other collections we'll make */
        ArrayList bucket = new ArrayList(1000000);
        bucket.size();
        System.out.println("Free Memory: " + Runtime.getRuntime().freeMemory() / 1024 / 1024);

        while (true) {

            /**
             * we make the memoryHog list big enough that it won't have to rebuild itself
             * <p>
             * We want to avoid collections hitting their limits, and rebuilding themselves, cause that would falsely increase memory usage in a spike
             * while it copies then releases objects
             */
            ArrayList memoryHog = new ArrayList(150000);
            for (int i = 0; i < 100000; i++) {
                if (type == 1) {
                    memoryHog.add(new Integer(i));
                } else if (type == 2) {
                    memoryHog.add(new Long(9223372036854775807l));
                } else if (type == 3) {
                    /* append the int so that the String is a new string each time, otherwise it will reference the same object in memory */
                    memoryHog.add("a long string that will take up some memory in the memory hog array " + i);
                }
            }

            bucket.add(memoryHog);

            System.out.println("Bucket Size: " + bucket.size() + "   Free Memory: " + Runtime.getRuntime().freeMemory() / 1024 / 1024);

        }


    }


}

