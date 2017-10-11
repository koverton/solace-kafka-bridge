package com.solacesystems.poc;

public class MultithreadedRingBufferTest {
    private class TestObj implements HasMsgID {
        public TestObj(long id) {
            this.id = id;
        }
        @Override
        public void setMsgID(long id) {
            this.id = id;
        }
        @Override
        public long getMsgID() {
            return id;
        }
        private long id;
    }

    final RingBuffer<TestObj> buffer = new RingBuffer<>(TestObj.class, 1000000);
    private static final int START = 1;
    private long delcount = 0;
    private long addcount = 0;

    void addLoop(long cycles) {
        long seq = START;
        long reportCycles = cycles / 10;
        while(addcount < cycles) {
            try {
                if (buffer.used() < buffer.capacity()) {
                    buffer.append(new TestObj(seq++));
                    addcount++;
                    if (addcount%reportCycles == 0) System.out.println("Added "+addcount);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    void removeLoop(long cycles) {
        long seq = START;
        long reportCycles = cycles / 10;
        while(delcount < cycles) {
            try {
                if (buffer.used() > 0) {
                    TestObj i = buffer.remove();
                    delcount++;
                    if (i.getMsgID() != seq) {
                        System.out.println("MAYDAY: EXPECTED " + seq + " GOT " + i.getMsgID());
                        System.out.println("ADDED: "+addcount+" REM'D: " + delcount);
                        System.exit(1);
                    }
                    seq++;
                    if (delcount%reportCycles == 0) System.out.println("Deleted "+delcount);
                }
                else {
                    //System.out.println("Can't remove from empty buffer.");
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("\tUSAGE: <long-integer-number-of-adds-to-run>");
            System.out.println("");
            System.exit(1);
        }

        final MultithreadedRingBufferTest test = new MultithreadedRingBufferTest();

        final long cycles = Long.parseLong(args[0]);
        new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        //try { Thread.sleep(1); } catch(InterruptedException e) {}
                        test.removeLoop(cycles);
                    }
                }
        ).start();
        test.addLoop(cycles);
    }
}
