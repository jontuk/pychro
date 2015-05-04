package uk.me.jpt.pychro.test.setup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.openhft.chronicle.*;
import org.slf4j.*;


/**
 * Created by jon on 11/01/2015.
 */
public class Application
{
    static Logger LOG = LoggerFactory.getLogger(Application.class.getSimpleName());

    void createTestChronicle(String path, int numThreads, int numMessages) throws IOException, InterruptedException
    {
        Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build();
        chronicle.clear();

        ArrayList<Thread> threads = new ArrayList<Thread>();
        ArrayList<BlockingQueue> queues = new ArrayList<BlockingQueue>();

        BlockingQueue out;
        BlockingQueue in;

        for (int i = 0; i < numThreads; ++i) {
            if (i == 0) {
                in = new LinkedBlockingQueue();
                queues.add(in);
            }
            else
                in = queues.get(queues.size()-1);
            out = new LinkedBlockingQueue();
            queues.add(out);

            Thread thread = new Thread(new AddMessagesRunner(i, numThreads, chronicle, in, out));
            thread.start();
            threads.add(thread);
        }
        BlockingQueue first_in = queues.get(0);
        BlockingQueue last_out = queues.get(queues.size()-1);

        for (int i = 0; i < numMessages; ++i) {
            first_in.put(new Integer(i));
            if((Integer)last_out.take() != i)
                throw new RuntimeException("out of order");
        }

        LOG.info(String.format("Created %s with %s threads", path, numThreads));

        //signal threads to close
        first_in.put(new Integer(-1));

        for (int i = 0; i < threads.size(); ++i)
            threads.get(i).join();

        chronicle.close();
    }

    public static void main(String [] args) throws IOException, InterruptedException {

        Application app = new Application();
        /*app.createTestChronicle(args[0] + "/PychroTestChron1.Small", 1, 10);
        app.createTestChronicle(args[0] + "/PychroTestChron2.Small", 2, 10);
        app.createTestChronicle(args[0] + "/PychroTestChron3.Small", 3, 10);
        app.createTestChronicle(args[0] + "/PychroTestChron1.Large", 1, 100_000);
        app.createTestChronicle(args[0] + "/PychroTestChron2.Large", 2, 100_000);
        app.createTestChronicle(args[0] + "/PychroTestChron3.Large", 3, 100_000);*/
        app.createTestChronicle(args[0] + "/PychroTestChron4.XLarge", 5, 3_000_000);
        LOG.info("Done.");
    }
}
