package uk.me.jpt.pychro.test.setup;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jon on 31/01/15.
 */
public class AddMessagesRunner implements Runnable {

    static Logger LOG = LoggerFactory.getLogger(AddMessagesRunner.class.getSimpleName());

    int myNum;
    int numThreads;
    Chronicle chronicle;
    BlockingQueue in;
    BlockingQueue out;
    ExcerptAppender appender;
    StringBuilder sb = new StringBuilder();

    public AddMessagesRunner(int myNum, int numThreads, Chronicle chronicle, BlockingQueue in, BlockingQueue out) throws IOException
    {
        this.myNum = myNum;
        this.numThreads = numThreads;
        this.chronicle = chronicle;
        this.in = in;
        this.out = out;

        this.appender = this.chronicle.createAppender();
    }

    String strOfSize(int size) {
        sb.setLength(0);
        sb.append(size);
        size -= sb.length();

        sb.append("=");
        size--;

        for (int i = 0; i < size; ++i) {
            sb.append(i%10);
        }
        return sb.toString();
    }

    void writeMessage(int command) {

        appender.startExcerpt();

        appender.writeInt(command);

        for (int i = 0; i < command%10; ++i)
        {
            switch (i%5)
            {
                case 0:
                    appender.writeDouble(1 / (double)command);
                    break;
                case 1:
                    appender.writeUTFΔ(strOfSize(command%(1024*10)));
                    break;
                case 2:
                    appender.writeUnsignedByte(command%256);
                    break;
                case 3:
                    appender.writeLong((long)command<<32);
                    break;
                case 4:
                    appender.writeChar('\uD1A9');
                    break;
            }
        }

        appender.finish();
    }

    public void run() {

        Integer command = null;
        while (command == null || command != -1)
        {
            while (true)
            {
                try
                {
                    command = (Integer) this.in.take();
                } catch (InterruptedException e)
                {
                    continue;
                }
                break;
            }

            if (command >= 0 && ((command % numThreads) == myNum)) {
                writeMessage(command);
            }

            while (true)
            {
                try
                {
                    this.out.put((Object) command);
                } catch (InterruptedException e)
                {
                    continue;
                }
                break;
            }
        }

        appender.close();
    }
}

