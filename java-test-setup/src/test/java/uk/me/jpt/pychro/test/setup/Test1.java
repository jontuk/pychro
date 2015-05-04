package uk.me.jpt.pychro.test.setup;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.OSEnvironment;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempDirectory;

/**
 * Created by jon on 11/01/2015.
 */
public class Test1
{
    static Logger LOG = LoggerFactory.getLogger(Test1.class.getSimpleName());

    @Test
    public void test1() throws IOException, InterruptedException
    {
        Path dir = createTempDirectory("pychro-java-test-setup");
        String path = dir.toString() + "/PychroTestChron3.Small";

        new Application().createTestChronicle(path, 3, 10);
        
        Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build();
        ExcerptTailer tailer = chronicle.createTailer();

        int i = 0;
        int day = -1;
        while (tailer.nextIndex()) {
            int command = tailer.readInt();
            if (command == 0)
                day++;
            LOG.info(String.format("command=%s(%s)", command, day));
            Assert.assertEquals(i, command+10*day);

            for (int j = 0; j < command%10; ++j)
            {
                Object val = null;
                switch (j%5)
                {
                    case 0:
                        val = tailer.readDouble();
                        Assert.assertEquals(1 / (double)command, (double)val, 0);
                        break;
                    case 1:
                        val = tailer.readUTFΔ();
                        break;
                    case 2:
                        val = tailer.readUnsignedByte();
                        break;
                    case 3:
                        val = tailer.readLong();
                        break;
                    case 4:
                        val = tailer.readChar();
                        break;
                }
                LOG.info(String.format("%s: %s", j, val));
            }
            LOG.info("");

            tailer.finish();
            i++;
        }

        tailer.close();
        chronicle.close();
    }
}
