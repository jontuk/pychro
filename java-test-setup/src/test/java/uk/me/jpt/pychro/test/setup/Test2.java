package uk.me.jpt.pychro.test.setup;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.nio.file.Files.createTempDirectory;

/**
 * Created by jon on 22/02/2015.
 */

public class Test2
{
    static Logger LOG = LoggerFactory.getLogger(Test2.class.getSimpleName());

    @Test
    public void test1() throws IOException, InterruptedException {
        String path = "/tmp/java-second-writer3";
        Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build();
        chronicle.clear();

        ExcerptAppender appender = chronicle.createAppender();

        for (int i = 0; i < 10; ++i) {
            Thread.sleep(1000);

            appender.startExcerpt();

            appender.writeLong(System.currentTimeMillis());

            appender.finish();
        }

        chronicle.close();
    }

}
