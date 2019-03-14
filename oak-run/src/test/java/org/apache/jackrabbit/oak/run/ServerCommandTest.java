package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static junit.framework.TestCase.assertNotNull;

/**
 * Smoke Test for the Server Command class
 *
 * For now only the Oak Segment Tar DataStore is tested here
 */
public class ServerCommandTest {

    private ServerCommand serverCommand;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();
//    @Rule
//    public TemporaryPort serverPort2 = new TemporaryPort();

    @Before
    public void before() {
        serverCommand = createServerCommand();
    }

    @Test
    public void testSegmentTarDS() throws Exception {
        serverCommand.execute(
            new String[] {
                "http://localhost:" + serverPort.getPort(),
                "Oak-Segment-Tar-DS",
                "--base", temporaryFolder.getRoot().getAbsolutePath() + "/" + "oak-segment-tar-ds.test.tar",
                "--dsCache", "123",
            }
        );
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSegmentTarDSWithoutBase() throws Exception {
        serverCommand.execute(
            new String[] {
                "http://localhost:" + serverPort.getPort(),
                "Oak-Segment-Tar-DS",
                "--dsCache", "123"
            }
        );
    }


    private ServerCommand createServerCommand() {
        ServerCommand answer = new ServerCommand();
        assertNotNull(answer);
        return answer;
    }
}
