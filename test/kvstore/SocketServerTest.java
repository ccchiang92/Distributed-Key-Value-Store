/**
 * Test code for SocketServer goes here
 */
package kvstore;
import static org.mockito.Mockito.*;
import org.junit.*; 
import java.io.IOException;
public class SocketServerTest 
{
    SocketServer mockServer;

    @Before
    public void init() {
    }

    @Test
    public void runall() throws IOException {
        test1();
    }

    public void test1() throws IOException {
        mockServer = mock(SocketServer.class);
        System.out.println("Beginning SocketServer Test1");
        mockServer.connect();
        mockServer.start();

        /* Verify that the following methods were called the correct number of times */ 
        verify(mockServer).connect();
        verify(mockServer).start();
    }
}
