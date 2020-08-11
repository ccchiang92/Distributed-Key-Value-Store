/**
 * This file will be used to initialize all desired unit test for Mokito
 */
package kvstore;
import static org.mockito.Mockito.*;
public class MokitoInitTest
{
    public static void main(String[] args) {
        Task5();
    }

    /* Tests for associated tasks get called here */
    public static void Task5() {
        ServerClientHandlerTest.runall();
        ThreadPoolTest.runall();
        SocketServerTest.runall();    
    }

    public static void Task4() {
    }

    public static void Task3() {
    }

    public static void Task2() {
    }

    public static void Task1() {
    }
} 
