package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.support.membermodification.MemberMatcher.method;





@RunWith(PowerMockRunner.class)
@PrepareForTest({KVClient.class, KVMessage.class,Socket.class})
public class KVClientTest2{
		
	    String hostname;

	    @Before
	    public void setUp() throws IOException, InterruptedException {
	        hostname = InetAddress.getLocalHost().getHostAddress();

	      
	    }

   
@Test
public void MockTestSuccess(){
	try{
		Socket mockSock=mock(Socket.class);
		KVClient Test = PowerMockito.spy(new KVClient(hostname,8080));
		PowerMockito.doReturn(mockSock).when(Test, method(KVClient.class, "connectHost"));
		//KVClient2 Test=new KVClient2(hostname,8080);
		//Socket mockSock=Test.getMock();
		int a=0;
		//doNothing().doThrow(new RuntimeException()).when(mockSock).close();
		KVMessage mockReqMessage=PowerMockito.mock(KVMessage.class);
		PowerMockito.whenNew(KVMessage.class).withArguments(Mockito.anyString()).thenReturn(mockReqMessage);
		KVMessage mockReturnMessage=PowerMockito.mock(KVMessage.class);
		PowerMockito.whenNew(KVMessage.class).withArguments(mockSock,a).thenReturn(mockReturnMessage);
		//doNothing().doThrow(new RuntimeException()).when(mockReqMessage).setKey("10");
		//doNothing().doThrow(new RuntimeException()).when(mockReqMessage).setValue("15");
		//doNothing().doThrow(new RuntimeException()).when(mockReqMessage).sendMessage(mockSock);
		when(mockReturnMessage.getMessage()).thenReturn(SUCCESS);
		when(mockReturnMessage.getKey()).thenReturn("10");
		when(mockReturnMessage.getValue()).thenReturn("15");
		//when(mockReqMessage.getMessage()).thenReturn(SUCCESS);
		Test.put("10", "15");
		assertEquals(Test.get("10"),"10");
		Test.del("10");
		
		
		
		 
		/*PowerMockito.verifyPrivate(TestClient, times(1)).invoke("connectHost");
		verify(mockSock, times(3)).close();
		verify(mockReqMessage, times(3)).setKey("10");
		verify(mockReqMessage, times(3)).sendMessage(mockSock);
		verify(mockReqMessage).setValue("15");
		verify(mockReturnMessage).getKey();
		verify(mockReturnMessage).getValue();
		verify(mockReturnMessage).getMessage();*/
	} catch (KVException kve) {
		String errorMsg = kve.getKVMessage().getMessage();
		fail(errorMsg);
	}catch (Exception E){
		fail(E.toString());
	}
}
	@Test
	public void MockTestFailPut(){
		try{
			Socket mockSock=mock(Socket.class);
			KVClient Test = PowerMockito.spy(new KVClient(hostname,8080));
			PowerMockito.doReturn(mockSock).when(Test, method(KVClient.class, "connectHost"));
			int a=0;
			KVMessage mockReqMessage=PowerMockito.mock(KVMessage.class);
			PowerMockito.whenNew(KVMessage.class).withArguments(Mockito.anyString()).thenReturn(mockReqMessage);
			KVMessage mockReturnMessage=PowerMockito.mock(KVMessage.class);
			PowerMockito.whenNew(KVMessage.class).withArguments(mockSock,a).thenReturn(mockReturnMessage);
			when(mockReturnMessage.getMessage()).thenReturn(ERROR_INVALID_FORMAT);
			when(mockReturnMessage.getKey()).thenReturn("10");
			when(mockReturnMessage.getValue()).thenReturn("15");
			Test.put("10", "15");
		
			/*PowerMockito.verifyPrivate(TestClient, times(1)).invoke("connectHost");
			verify(mockSock, times(3)).close();
			verify(mockReqMessage, times(3)).setKey("10");
			verify(mockReqMessage, times(3)).sendMessage(mockSock);
			verify(mockReqMessage).setValue("15");
			verify(mockReturnMessage).getKey();
			verify(mockReturnMessage).getValue();
			verify(mockReturnMessage).getMessage();*/
			fail("did not fail");
		} catch (KVException kve) {
			String errorMsg = kve.getKVMessage().getMessage();
			assertEquals(errorMsg,ERROR_INVALID_FORMAT);
		}catch (Exception E){
			fail(E.toString());
		}
	}
		@Test
		public void MockTestFailGet(){
			try{
				Socket mockSock=mock(Socket.class);
				KVClient Test = PowerMockito.spy(new KVClient(hostname,8080));
				PowerMockito.doReturn(mockSock).when(Test, method(KVClient.class, "connectHost"));
				int a=0;
				KVMessage mockReqMessage=PowerMockito.mock(KVMessage.class);
				PowerMockito.whenNew(KVMessage.class).withArguments(Mockito.anyString()).thenReturn(mockReqMessage);
				KVMessage mockReturnMessage=PowerMockito.mock(KVMessage.class);
				PowerMockito.whenNew(KVMessage.class).withArguments(mockSock,a).thenReturn(mockReturnMessage);
				when(mockReturnMessage.getMessage()).thenReturn(ERROR_INVALID_FORMAT);
				when(mockReturnMessage.getKey()).thenReturn("10");
				when(mockReturnMessage.getValue()).thenReturn("15");
				Test.get("10");
			
				/*PowerMockito.verifyPrivate(TestClient, times(1)).invoke("connectHost");
				verify(mockSock, times(3)).close();
				verify(mockReqMessage, times(3)).setKey("10");
				verify(mockReqMessage, times(3)).sendMessage(mockSock);
				verify(mockReqMessage).setValue("15");
				verify(mockReturnMessage).getKey();
				verify(mockReturnMessage).getValue();
				verify(mockReturnMessage).getMessage();*/
				fail("did not fail");
			} catch (KVException kve) {
				String errorMsg = kve.getKVMessage().getMessage();
				assertEquals(errorMsg,ERROR_INVALID_FORMAT);
			}catch (Exception E){
				fail(E.toString());
			}
		}
			@Test
			public void MockTestFailDel(){
				try{
					Socket mockSock=mock(Socket.class);
					KVClient Test = PowerMockito.spy(new KVClient(hostname,8080));
					PowerMockito.doReturn(mockSock).when(Test, method(KVClient.class, "connectHost"));
					int a=0;
					KVMessage mockReqMessage=PowerMockito.mock(KVMessage.class);
					PowerMockito.whenNew(KVMessage.class).withArguments(Mockito.anyString()).thenReturn(mockReqMessage);
					KVMessage mockReturnMessage=PowerMockito.mock(KVMessage.class);
					PowerMockito.whenNew(KVMessage.class).withArguments(mockSock,a).thenReturn(mockReturnMessage);
					when(mockReturnMessage.getMessage()).thenReturn(ERROR_INVALID_FORMAT);
					when(mockReturnMessage.getKey()).thenReturn("10");
					when(mockReturnMessage.getValue()).thenReturn("15");
					Test.del("10");
				
					/*PowerMockito.verifyPrivate(TestClient, times(1)).invoke("connectHost");
					verify(mockSock, times(3)).close();
					verify(mockReqMessage, times(3)).setKey("10");
					verify(mockReqMessage, times(3)).sendMessage(mockSock);
					verify(mockReqMessage).setValue("15");
					verify(mockReturnMessage).getKey();
					verify(mockReturnMessage).getValue();
					verify(mockReturnMessage).getMessage();*/
					fail("did not fail");
				} catch (KVException kve) {
					String errorMsg = kve.getKVMessage().getMessage();
					assertEquals(errorMsg,ERROR_INVALID_FORMAT);
				}catch (Exception E){
					fail(E.toString());
				}
	
	
    
}

}
