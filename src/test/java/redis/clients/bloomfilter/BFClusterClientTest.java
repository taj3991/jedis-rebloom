package redis.clients.bloomfilter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.rebloom.bloomfilter.BFClusterClient;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @since 2018/11/2 11:30
 */
public class BFClusterClientTest {

	BFClusterClient bfClusterClient;
	int len = 10;
	String[] items = new String[len];
	String key = "mybf";
	@Before
	public void setUp() throws Exception {
		Set<HostAndPort> nodes = new HashSet<>();
		HostAndPort node = new HostAndPort("localhost", 7000);
		HostAndPort node1 = new HostAndPort("localhost", 7001);
		HostAndPort node2 = new HostAndPort("localhost", 7002);
		HostAndPort node3 = new HostAndPort("localhost", 7003);
		HostAndPort node4 = new HostAndPort("localhost", 7004);
		HostAndPort node5 = new HostAndPort("localhost", 7005);
		nodes.add(node);
		nodes.add(node1);
		nodes.add(node2);
		nodes.add(node3);
		nodes.add(node4);
		nodes.add(node5);
		bfClusterClient = new BFClusterClient(nodes);
		for (int i = 0; i < len; i++) {
			items[i] = "item" + i;
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void add() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(bfClusterClient.add(key, items[i]));
		}

	}

	@Test
	public void add1() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(bfClusterClient.add(key, items[i].getBytes()));
		}

	}

	@Test
	public void addMulti() throws Exception {
		Boolean[] rets = bfClusterClient.addMulti(key, "11", "2", "31");
		for(boolean ret:rets){
			System.out.println(ret);
		}
	}

	@Test
	public void addMulti1() throws Exception {
	}

	@Test
	public void createFilter() throws Exception {
		assertTrue(bfClusterClient.createFilter(key,0.001d,1000));
	}

	@Test
	public void exists() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(bfClusterClient.exists(key, items[i]));
		}
	}

	@Test
	public void exists1() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(bfClusterClient.exists(key, items[i].getBytes()));
		}
	}

	@Test
	public void existsMulti() throws Exception {
		Boolean[] booleans = bfClusterClient.existsMulti(key, items);
		for(Boolean b:booleans){
			assertTrue(b);
		}
	}

	@Test
	public void existsMulti1() throws Exception {
		byte[][] bytes = new byte[items.length][];
		int i = 0;
		for(String item:items){
			bytes[i++] = item.getBytes();
		}
		Boolean[] booleans = bfClusterClient.existsMulti(key, bytes);
		for(Boolean b:booleans){
			assertTrue(b);
		}
	}

	@Test
	public void deleteFilter() throws Exception {
		assertTrue(bfClusterClient.deleteFilter(key));
	}

}