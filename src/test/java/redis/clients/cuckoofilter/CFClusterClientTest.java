package redis.clients.cuckoofilter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.rebloom.cuckoofilter.CFClusterClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @since 2018/11/2 10:30
 */
public class CFClusterClientTest {

	CFClusterClient cfClusterClient = null;

	int len = 10;
	String[] items = new String[len];
	String key = "mycf";

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
		cfClusterClient = new CFClusterClient(nodes);

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
			assertTrue(cfClusterClient.add(key, items[i]));
		}


	}

	@Test
	public void addByte() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(cfClusterClient.add(key, items[i].getBytes()));
		}
	}

	@Test
	public void addIfNotExist() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(cfClusterClient.addIfNotExist(key, items[i]));
		}

	}

	@Test
	public void addIfNotExistByte() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(cfClusterClient.addIfNotExist(key, items[i].getBytes()));
		}
	}


	@Test
	public void count() throws Exception {
		for (int i = 0; i < len; i++) {
			long count = cfClusterClient.count(key, items[i]);
			System.out.println(count);
			assertTrue(count > 0);
		}

	}

	@Test
	public void countByte() throws Exception {
		for (int i = 0; i < len; i++) {
			long count = cfClusterClient.count(key, items[i].getBytes());
			System.out.println(count);
			assertTrue(count > 0);
		}
	}


	@Test
	public void delete() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(cfClusterClient.delete(key, items[i]));
		}
	}

	@Test
	public void deleteByte() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(cfClusterClient.delete(key, items[i].getBytes()));
		}
	}

	@Test
	public void exists() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(cfClusterClient.exists(key, items[i]));
		}
	}

	@Test
	public void existsByte() throws Exception {
		for (int i = 0; i < len; i++) {
			assertTrue(cfClusterClient.exists(key, items[i].getBytes()));
		}
	}

	@Test
	public void createFilter() throws Exception {
		assertTrue(cfClusterClient.createFilter(key,1000));
	}

	@Test
	public void deleteFilter() throws Exception {
		assertTrue(cfClusterClient.deleteFilter(key));
	}

	@Test
	public void insert() throws Exception {
		String [] items ={"wow","hoo"};
		List<Long> ret = cfClusterClient.insert(key,1000,items);
		assertTrue(ret.get(0) == 1);
		assertTrue(ret.get(1) == 1);
	}

	@Test
	public void insert2() throws Exception {
		String [] items ={"wow","hoo"};
		List<Long> ret = cfClusterClient.insert(key,items);
		assertTrue(ret.get(0) == 1);
		assertTrue(ret.get(1) == 1);
	}


	@Test
	public void insertIfNotExist() throws Exception {
		String [] items ={"wow11","hoo11"};
		List<Long> ret = cfClusterClient.insertIfNotExist(key,100,items);
		assertTrue(ret.get(0) == 1);
		assertTrue(ret.get(1) == 1);
	}

	@Test
	public void insertIfNotExist2() throws Exception {
		String [] items ={"wow","hoo"};
		List<Long> ret = cfClusterClient.insertIfNotExist(key,items);
		assertTrue(ret.get(0) == 1);
		assertTrue(ret.get(1) == 1);
	}

}