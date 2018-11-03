package redis.rebloom.cuckoofilter;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Set;

/**
 * @since 2018/10/31 20:14
 */
public class CFClusterClient extends JedisCluster {

	public CFClusterClient(Set<HostAndPort> nodes) {
		super(nodes);
	}

	public CFClusterClient(Set<HostAndPort> nodes, int timeout) {
		super(nodes, timeout);
	}

	public CFClusterClient(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
		super(nodes, timeout, maxAttempts);
	}

	public CFClusterClient(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
		super(nodes, poolConfig);
	}

	public CFClusterClient(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
		super(nodes, timeout, poolConfig);
	}

	public CFClusterClient(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, timeout, maxAttempts, poolConfig);
	}

	public CFClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
	}

	public CFClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
	}

	public CFClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
	}


	private Connection sendCommand(Jedis jedis, CFCommand command, String... args) {
		jedis.getClient().sendCommand(command, args);
		return jedis.getClient();
	}


	private Connection sendCommand(Jedis jedis, CFCommand command, byte[]... args) {
		jedis.getClient().sendCommand(command, args);
		return jedis.getClient();
	}


	/**
	 * Adds an item to the cuckoo filter, creating the filter with defaultInitCapacity = 1000
	 * if it does not exist.
	 * Cuckoo filters can contain the same item multiple times, and consider each insert to be separate.
	 * You can use CF.ADDNX to only add the item if it does not yet exist.
	 *
	 * @param key  The name of the filter
	 * @param item The value to add to the filter
	 * @return true if the item was not previously in the filter.
	 */
	public boolean add(final String key, final String item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.ADD, key, item).getIntegerReply() != 0;
			}
		}.run(key);
	}

	/**
	 * Adds an item to the cuckoo filter, creating the filter  with defaultInitCapacity = 1000
	 * if it does not exist.
	 * Cuckoo filters can contain the same item multiple times, and consider each insert to be separate.
	 * You can use CF.ADDNX to only add the item if it does not yet exist.
	 * Similar to {@link #add(String, String)}
	 *
	 * @param key  The name of the filter
	 * @param item The value to add to the filter
	 * @return true if the item was not previously in the filter.
	 */
	public boolean add(final String key, final byte[] item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.ADD, key.getBytes(), item).getIntegerReply() != 0;
			}
		}.run(key);
	}


	/**
	 * Adds an item to a cuckoo filter if the item did not exist previously.
	 * Creating the filter with defaultInitCapacity = 1000 if it does not exist.
	 * See documentation on CF.ADD for more information on this command.
	 * Note that this command may be slightly slower than CF.ADD because it must first check to see if the item exists
	 *
	 * @param key  The name of the filter
	 * @param item The item to add
	 * @return true if the item was added to the filter, false if the item already exists.
	 */
	public boolean addIfNotExist(final String key, final String item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.ADDNX, key, item).getIntegerReply() != 0;
			}
		}.run(key);
	}

	/**
	 * Adds an item to a cuckoo filter if the item did not exist previously.
	 * Creating the filter with defaultInitCapacity = 1000 if it does not exist.
	 * See documentation on CF.ADD for more information on this command.
	 * Note that this command may be slightly slower than CF.ADD because it must first check to see if the item exists
	 * Similar to {@link #addIfNotExist(String, String)}
	 *
	 * @param key  The name of the filter
	 * @param item The item to add
	 * @return true if the item was added to the filter, false if the item already exists.
	 */
	public boolean addIfNotExist(final String key, final byte[] item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.ADDNX, key.getBytes(), item).getIntegerReply() != 0;
			}
		}.run(key);
	}

	/**
	 * Returns the number of times an item may be in the filter.
	 * Because this is a probablistic data structure, this may not necessarily be accurate.
	 * If you simply want to know if an item exists in the filter, use CF.EXISTS,
	 * as that function is more efficient for that purpose.
	 *
	 * @param key  The name of the filter
	 * @param item The item to count
	 * @return The number of times the item exists in the filter
	 */
	public long count(String key, String item) {
		return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
			@Override
			public Long execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.COUNT, key, item).getIntegerReply();
			}
		}.run(key);
	}


	/**
	 * Similar to {@link #count(String, String)}
	 * Returns the number of times an item may be in the filter.
	 * Because this is a probablistic data structure, this may not necessarily be accurate.
	 * If you simply want to know if an item exists in the filter, use CF.EXISTS,
	 * as that function is more efficient for that purpose.
	 *
	 * @param key  The name of the filter
	 * @param item The item to count
	 * @return The number of times the item exists in the filter
	 */
	public long count(String key, byte[] item) {
		return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
			@Override
			public Long execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.COUNT, key.getBytes(), item).getIntegerReply();
			}
		}.run(key);
	}


	/**
	 * delete an item from the filter
	 *
	 * @param key  The name of the filter
	 * @param item The item to delete from the filter
	 * @return true if the item has been deleted, false if the item was not found.
	 */
	public boolean delete(String key, String item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.CF_DEL, key, item).getIntegerReply() != 0;
			}
		}.run(key);
	}

	/**
	 * delete an item from the filter. Similar to {@link #delete(String, String)}
	 *
	 * @param key  The name of the filter
	 * @param item The item to delete from the filter
	 * @return true if the item has been deleted, false if the item was not found.
	 */
	public boolean delete(String key, byte[] item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.CF_DEL, key.getBytes(), item).getIntegerReply() != 0;
			}
		}.run(key);
	}


	/**
	 * Check if an item exists in the filter
	 *
	 * @param key  The name of the filter
	 * @param item The item to check for
	 * @return false if the item certainlty does not exist, true if the item may exist.
	 * Because this is a probablistic data structure, false positives (but not false negatives) may be returned.
	 */
	public boolean exists(String key, String item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.EXISTS, key, item).getIntegerReply() != 0;
			}
		}.run(key);
	}


	/**
	 * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
	 *
	 * @param key  The name of the filter
	 * @param item The item to check for
	 * @return false if the item certainlty does not exist, true if the item may exist.
	 * Because this is a probablistic data structure, false positives (but not false negatives) may be returned.
	 */
	public boolean exists(String key, byte[] item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.EXISTS, key.getBytes(), item).getIntegerReply() != 0;
			}
		}.run(key);
	}

	/**
	 * Create an empty cuckoo filter with an initial capacity of {capacity} items.
	 * Unlike a bloom filter, the false positive rate is fixed at about 3%, depending on how full the filter is.
	 * The filter will auto-expand (at the cost of reduced performance) if the initial capacity is exceeded,
	 * though the performance degradation is variable depending on how far the capacity is exceeded.
	 * In general, the false positive rate will increase by for every additional {capacity} items beyond initial capacity.
	 *
	 * @param key          The key under which the filter is to be found
	 * @param initCapacity Estimated capacity for the filter.
	 * @return true on success, false otherwise
	 */
	public boolean createFilter(String key, long initCapacity) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.RESERVE, key, initCapacity + "").getStatusCodeReply() != "OK";

			}
		}.run(key);
	}


	/**
	 * Delte the filter
	 *
	 * @param key The key under which the filter is to be found
	 */
	public boolean deleteFilter(String key) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, CFCommand.DEL, key).getIntegerReply() != 0;

			}
		}.run(key);
	}


	/**
	 * Adds one items to a cuckoo filter,
	 * allowing the filter to be created with a custom capacity if it does not yet exist.
	 * These commands offers more flexibility over the ADD and ADDNX commands, at the cost of more verbosity.
	 *
	 * @param key          The name of the filter
	 * @param items        items to add to the filter
	 * @param initCapacity If specified, should be followed by the desired capacity of the new filter,
	 *                     if this filter does not yet exist. If the filter already exists, then this parameter is ignored.
	 *                     If the filter does not yet exist and this parameter is not specified,
	 *                     then the filter is created with the module-level default capacity.
	 *                     See CF.RESERVE for more information on cuckoo filter capacities.
	 * @return > 0 if the item was successfully inserted
	 * 0 if the item already existed and INSERTNX is used.
	 * <0 if an error ocurred
	 * Note that for CF.INSERT, unless an error occurred, the return value will always be an array of >0 values.
	 */
	public List<Long> insert(final String key, final long initCapacity, final String... items) {
		return new JedisClusterCommand<List<Long>>(connectionHandler, maxAttempts) {
			@Override
			public List<Long> execute(Jedis jedis) {
				int offset = 4;
				String[] args = new String[items.length + offset];
				args[0] = key;
				args[1] = "CAPACITY";
				args[2] = initCapacity + "";
				args[3] = "ITEMS";
				System.arraycopy(items, 0, args, offset, items.length);
				return sendCommand(jedis, CFCommand.INSERT, args).getIntegerMultiBulkReply();
			}
		}.run(key);
	}

	/**
	 * Similar to {@link #insert(String,long,String[])} but no spicify capacity
	 * use defaultInitCapacity = 1000
	 * @param key
	 * @param items
	 * @return
	 */
	public List<Long> insert(final String key, final String... items) {
		return new JedisClusterCommand<List<Long>>(connectionHandler, maxAttempts) {
			@Override
			public List<Long> execute(Jedis jedis) {
				int offset = 2;
				String[] args = new String[items.length + offset];
				args[0] = key;
				args[1] = "ITEMS";
				System.arraycopy(items, 0, args, offset, items.length);
				return sendCommand(jedis, CFCommand.INSERT, args).getIntegerMultiBulkReply();
			}
		}.run(key);
	}


	/**
	 * Adds one items to a cuckoo filter, allowing the filter to be created with a custom capacity if it does not yet exist.
	 * These commands offers more flexibility over the ADD and ADDNX commands, at the cost of more verbosity.
	 *
	 * @param key          The name of the filter
	 * @param items        items to add to the filter
	 * @param initCapacity If specified, should be followed by the desired capacity of the new filter,
	 *                     if this filter does not yet exist. If the filter already exists, then this parameter is ignored.
	 *                     If the filter does not yet exist and this parameter is not specified,
	 *                     then the filter is created with the module-level default capacity.
	 *                     See CF.RESERVE for more information on cuckoo filter capacities.
	 * @return > 0 if the item was successfully inserted
	 * 0 if the item already existed and INSERTNX is used.
	 * <0 if an error ocurred
	 * Note that for CF.INSERT, unless an error occurred, the return value will always be an array of >0 values.
	 */
	public List<Long> insertIfNotExist(final String key, final long initCapacity, final String... items) {
		return new JedisClusterCommand<List<Long>>(connectionHandler, maxAttempts) {
			@Override
			public List<Long> execute(Jedis jedis) {
				int offset = 4;
				String[] args = new String[items.length + offset];
				args[0] = key;
				args[1] = "CAPACITY";
				args[2] = initCapacity + "";
				args[3] = "ITEMS";
				System.arraycopy(items, 0, args, offset, items.length);
				return sendCommand(jedis, CFCommand.INSERTNX, args).getIntegerMultiBulkReply();
			}
		}.run(key);
	}


	/**
	 * Similar to {@link #insertIfNotExist(String,long,String[])} but no spicify capacity
	 * use defaultInitCapacity = 1000
	 *
	 * @param key
	 * @param items
	 * @return
	 */
	public List<Long> insertIfNotExist(final String key,  final String... items) {
		return new JedisClusterCommand<List<Long>>(connectionHandler, maxAttempts) {
			@Override
			public List<Long> execute(Jedis jedis) {
				int offset = 2;
				String[] args = new String[items.length + offset];
				args[0] = key;
				args[1] = "ITEMS";
				System.arraycopy(items, 0, args, offset, items.length);
				return sendCommand(jedis, CFCommand.INSERTNX, args).getIntegerMultiBulkReply();
			}
		}.run(key);
	}


}
