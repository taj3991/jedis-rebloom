package redis.rebloom.cuckoofilter;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

/**
 * @since 2018/10/31 20:15
 */
public enum CFCommand implements ProtocolCommand {
	RESERVE("CF.RESERVE"),
	ADD("CF.ADD"),
	ADDNX("CF.ADDNX"),
	INSERT("CF.INSERT"),
	INSERTNX("CF.INSERTNX"),
	COUNT("CF.COUNT"),
	CF_DEL("CF.DEL"),
	EXISTS("CF.EXISTS"),
	DEL("DEL");

	private final byte[] raw;
	CFCommand(String alt) {
		raw = SafeEncoder.encode(alt);
	}
	@Override
	public byte[] getRaw() {
		return raw;
	}
}
