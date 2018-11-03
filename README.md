# jedis-rebloom

A Java Client Library for [ReBloom](https://rebloom.io)


 ##Reference
 
 https://github.com/RedisLabs/JReBloom
 

 
## Overview
 
This project contains a Java library abstracting the API of the ReBloom Redis module, that implements a high
perfomance bloom filter with an easy-to-use API
 
See [http://rebloom.io](http://rebloom.io) for installation instructions of the module.

## Usage example


Only support redis cluster client
```java
Set<HostAndPort> nodes = new HashSet<>();
		HostAndPort node = new HostAndPort("localhost", 7000);
		HostAndPort node1 = new HostAndPort("localhost", 7001);
	        nodes.add(node);
		nodes.add(node1);
		
```



###BloomFilter
Initializing the client:

```java
import redis.rebloom.bloomfilter
BFClusterClient bfClusterClient = new BFClusterClient(nodes);
```

Adding items to a bloom filter (created using default settings [defaultErrorRate = 0.01 and defaultInitCapacity=100]):

```java
bfClusterClient.add("simpleBloom", "Mark");
// Does "Mark" now exist?
bfClusterClient.exists("simpleBloom", "Mark"); // true
bfClusterClient.exists("simpleBloom", "Farnsworth"); // False
```


Use multi-methods to add/check multiple items at once:

```java
boolean [] rets = bfClusterClient.addMulti("simpleBloom", "foo", "bar", "baz", "bat", "bag");

// Check if they exist:
boolean[] rv = bfClusterClient.existsMulti("simpleBloom", "foo", "bar", "baz", "bat", "mark", "nonexist");
```

Reserve a customized bloom filter:

```java
bfClusterClient.createFilter("specialBloom", 10000, 0.0001);
bfClusterClient.add("specialBloom", "foo");

```

Delte the fitler
```java
bfClusterClient.deleteFilter("specialBloom") //true
```

###CuckooFilter
Initializing the client:

```java
import redis.rebloom.cuckooFilter
CFClusterClient cfClusterClient = new CFClusterClient(nodes);
```


Adding items to cuckoo filter (created using default initCapacity 1000]):
```java
cfClusterClient.add("cfBloom", "Mark"); //true
//add "Mark" again ,still return true
cfClusterClient.add("cfBloom", "Mark"); //true


//add if not exsit
cfClusterClient.addIfNotExist.add("cfBloom", "Mark");//false
cfClusterClient.addIfNotExist.add("cfBloom", "jack");//true

// Does "Mark" now exist?
cfClusterClient.exists("cfBloom", "Mark"); // true
cfClusterClient.exists("cfBloom", "Farnsworth"); // False
```


Insert items to  cuckoo filter with  initCapacity 2000
```java
cfClusterClient.insert(key,2000,["Mike","Tom"]); //[true,true]

cfClusterClient.insertIfNotExist(key,2000,["Mike","Tom"]); //[false,false]
```

Insert items to  cuckoo filter with  defalut initCapacity 1000
```java
cfClusterClient.insert(key,["Mike","Tom"]); //[true,true]

cfClusterClient.insertIfNotExist(key,["Mike","Tom"]); //[false,false]
```


Count the number of times the item exists in the filter
```java
cfClusterClient.count("cfBloom", "jack") //1
cfClusterClient.count("cfBloom", "lucy") //0
```

Delete items from the filter
```java
cfClusterClient.delete("cfBloom","Mark") //true
// "Mark" has been added twice before ,so delete it  still return true
cfClusterClient.delete("cfBloom","Mark") //true

//delete an item  if not exsit in the filter
cfClusterClient.delete("cfBloom","big boom") //false
```

Delte the fitler
```java
cfClusterClient.deleteFilter("cfBloom") //true
```
