#### A Jedis example where we use the SortedSet datatype to count the annoyance factor of our hungry customers. EveryTime they email us, we log it in a SortedSet with a timestamp as the score. Later, we can fetch the total count or a count for a specific window of time.

* This program showcases how LUA can be used to address the need to measure counts of entries stored in SortedSets
* The idea is - folks from various email addresses ping our restaurant asking if we are offering their favorite dish yet
* We want to count how many such pings we get in total and in the last X seconds
* The Set of whoIsHungry? is established first and the participating members are written to a Redis Set Object - then for each of the members found in that Set, a SortedSet is created that tracks their annoyance factor. These SortedSet representations are examined using ZCOUNT and ZCARD
* It is important to note that for this example to function - all the generated keys use the same routing value
* ^ this allows the LUA script to execute against them all in the same shard/process
*
* You can control the time window (looking back from now) by using the 
  
  ``` 
  --lookbackseconds
  ``` 
  argument
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600"
```
* You can control how many keys show new pings by using the 
```
--keyquantity
```   
argument
*  If you want to clear out the membersKey so that only the --keyquantity number of members is examined you can add the argument --resetmembers
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600 --keyquantity 120 --resetmembers true"
```

```
--entrycount
```
controls how many pings to record in each of the member keys
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600 --keyquantity 12 --entrycount 10"
```

