package com.redislabs.sa.ot.aggz;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;

/**
 * This program showcases how LUA can be used to address the need to measure counts of entries stored in SortedSets
 * The idea is - folks from various email addresses ping our restaurant asking if we are offering their favorite dish yet
 * We want to count how many such pings we get in total and in the last X seconds
 * The Set of whoIsHungry? is established first - then the members of that Set are examined using ZCOUNT and ZCARD
 * It is important to note that for this example to function - all the generated keys use the same routing value
 * ^ this allows the LUA script to execute against them all in the same shard/process
 *
 * You can control the time window (looking back from now) by using the --lookbackseconds argument
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600"
 * You can control how many keys show new pings by using the --keyquantity argument
 *  If you want to clear out the membersKey so that only the --keyquantity number of members is examined you can add the argument --resetmembers
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600 --keyquantity 120 --resetmembers true"
 * --entrycount controls how many pings to record in each of the member keys
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600 --keyquantity 12 --entrycount 10"
 *
 */
public class Main {

    static String host = "localhost";
    static int port = 6379;
    static String username = "default";
    static String password = "";
    static URI jedisURI = null;
    static boolean isReadOnly=false;
    static int lookBackSeconds = 3;
    static ConnectionHelper connectionHelper = null;
    static String membersKey = "{whoIsHungry?}";
    static String sampleMembersKey = "{whoIsHungry?}:sampleSet";
    static int targetKeyQuantity = 25;
    static int keyQuantity = 0;//will set this based on the number of members in the set if not provided as an argument
    static int entriesToAddToKeys = 25;
    static boolean resetMembers = false;
    static int keysInSample = 0;
    static boolean useTLS = false;

    //
    public static void main(String [] args){
        if(args.length>0) {
            ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--host")) {
                int hostIndex = argList.indexOf("--host");
                host = argList.get(hostIndex + 1);
            }
            if (argList.contains("--port")) {
                int portIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(portIndex + 1));
            }
            if (argList.contains("--user")) {
                int userNameIndex = argList.indexOf("--user");
                username = argList.get(userNameIndex + 1);
            }
            if (argList.contains("--password")) {
                int passwordIndex = argList.indexOf("--password");
                password = argList.get(passwordIndex + 1);
            }
            if (argList.contains("--readonly")) {
                int index = argList.indexOf("--readonly");
                isReadOnly = Boolean.parseBoolean(argList.get(index + 1));
            }
            if (argList.contains("--resetmembers")) {
                int index = argList.indexOf("--resetmembers");
                resetMembers = Boolean.parseBoolean(argList.get(index + 1));
            }
            if (argList.contains("--lookbackseconds")) {
                int index = argList.indexOf("--lookbackseconds");
                lookBackSeconds = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--memberskey")) {
                int index = argList.indexOf("--memberskey");
                membersKey = argList.get(index + 1);
            }
            if (argList.contains("--targetkeyquantity")) {
                int index = argList.indexOf("--targetkeyquantity");
                targetKeyQuantity = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--entrycount")) {
                int index = argList.indexOf("--entrycount");
                entriesToAddToKeys = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--samplesize")) { // example 100
                int index = argList.indexOf("--samplesize");
                keysInSample = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--usetls")) {
                int index = argList.indexOf("--usetls");
                useTLS = Boolean.parseBoolean(argList.get(index + 1));
            }
        }
        connectionHelper = new ConnectionHelper(ConnectionHelper.buildURI(host,port,username,password,useTLS));
        testJedisConnection(host,port);
        keyQuantity = Integer.parseInt((""+connectionHelper.getPooledJedis().scard(membersKey)));
        if(!isReadOnly) {
            System.out.println("\nAdding "+entriesToAddToKeys+" entries to each of the "+keyQuantity+" keys that are members of the set "+membersKey);
            if(connectionHelper.getPooledJedis().scard(membersKey)<targetKeyQuantity) {
                System.out.println("The set "+membersKey+" has only "+connectionHelper.getPooledJedis().scard(membersKey)+" members, but you wanted to add entries to "+targetKeyQuantity+" members - so we will add the missing members");
                buildSetOfTargetKeys(membersKey,targetKeyQuantity);
            }
            writeZ(membersKey,entriesToAddToKeys);
        }
        if(keysInSample>0){
            System.out.println("\nQuerying a sample of "+keysInSample+" keys out of the total "+connectionHelper.getPooledJedis().scard(membersKey)+" members in the set "+membersKey);
            buildSetOfTargetKeys(sampleMembersKey,keysInSample);
            showCaseCountingZ(sampleMembersKey,lookBackSeconds);            
        }else{
            showCaseCountingZ(membersKey,lookBackSeconds);
        }
    }

    static void clearSetOfTargetKeys(String setName){
        Pipeline jedisPipe = connectionHelper.getPipeline();
        if(resetMembers){
            String luaCleanup = "local cursor = 0 local keyNum = 0 repeat " +
                    "local res = redis.call('scan',cursor,'MATCH',KEYS[1]..'*') " +
                    "if(res ~= nil and #res>=0) then cursor = tonumber(res[1]) " +
                    "local ks = res[2] if(ks ~= nil and #ks>0) then " +
                    "for i=1,#ks,1 do " +
                    "local key = tostring(ks[i]) " +
                    "redis.call('UNLINK',key) end " +
                    "keyNum = keyNum + #ks end end until( cursor <= 0 ) return keyNum";
            jedisPipe.eval(luaCleanup,1,membersKey);
            jedisPipe.del(setName);
        }
        jedisPipe.del(setName);
        jedisPipe.sync();
    }

    static void buildSetOfTargetKeys(String setName,int howmany){
        System.out.println("\nStoring a set of "+howmany+" members in the keyname "+setName);
        Pipeline jedisPipe = connectionHelper.getPipeline();
        jedisPipe.del(setName);
        for(int x=0;x<howmany;x++){
            jedisPipe.sadd(setName,membersKey+"email"+x+"@email.com");
        }
        jedisPipe.sync();
    }

    //returns the set of keynames suitable for our use 
    //would be a set targeting a specific grouping such as {whoIsHungry?} or a sample of that set such as {whoIsHungry?}:sampleSet
    static Set<String> getSetOfTargetKeys(String setName) {
        Pipeline jedisPipe = connectionHelper.getPipeline();
        jedisPipe.smembers(setName);
        List<Object> response = jedisPipe.syncAndReturnAll();
        Set<String> keySet = null;
        for (Object o : response) {
            if (o instanceof java.util.HashSet) {
                keySet = (Set<String>) o;
            }
        }
        return keySet;
    }

    private static void testJedisConnection(String host,int port) {
        System.out.println("\ntesting jedis connection using URI == "+host+":"+port);
        JedisPooled jedis = connectionHelper.getPooledJedis();
        System.out.println("Testing connection by executing 'DBSIZE' response is: "+ jedis.dbSize());
    }


    //For each member, we can write a SortedSet that tracks email-address-related ping events with timestamps
    //passed in is the setOfKeys that contains the keynames to which we want to add entries and the number of entries to add to each key
    static void writeZ(String setOfKeys,int numberEntries) {
        Pipeline jedisPipe = connectionHelper.getPipeline();
        Set<String> rSet = getSetOfTargetKeys(setOfKeys);
        long delta = 0;
        for (String targetKeyName : rSet) {
            for (int x = 0; x < numberEntries; x++) {
                delta = System.nanoTime() % 30000;
                double timestamp = System.currentTimeMillis() - delta;
                jedisPipe.zadd(targetKeyName, timestamp, timestamp + "");
            }
        }
        jedisPipe.sync();
    }

        //
    static void showCaseCountingZ(String setOfKeys,long secondsBackInTime) {
        //SortedSet API offers ZCARD and ZCOUNT:
        String luaScript = "local resultString = '' local ruleSetKey = ARGV[1] " +
                "local txTime = ARGV[2] local lookBackSeconds = ARGV[3] " +
                "local keyNames = redis.call('SMEMBERS',ruleSetKey) " +
                "if #{keyNames} > 0 then " +
                "local innerLoop = 1 " +
                "while #{keyNames[innerLoop]} > 0 " +
                "do resultString = resultString..' '" +
                "..'\\n'..keyNames[innerLoop]..' '..(redis.call('ZCARD',keyNames[innerLoop]))..' '" +
                "..(redis.call('ZCOUNT',keyNames[innerLoop],(txTime-(lookBackSeconds*1000)),txTime))" +
                " innerLoop=(innerLoop+1) end end return resultString";
        JedisPooled jedis = connectionHelper.getPooledJedis();
        double timestamp = System.currentTimeMillis();
        Object lrObject = jedis.eval(luaScript,1,setOfKeys,setOfKeys,""+timestamp,""+lookBackSeconds);
        String luaResponse = (String) lrObject;
        System.out.println("\n\nCalling the lua script (round-trip) with SMEMBERS/SCARD/ZCOUNT logic took "+(System.currentTimeMillis()-timestamp+" milliseconds"));
        // 1. Print the table header with defined column widths
        // %-25s means: String, left-justified, 25 characters wide
        System.out.printf("\n%-35s %-24s %-24s%n", "Key Name", "|| Total Count [zcard]", "|| Window Count [zcount]");
        System.out.println("--------------------------------------------------------------------------------------");

        // 2. Split the response into rows and print each row formatted
        String[] rows = luaResponse.split("\n");
        for (String row : rows) {
            if (!row.trim().isEmpty()) {
                // Split by whitespace to extract individual fields
                String[] fields = row.split("\\s+"); 
                
                if (fields.length >= 3) {
                    System.out.printf("%-44s %-24s %-24s%n", fields[0], fields[1], fields[2]);
                }
            }
        }
        System.out.println("The script gathered the data from "+jedis.scard(setOfKeys)+" keys");
        System.out.println("The time window addressed was "+lookBackSeconds+" seconds");
        String exampleZSetKey = jedis.srandmember(setOfKeys);
        System.out.println("An example of one of the keys stored is "+exampleZSetKey+" which holds the following data: "+jedis.zrangeWithScores(exampleZSetKey,0,-1));
    }
}

class ConnectionHelper{

    final PooledConnectionProvider connectionProvider;
    final JedisPooled jedisPooled;

    /**
     * Used when you want to send a batch of commands to the Redis Server
     * @return Pipeline
     */
    public Pipeline getPipeline(){
        return  new Pipeline(jedisPooled.getPool().getResource());
    }

    /**
     * Assuming use of Jedis 4.3.1:
     * https://github.com/redis/jedis/blob/82f286b4d1441cf15e32cc629c66b5c9caa0f286/src/main/java/redis/clients/jedis/Transaction.java#L22-L23
     * @return Transaction
     */
    public Transaction getTransaction(){
        return new Transaction(jedisPooled.getPool().getResource());
    }

    /**
     * Obtain the default object used to perform Redis commands
     * @return JedisPooled
     */
    public JedisPooled getPooledJedis(){
        return jedisPooled;
    }

    /**
     * Use this to build the URI expected in this classes' Constructor
     * @param host
     * @param port
     * @param username
     * @param password
     * @param useTls - if true, the URI will be built with the rediss:// scheme which will trigger TLS/SSL in the JedisClientConfig 
     * @return
     */
    public static URI buildURI(String host, int port, String username, String password, boolean useTls) {
        URI uri = null;
        // Use rediss:// for TLS, redis:// for standard
        String scheme = useTls ? "rediss://" : "redis://"; 
        try {
            if (password != null && !password.trim().isEmpty()) {
                uri = new URI(scheme + username + ":" + password + "@" + host + ":" + port);
            } else {
                uri = new URI(scheme + host + ":" + port);
            }
        } catch (URISyntaxException use) {
            use.printStackTrace();
            System.exit(1);
        }
        return uri;
    }

    public ConnectionHelper(URI uri){
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        
        // Check if the URI scheme dictates a TLS connection
        boolean isSsl = "rediss".equalsIgnoreCase(uri.getScheme());

        DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(30000)
                .timeoutMillis(120000)
                .ssl(isSsl); // <--- CRITICAL: Enables TLS/SSL

        System.out.println("$$$ "+uri.getAuthority().split(":").length);
        if(uri.getAuthority().split(":").length==3){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1].split("@")[0];
            
            System.out.println("\n\nUsing user: "+user+" / password @@@@@@@@@@"+password);
            configBuilder.user(user).password(password);
        }

        JedisClientConfig clientConfig = configBuilder.build();

        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(1000);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWait(Duration.ofMinutes(1));
        poolConfig.setTestOnCreate(true);

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
    }
}
