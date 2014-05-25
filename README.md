flume-async
===========

Prototype replacement Rpc clients for flume.


[![Build Status](https://travis-ci.org/gid79/flume-async.png?branch=master)](https://travis-ci.org/gid79/flume-async)


Status
======

 - [x] API compatible `NettyAvroRpcClient` replacement, using a common `SocketChannelFactory`
 - [x] Replacement `NettyLoadBalancingRpcClient` 
 - [ ] Async replacement client client
 - [ ] Async API for flume
 
Usage
=====

```
client.type = com.logicalpractice.flume.api.NettyLoadBalancingRpcClient
```

```java
```
