# REDIMO

[![Go](https://github.com/sudhirj/redimo.go/workflows/Go/badge.svg)](https://github.com/sudhirj/redimo.go/actions)
[![GoDoc](https://godoc.org/github.com/sudhirj/redimo.go?status.svg)](https://pkg.go.dev/github.com/sudhirj/redimo.go?tab=doc)

Redimo is a library that allows you to use the Redis API on DynamoDB. The DynamoDB system is excellent at what it does on a specific set of use cases, but is more difficult to use than it should be because the API is very low level and requires a lot of arcane knowledge. The Redis API, on the other hand, deals with familiar data structures (key-value, sets, sorted sets, lists, streams, hash maps) that you can directly map to your application's data. Redimo bridges the two and translates the Redis API operations into space / time / cost-efficient DynamoDB API calls. 

Redimo is especially well suited to serverless environments, since there is no pool of connections to handle and DynamoDB is purpose-built for near-zero management use. But you can use it with regular servers well, especially when you want excellent horizontal scalability. See the section on [differences between Redis and DynamoDB](#differences-between-redis-and-dynamodb) below for more information.

### Licensing 
The default license is the [GPL-3](https://tldrlegal.com/license/gnu-general-public-license-v3-(gpl-3)), which obligates you to release any software you write with this library under the same license. I'm also offering the following perpetual licenses:
 
 * [PolyForm Noncommercial License 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/) ($9) for hobbyist / educational / charitable use 
 * [LGPL-3.0](https://tldrlegal.com/license/gnu-lesser-general-public-license-v3-(lgpl-3)) ($99) for commercial use  - allows you to use the library without modifying it, without any obligations to release your code
 * [Apache-2.0](https://tldrlegal.com/license/apache-license-2.0-(apache-2.0)) ($999) - a permissive and enterprise-friendly license if you want to make changes to the library or redistribute code with almost no obligations.
 * If you want a different or custom license you can contact me at sudhir.j@gmail.com - but you'll need to bring a lawyer and be ready to pay for mine. 
 
 Please contact me at sudhir.j@gmail.com and I'll send you an invoice. All licenses are 50% off until the v1 API freeze on the 1st of July. If you're from a developing country, I'm happy to adjust prices to your purchasing power based on the Big Mac Index, so let me know your country and currency.   
 
 All licenses are perpetual and last as long as you use the software. You only need one license per entity (person or company) that owns the code that uses the library. So whether you're an individual, company or consultant / agency, whoever legally owns the code buys one license for all the code they own.
 
 ### Roadmap
 The library is currently in `v0`, so I'm asking for comments and feedback on the interface and data schema. I expect to freeze the API to `v1` on the 1st of July, after which all `v1` releases will be guaranteed not to break backwards compatibility and work on pre-existing `v1` DynamoDB tables without any data migrations. Will also be adding documentation for every method, linking to the corresponding Redis docs and detailing the differences in how they function and what their limitations are.
 
 The first priority is to mirror the Redis API as much as possible, even in the cases where that means the DynamoDB mapping is inefficient. After v1, I'd like to add more efficient operations as extra methods.
 
 This library is the Go version, but I'm thinking of building Ruby, JavaScript, Python and Java versions as well. You can contact me if you'd like to prioritise or sponsor any of them.
 
 ### Limitations
 Some parts of the Redis API are unfeasible (as far as I know, and as of now) on DynamoDB, like the binary / bit twiddling operations and their derivatives, like `GETBIT`, `SETBIT`, `BITCOUNT`, etc. and HyperLogLog. These have been left out of the API for now. 
 
 TTL operations are possible, but a little more complicated, and will likely be added soon.
 
 Pub/Sub isn't possible as a DynamoDB feature itself, but it should be possible to add integration with AWS IoT Core or similar in the future. This isn't useful in a serverless environment, though, so it's a lower priority. Contact me if you disagree and want this quickly.
 
 Lua Scripting is currently not applicable - the library runs inside your codebase, so anything you wanted to do with Lua would just be done with normal library calls inside your application, with the data loaded in and out of DynamoDB. 
 
 ACLs (access control lists) are not currently supported.  
 
 Transactions across arbitrary operations, using `MULTI WATCH EXEC`, are not yet supported. Talk to me if you need this. 
 
 ### Differences between Redis and DynamoDB
 Why bother with this at all? Why not just use Redis?  

* In Redis, the size of your dataset is limited to the RAM available in a single machine, while in DynamoDB it is distributed across many machines and is effectively unlimited. Redis has separate clustering support, but it doesn't match the ease of use and automatic scalability of a managed service like DynamoDB.

* Redis is connection based and supports a limited (but pretty large) number of connections. DynamoDB has no such limits, so it's great for serverless or very horizontally scaled deployments.  

* Redis bandwidth is limited by the network interface of the server that it's running on, DynamoDB is distributed and doesn't have hard bandwidth limits. 

* In Redis all operations run on a single CPU / thread - so each operation is extremely fast, but there's only one operation running at a time. Slow operations, or a large queue, will block other operations. In DynamoDB, each key's operations runs on different machines, so there's no block of any sort across keys. Operations on the same key will block using optimistic concurrency, though - Redimo will try the operation, but will fail if the data has been unexpectedly modified since the last check. The integrity of the data is still preserved, but in DynamoDB there's more probability that lots of operations on the same keys will step on each other's toes.   

* Redis provides very fast response times (few microseconds), but because all operations are happening one by one, the total throughput is fixed. DynamoDB is relatively slower for each individual operation (few milliseconds) but because of the distributed nature of the system the response time will remain constant at thousands or millions of requests per second. Some workloads even get faster at higher loads because the system starts allocating more servers to your table.   

* In Redis a high-availability system isn't that easy to set up, while on DynamoDB it's the default.

* The [DynamoDB Global Tables](https://aws.amazon.com/dynamodb/global-tables/) feature allows you to have your data *eventually replicated* in many regions across the world, enabling master-master (both reads and writes) at any region. Note that if you use Global Tables, the data structures that require lots of transactional updates – streams and lists particularly – cannot work reliably across regions. Mostly because the speed of light isn't as fast as we'd like it to be. This is still excellent for publishing metadata or any information that doesn't change every often throughout the world at low latency. Key-values, hashes, sets, sorted sets, etc. work great in a write-here and read-everywhere scenario. 

* The persistence guarantees offered by DynamoDB allows you to use the Redimo service as your primary / only ACID database, while Redis has a periodic file system sync (so you might lose data since the last sync). While you can switch Redis to wait for a file system write in all cases or set up a quorum cluster, DynamoDB has much higher reliability, including Multi-AZ guarantees, right out of the box. 

* With Redis you'll need to run servers and either buy or rent fixed units of CPU and RAM, while with DynamoDB you have the option of paying on-demand (per request) or setting up auto-scaling bandwidth slabs. 

* With DynamoDB, being a distributed system, you will not get a lot of the transactional and atomic behaviour that comes freely and easily with Redis. The Redimo library uses whatever transactional APIs are available where necessary, so the limits of those APIs will be passed on to you - in DynamoDB you can only have up to 25 items in a transaction, for example, so the `MSET` operation on has a 25 key limit when using Redimo / DynamoDB. Redis does not have any such limitations.

* DynamoDB is geared towards having lots of small objects across many partitions, while Redis is workload agnostic. For example, with Redis if you can do N writes per second across one or many keys, in DynamoDB you can do only one tenth of that on a single key – but you could operate at millions of times N across all your keys, if you have a lot of keys.

* In the same vein, Redis allows all keys and values to be up to 512MB (although big keys or values is always a bad idea and will naturally cause bandwidth constraints on a single server) - in DynamoDB your keys (and set members) can only be up to 1KB, while your values can only be up to 400KB in size.

So there's no clear-cut answer to which is better – it depends entirely on your application and expected workload. The point of this library is not to promote one over the other – it's to make your development much easier and more comfortable if you decide to use DynamoDB, because the Redis API is much more approachable and easier to model with.

If you still need help with making a decision, you can contact me at sudhir.j@gmail.com   
   

 

