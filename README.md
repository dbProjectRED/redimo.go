# REDIMO

Redimo is a Redis-compatible server that uses AWS DynamoDB as the storage backend, which offers a lot of important benefits:

* The size of your data is no longer limited to the RAM in a single machine – it lives on the near-infinite capacity offered by DynamoDB.
* The Redimo server is stateless and supports load balancing across multiple instances, so you can scale horizontally to any capacity you need. 
* All operations happen completely in parallel, so no single operation is ever held up by any others.
* There are no limits on the number of connections supported – each server supports a large number of connections (limited only by CPU and RAM) and you can add as many servers as you need. 
* DynamoDB provides predictable response times (in single digit milliseconds) at practically any load. 
* A high-availability system is easy to set up.
* The Redimo server is written in Go, so it can run on as little as 32MB of RAM; and will scale linearly with the number of CPU cores you provide.
* The DynamoDB Global Tables feature allows you to have your data *eventually replicated* in many regions across the world, enabling master-master (both reads and writes) at any region.
* The persistence guarantees offered by DynamoDB allows you to use the Redimo service as your primary (and only) database. 

## Compatiblity
Redimo aims for full compatibility with the Redis protocol, so any standard Redis client should work without modification. 

Redimo aims to support all relevant Redis commands, and will return empty non-error values for the commands that do not apply to this scenario. See the lists below to see which commands have been implemented so far.

## Limitations 
While Redimo removes many of the Redis limitations, it does add some of its own, which are primarily carried forward from DynamoDB. 
* If you want Sorted Sets with strong consistency, the total amount of data in a single Sorted Set cannot exceed 10GB. This is because Redimo needs an index for sorted sets, and the LSI (Local Secondary Index) that ensures strong consistency on sequential writes and reads is limited to 10GB. If your applicaton can tolerate eventual consistency in sorted sets, you can configure the index to be a GSI (Global Secondary Index), which has no such limits.
* Redis supports strings up to 512MB as values – Redimo removes this limitation and allows you to store values of any size. But Redis also supports keys as large as 512MB, which Redimo cannot currently support. Redimo will pass through the DynamoDB key size limitation of 1KB. 
* Transactions (`MULTI - WATCH - EXEC`) will be limited to 25 keys and 4MB of data – a limit passed through from DynamoDB.
* The free community version of Redimo has a few restrictions with Pub/Sub and strong vs eventual consistency - see the section below for details.

## Licenses & Limits
Redimo has a free community version that is offered under the GNU AGPL license, which allows you to run the Redimo server **without modification** at no charge and no obligations. If you plan to modify Redimo in any way, you and the applications that connect to it will be subject to the terms of the AGPL license. 

A more permissive commercial license for your organization is available at USD $999 per year. This license allows unrestricted use on an unlimited number of servers and processors. This license also allows private modifications.

Custom / Enterprise licenses are also available, please contact us for a quote.

## Version Differences
* In the free community version of Redimo, Pub/Sub support is bounded to each single instance – no events will be sent or received between servers. The commercial version adds an adapter to AWS IoT Core (a serverless near-real-time messaging platform) to allow Pub/Sub across your entire fleet of servers.
* The free community version of Redimo only allows operations with eventual consistency. The commercial version adds a command and connection level setting that allows switching between eventual and strong consistency. 
* The commercial version also includes an option to use the DAX write-through cache for very high (microseconds) levels of performance.

## Supported Commands

### Connections
* [ ] `AUTH`
* [ ] `ECHO`
* [ ] `PING`
* [ ] `QUIT`
* [ ] `SELECT`
* [ ] `SWAPDB` - Not feasible

### Strings

* [ ] `APPEND`
* [ ] `BITCOUNT`
* [ ] `BITFIELD`
* [ ] `BITOP`
* [ ] `BITPOS`
* [ ] `DECR`
* [ ] `DECRBY`
* [ ] `GET`
* [ ] `GETBIT`
* [ ] `GETRANGE`
* [ ] `GETSET`
* [ ] `INCR`
* [ ] `INCRBY`
* [ ] `INCRBYFLOAT`
* [ ] `MGET`
* [ ] `MSET`
* [ ] `MSETNX`
* [ ] `PSETEX`
* [ ] `SET`
* [ ] `SETBIT`
* [ ] `SETEX`
* [ ] `SETNX`
* [ ] `SETRANGE`
* [ ] `STRLEN`




