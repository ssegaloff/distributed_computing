# DS 7200 Study Guide — Modules 1-3

Sep 25, 2026 · Reference notes for journaling and review

## How to use this guide

Everything here comes from the files in your repo's Module 1–3 folders: the lecture notebooks, the lecture PDFs, the reading lists, and your completed labs. Where I added an explanation or example that isn't in the course material, it's marked **(my addition)**. Where the source material itself has a problem, it's marked **Heads up**.

The guide follows the repo's folder structure, so each lecture sits in the module where the course put it. That means the *Intro to Distributed Systems* lecture is in Module 2, and the *System Architecture* and *Unique ID Generator* lectures are in Module 3.

A few things the course assigns aren't in the repo, so this guide can't cover them from source: the MapReduce paper, the *Learning Spark* and *Learning PySpark* chapters, and *Designing Data-Intensive Applications* Ch. 1. The one place I summarize one of those readings is marked.

## Module 1: Big Data & Distributed Systems

This module sets up the vocabulary for the rest of the course. It covers the components big data systems are built from, MapReduce (the processing model Spark grew out of), your first Spark code, and two core ideas about storing data across many machines: the CAP theorem and consistent hashing.

### System building blocks

Big data systems are assembled from a small set of recurring components. Learning them is mostly vocabulary, but it's the vocabulary every architecture diagram in the course uses.

- **Server:** hardware or software that provides a service to another program and its user. Often a powerful machine. Examples: a database server, a web server.
- **Client:** the user program that connects to a server to use its service, such as a web browser.
- **Database:** an organized collection of data stored electronically. Relational databases store tables that can be joined on shared fields; non-relational (NoSQL) databases store other shapes, like documents or key-value pairs.
- **Data warehouse vs. data lake:** both are central data repositories. A warehouse holds **structured** data, usually a collection of relational databases across an organization. A lake holds data of **any** structure, so semi-structured data (JSON) and unstructured data (images, text) fit too.
- **Load balancer:** sits in front of a group of servers and spreads incoming work across them. If one web server is overwhelmed by traffic, you put several servers behind a load balancer and it distributes requests evenly.
- **Front end vs. back end:** the front end is the visual layer users see, like a website (usually UX designers' territory). The back end holds structure, business logic, and data, including the database.
- **Cloud provider:** a company that maintains hardware and software you rent with pay-as-you-go pricing. The big three are AWS, Microsoft Azure, and Google Cloud Platform. This course uses AWS.

### DNS (Domain Name System)

DNS gets its own section in the lecture because it's critical and is often the cause of large system failures. It maps domain names to IP addresses, so your computer knows where to connect.

1. You type `www.virginia.edu` into a browser.
2. Your computer asks a DNS server to look up the site's IP address.
3. The DNS server responds with the address (`132.148.77.44` in the lecture example).
4. Your browser connects to that address and loads the site.

DNS is **globally distributed** (servers in many data centers around the world) and **cached** (earlier lookups are kept so they don't have to be repeated). Records update as infrastructure changes, sometimes by hand. Things that go wrong include a wrong IP, a deleted record, or circular references. Any of these can make a system unreachable even when its servers are running fine.

### Scaling: vertical vs. horizontal

When a system needs more computing power, there are two options.

- **Vertical scaling (scale up):** give one server more resources, such as CPUs, RAM, or disk. It's relatively expensive, hardware sets a ceiling on how far you can go, and the server stays a **single point of failure**: if it goes down, the application goes down with it.
- **Horizontal scaling (scale out):** add more servers. Cloud services make this easy and relatively cheap, and the extra servers give you redundancy, so there's no single point of failure.

**(My addition)** The cost of horizontal scaling is that data and work now have to be split across machines that communicate over a network. Handling that split well is what most of this course is about.

### Batch vs. streaming

- **Batch job:** runs on a finite amount of data, such as computing analytics or making a set of predictions.
- **Streaming job:** runs on data that never ends, such as processing a live X feed. Streaming is harder because with infinite data you have to decide what to save and when to report results.

### Architecture diagrams and the three-tier architecture

Architecture diagrams show a system at a high level so it's easy to understand. Solution architects usually draw them. The most popular pattern is the **three-tier architecture**:

1. **Presentation layer** (the notebook calls it the "design layer"): the front-end website.
2. **Application layer:** business logic, machine learning models, and so on.
3. **Data layer:** the database.

The lecture's example diagram (from *Solution Architect's Handbook*) builds this with AWS services:

- **EC2:** servers, which AWS calls *instances*. One fleet of instances runs the web layer and a separate fleet runs the application layer. Fleets grow or shrink as load changes, which is horizontal scaling.
- **Amazon RDS:** a managed relational database service. Good practice is to keep one or more backups for redundancy; *read replicas* (read-only copies) are common.
- **Route 53:** AWS's DNS service.
- **S3 (Simple Storage Service):** a data lake. It's an *object store* built on key-value pairs, so it's a NoSQL store. It's very common to keep all of a system's data in S3.

### The big data pipeline

At a high level, every big data system goes through four steps: **ingestion → storage → processing → reporting & visualization**. Each step is hard at massive scale, and specialized software and hardware exist for each.

- **Ingestion:** collecting data for transfer and storage. Data comes from databases, streams, logs, and files: devices (sensors, phones, wearables), clickstream logs, server logs, images. It can be flat files, images, video, or audio. Tools for streaming ingestion include **Apache Kafka** and **Amazon Kinesis**.
- **Storage:** the right choice depends on the data's structure and how it will be used. Most data at rest lives in relational databases. Semi-structured data (like JSON) and unstructured data usually go in NoSQL stores. JSON is popular for passing data between applications (APIs) because its structure is simple and hierarchical. Systems often combine storage options to balance cost against latency (how fast data must be delivered).
- **Processing:** where analytics and predictive modeling happen. Spark is the main tool in this course. It processes data **in memory**, using the RAM on each machine (*node*). The course covers analytics, Spark SQL, machine learning with MLlib, and Spark Streaming.
- **Reporting & visualization:** pulling insight out of results. Tools include Tableau (interactive visuals), Power BI (Microsoft), and Kibana (open source, used for stream data and logs).

### MapReduce

MapReduce is a programming framework for processing large datasets with a parallel, distributed algorithm on a cluster. Google engineers published it in 2004. The core idea has two steps: **map** the dataset into a collection of `<key, value>` pairs, then **reduce** over all the pairs that share a key. The model is simple, but a surprisingly wide range of problems fits it. The standard minimal example is counting words.

**Vocabulary.** A MapReduce **job** is a unit of work the client wants done. It consists of input data, a MapReduce program, and configuration. The job is divided into **tasks** of two types: map tasks and reduce tasks.

**The four steps in the word count diagram:**

1. **Splitting:** the input is divided into chunks (*splits*), and each worker gets a set of records. More splits means less processing time per split, but past a certain point the overhead of managing splits and creating map tasks starts to dominate total job time.
2. **Mapping:** each word becomes a `<word, 1>` pair, where the 1 is a count. Nothing is added up yet: if a word appears *n* times on a worker, that worker holds *n* separate `<word, 1>` pairs.
3. **Shuffling:** pairs are moved across machines so that all pairs with the same key end up on the same machine. This is the costly step, because data travels over the network.
4. **Reducing:** a *reducer* applies an operation to the values for each key. For word count, it sums the 1s to get each word's total.

**Map and reduce in general.** Both are *higher-order functions* (functions that take other functions as input). **Map** applies a function to every element of a list, such as `x => x**2`. **Reduce** combines elements with an operation, such as a running sum.

**Combiner functions.** Shuffling is expensive, and the cluster's network bandwidth is the limiting factor. A **combiner** runs on each node's map output *before* the shuffle, doing a local partial reduce so less data has to move. This doesn't work for every operation: a combiner can compute a maximum but not an average. **(My addition)** The max of each node's local max is the true max. An average of per-node averages is wrong whenever the nodes hold different numbers of values.

**Hadoop.** MapReduce is the computation model behind Hadoop. Hadoop was created by Doug Cutting and Mike Cafarella (an earlier version was called Nutch). Yahoo! provided a dedicated team and resources that turned it into a system that ran at web scale. The Apache Software Foundation develops and maintains it now.

**Hadoop vs. Spark.** Spark doesn't make you write explicit `Mapper` and `Reducer` functions the way Hadoop does, and the lecture calls that a big advantage. Reported speeds: Spark can be up to 10x faster than MapReduce for batch processing and up to 100x faster for in-memory analytics. The two also work together: a Spark job can read from and write to Hadoop's storage layer (HDFS).

### Getting started with Spark

**Why Spark:**

- Built to be **fast**, so you can work with data interactively instead of waiting hours.
- Built to handle big data.
- **General purpose.** Unlike Hadoop, it has several modules in one place: machine learning, SQL queries, streaming, and graph analytics.
- **Caching:** intermediate data can be kept in memory on the workers.
- **Accessible:** simple APIs for Python, Java, Scala, R, and SQL. It works with other big data tools (Hadoop, Cassandra) and can read from HDFS, Amazon S3, and other storage.

**Key vocabulary:**

- **Cluster:** a set of connected computers (*nodes*).
- **SparkSession:** the single entry point to Spark's functionality. For RDD work you also need its SparkContext: `sc = spark.sparkContext`.
- **Driver program:** holds the application's main function, defines RDDs on the cluster, and applies operations to them.
- **Worker node / executor:** the units that actually perform tasks.
- **RDD (Resilient Distributed Dataset):** Spark's most basic abstraction for distributed data.
  - *Resilient:* Spark keeps a list of dependencies recording how the RDD was built from its inputs. If the RDD is lost, Spark can rebuild it from those dependencies.
  - *Partitioned:* Spark automatically breaks the data into pieces (*partitions*).
  - *Distributed:* the partitions are spread across the cluster's nodes, so you can hold datasets too big for any single machine.
- **RDD history:** before Spark 2.0, the RDD was Spark's main programming interface. Spark 2.0 introduced Datasets and DataFrames, which are built on top of RDDs. The RDD interface is still supported.

A minimal session setup names the app and uses your own machine as the master:

```python
spark = SparkSession.builder.master("local").appName("pyspark_test").getOrCreate()
sc = spark.sparkContext
```

The first examples read Spark's README into an RDD with `sc.textFile("README.txt")`. The file has 41 lines. `lines.first()` returns `'Apache Spark'`, and `lines.filter(lambda x: "Spark" in x)` keeps only the lines that mention Spark. `collect()` returns an ordinary Python `list` on the driver.

### Word count, line by line

```python
words = lines.flatMap(lambda x: x.split())
wordcounts = words.map(lambda x: (x, 1)) \
                  .reduceByKey(lambda x, y: x + y) \
                  .map(lambda x: (x[1], x[0])) \
                  .sortByKey(False)
wordcounts.take(10)
```

- `flatMap(lambda x: x.split())` splits each line into words and flattens the results into one RDD of words (the mapping step's input).
- `map(lambda x: (x, 1))` turns each word into a `(word, 1)` pair: the MapReduce map step.
- `reduceByKey(lambda x, y: x + y)` sums the 1s for each word: the shuffle and reduce steps.
- `map(lambda x: (x[1], x[0]))` swaps each pair to `(count, word)` so the next step can sort by count.
- `sortByKey(False)` sorts by the key (now the count) in descending order.
- `take(10)` is an action. Nothing above it actually runs until an action is called. On the README, the top results are `(13, 'the')` and `(11, 'Spark')`.

**`map()` vs. `flatMap()`** is the key distinction here. Given `[3, 4, 5]` and the function `lambda x: [x, x*x]`:

- `map` returns a list of lists: `[[3, 9], [4, 16], [5, 25]]`
- `flatMap` returns one flat list: `[3, 9, 4, 16, 5, 25]`

**Exercise lesson:** to drop lines containing "Spark" before counting, the `filter` has to come *before* the `flatMap`. Once a line has been split into words, the line no longer exists as a unit you can test.

### Database transactions: ACID vs. BASE

The CAP theorem lecture starts with background on how databases handle changes. A **transaction** is a change of state performed against a database. Transactions protect against two problems: a system failure that stops some work from completing, and programs that interfere with each other when they access the database at the same time.

**ACID** is the traditional set of guarantees:

- **Atomicity:** every step in a transaction completes, or the database reverts to its original state. There are no partial completions.
- **Consistency:** data always meets the database's predefined integrity rules.
- **Isolation:** a new transaction waits until the previous one finishes before starting.
- **Durability:** the database keeps every committed record, even if the system fails.

**BASE** is the looser model used by many large distributed systems:

- **Basically available:** the focus is on keeping the data accessible to users at all times, concurrently.
- **Soft state:** data can pass through temporary states that change over time. For example, several applications may update the same record at once.
- **Eventually consistent:** a record becomes consistent once all the concurrent updates have finished.

|  | ACID databases | BASE databases |
| --- | --- | --- |
| Prioritize | Strict data consistency for critical transactions | Scalability and availability |
| Suited for | Critical transactions | Large-scale distributed systems |
| Examples | Most SQL databases: MySQL, PostgreSQL, Oracle, SQL Server | MongoDB, Cassandra, DynamoDB, Redis |

### Network partitions and the CAP theorem

Distributed systems connect multiple physical or virtual nodes over a network, and all cloud systems are distributed systems. No distributed system is safe from network failures. A **partition** is a break in communication between parts of the system. The lecture's example: four nodes (N1–N4) serve two services, the link between N3 and N4 fails, and a request that Service 2 sends to N4 now gets **stale data**, because N4 can no longer receive updates.

**The CAP theorem:** a distributed data store can guarantee only two of these three properties:

- **Consistency:** every read receives the most recent write, or an error. All users see the same data at the same time.
- **Availability:** every request receives a response, but there's no guarantee it contains the most recent write.
- **Partition tolerance:** the system keeps operating even when the network drops or delays messages between nodes.

**Why it's really a choice between C and A.** Partitions will happen, so when one does, the system has to decide:

- **Cancel the operation:** consistency is kept, availability drops.
- **Proceed with the operation:** availability is kept, but results may be inconsistent.

The "CA" option only works if the data isn't partitioned at all. The slides put it this way: *in practice, a CA distributed database cannot exist.*

**CP vs. AP databases:**

- **CP (consistency over availability):** the traditional ACID guarantees, with validity ensured by transactions. These are relational databases like Postgres and MySQL.
- **AP (availability over consistency):** the BASE philosophy, centered on eventual consistency. Common in NoSQL (non-relational) databases.

**(My addition)** The slides end with a discussion question: what does each choice require, and what's the downside? My answer: CP systems must be willing to refuse or delay requests during a partition, so users see errors or timeouts. AP systems always answer, but some answers are stale, and the application has to tolerate that and reconcile conflicting updates afterward. That's why AP fits things like social feeds and CP fits things like bank balances.

### Consistent hashing

**The problem.** As a business grows, data volumes grow, users increase, and workloads get more complex. Horizontal scaling means storing data across more and more servers, so you need a rule that decides which server holds each piece of data, and that rule has to keep working as servers are added or removed. The lecture calls this *elastic storage*.

**Hash function review.** A hash function maps data of any size to a fixed-size value. Here, the data being hashed is the **key**. With N servers (*buckets*), a good hash spreads keys evenly across them. The simplest method is **mod N hashing**:

`server_index = hash(key) mod N`

**Why mod N breaks.** Suppose you have 16 servers, the data grows 10x, and you add a 17th. The mod operation now gives a different server index for nearly every key, so almost all the data would have to move to different servers. Example: a key with hash value 20 lives on server `20 mod 16 = 4`, but after the change it belongs on server `20 mod 17 = 3`.

**The hash ring.** Consistent hashing is a kind of hashing designed so that much less data has to move. Picture the full range of hash values, from x₀ to xₙ₋₁, laid out on a line, then bend the line into a ring so the last value connects back to the first.

- Each **server** is hashed to a position on the ring.
- Each **key** is hashed to a position on the same ring.
- A key belongs to the server at its position, or else to the first server you reach moving **clockwise** from it.

**Adding a server.** In the lecture example, a fifth server is added. Only `key0`, which falls between the new server and its counterclockwise neighbor, moves (from server0 to server4). Every other key stays where it was. That's the big benefit.

**Server failure.** If one of four servers fails, its keys move to the next available server clockwise. The remaining keys don't move.

**Hot spots.** Data isn't always spread evenly. For example, when a server fails, all of its data lands on the next server. In the lecture's figure most keys end up on server 1, making it a **hot spot**.

**Virtual nodes (vnodes).** The fix is to put copies (*replicas*) of each server on the ring. Each vnode is hashed independently and lands at a random point on the ring, but all of a server's vnodes map back to the same physical machine. As the number of vnodes grows, keys spread more evenly. In practice, the number of vnodes is chosen to balance even distribution against the overhead of looking keys up.

**Summary:** consistent hashing makes horizontal scaling practical because only a small fraction of keys move when servers change, and vnodes counter hot spots.

**The uhashring lab.** The lab has you dig into [uhashring](https://github.com/ultrabug/uhashring), a Python implementation of a hash ring. The point is to see the ring concretely: it's a lookup that answers *"given this key, which server holds it?"*, and vnodes turn out to be nothing more than extra strings hashed onto the ring. What you found:

- Each physical node gets **160 vnodes** by default.
- Vnodes are named `{nodename}-{index}` (for example `node1-0` through `node1-159`). That string is what gets hashed to give each vnode its ring position.
- `hr.get_node(key)` returns which node owns a key, `hr.get(key)` returns the node's full metadata, and `hr.runtime._node_points[node]` lists every ring position for a node.
- The last task asks you to make the key `'coconut'` land on a different node. Adding nodes or changing their weights changes which vnodes sit near its hash on the ring, which can change the owner.

### Jupyter Notebooks and the Module 1 lab

The reading includes a primer on Jupyter Notebooks. Notebooks combine rich text and runnable code in one document, which makes them useful for instruction, demos, and debugging. The whole course runs on them.

The lab (your `JupyterTutorial_SHS` notebook) has two parts.

**Part 1, Python warmup:** list comprehensions (`[x for x in some_vals if x > 6]` → `[12, 34]`), selecting DataFrame columns whose names contain "domain", `.iloc[1]` to get a row by position (vs. `.loc` for labels), `apply` with a `lambda` to cube the age column (20 → 8000, 32 → 32768), and `";".join(...)` to produce `'the;quick;brown;fox'`.

**Part 2, log file analytics in Spark:**

- `logfile.txt` has **360 lines**.
- 4 lines contain `WARNING`, all of them `mailslot_create: setsockopt(MCAST_ADD) failed`.
- Your `count_logs()` function returned `(4, 145, 13, 1, 119)`: WARNING 4, INFO 145, EVENT 13, PROTERR 1, TRACE 119.

**(My addition)** `count_logs()` runs five separate `filter(...).count()` jobs, so Spark reads the file five times. That's fine for 360 lines. At scale you'd want one pass in the word count pattern: use `re` (which the lab imports) to pull out each line's log level, map it to `(level, 1)`, and `reduceByKey` once. Also, the function ignores its `line` parameter and uses the global `lines` instead.

## Module 2: RDDs & Running Spark on a Cluster

The learning outcomes: use RDDs and Pair RDDs for data analysis, understand the framework for running Spark on a cluster, and define data system reliability, scalability, and maintainability. This module's folder also holds the *Intro to Distributed Systems* lecture, which is covered at the end of this section.

### What an RDD is

An RDD is a distributed collection of elements. It's Spark's most basic abstraction and has existed since Spark began. All work with RDDs follows three stages: **create** an RDD, **transform** it, and run an **action** on it (for example, to compute a result).

**Two ways to create one:**

1. Load an external dataset: `lines = sc.textFile("README.txt")`
2. Distribute a collection of objects from the driver program: `nums = sc.parallelize([1, 2, 3, 4])`

**SparkSession vs. SparkContext.** `SparkSession` is the single entry point for working with Spark. It was introduced in Spark 2.0 to unify several earlier context managers. RDD work needs the SparkContext, which is an attribute of the session: `sc = spark.sparkContext`.

**When RDDs are useful:** unstructured data like documents, and certain models and applications that require them. For structured (tabular) data, DataFrames are more useful (Module 3). Under the hood, DataFrames are built from rows of RDDs.

### Transformations, actions, and lazy evaluation

- A **transformation** creates a new RDD from an existing one (for example `map`, `filter`, `flatMap`).
- An **action** returns a different data type, such as a number or a Python list. In the notebook, `sc.parallelize([1,2,3,4]).reduce(lambda x, y: x + y)` returns `10`, and its type is `int`, not RDD.

Spark is **lazy**: it does no actual work until it reaches an action like `count()`. Transformations only record what should happen. **(My addition)** Waiting lets Spark see the whole pipeline before running any of it, which is what lets it plan the work (the DAG, next).

**Debugging tip from the notebook:** call `count()` while testing to force Spark to evaluate. It shows you what breaks and how long each step takes.

### The DAG (directed acyclic graph) and lineage

Spark creates a logical plan, or roadmap, of the whole computation and optimizes it without any help from you. That plan is a **directed acyclic graph (DAG)**. *Directed* means the arrows go one way; *acyclic* means there are no loops.

The DAG defines the program's steps and is made of **RDD lineages**: the record of which RDDs were derived from which. Lineage is what makes RDDs *resilient*: if a job fails, Spark can re-create a lost RDD by replaying its lineage.

### Common transformations

- **`map()`** applies a function to each element, one output per input.
- **`flatMap()`** applies a function that returns a list for each element, then flattens everything into one list. Tokenizing sentences into words is the classic case.
- **`filter()`** returns a new RDD with only the records that meet a condition.
- **`parallelize()`** distributes local data to the workers, creating an RDD. (Strictly it's a SparkContext method, not a transformation, but the notebook lists it here.)

**Worked example: parsing pipe-delimited text.** Each line of `pipe_delim_data.txt` looks like `'10|105|-20|mmHg|4'`.

```python
pipe = sc.textFile("pipe_delim_data.txt")
pipe.map(lambda x: x.split('|'))                      # ['10', '105', '-20', 'mmHg', '4']
pipe.map(lambda x: x.split('|')).map(lambda x: (x[0], x[2]))   # ('10', '-20')
pipe.map(lambda x: x.split('|')).map(lambda x: (x[0], ','.join(x[2:])))  # ('10', '-20,mmHg,4')
```

The split turns each string into a list. A second `map` then keeps only the columns you want, as a tuple. Stringing operations together like this is called **chaining** or **pipelining**.

### Actions and other useful operations

- **`collect()`** retrieves the entire RDD to the driver. Be careful with large RDDs: the result has to fit in memory on a single machine.
- **`take(n)`** retrieves a small number of elements. The values may **not** be in order.
- **`first()`** retrieves the first element.
- **`reduce()`** combines elements two at a time into one new element of the same type. The exercise: the product of the odd numbers 1 through 15 is `2027025`.
- **`fold()`** works like `reduce()` but starts from a **zero value** that acts as the identity. **(My addition)** For example, 0 for a sum or 1 for a product.
- **`aggregate()`** works like reduce and fold but takes three things: an initial value, a function that combines values within each worker, and a function that merges results across workers. **(My addition)** It's useful when the result's type differs from the elements', such as building a (sum, count) pair to compute an average.
- **`countByValue()`** counts each distinct value: for `[1, 2, 3, 3, 4]`, `cv[1]` is 1 and `cv[3]` is 2.
- **`persist()` / `cache()`** store an RDD so it isn't recomputed. `cache()` is `persist()` with the default storage level. **(My addition)** Without caching, every action replays the RDD's lineage from scratch. Release it with `unpersist()` when you're done; `collect()` results are plain Python objects and need no cleanup.
- **`saveAsTextFile()`, `saveAsSequenceFile()`** save an RDD; which one you call depends on the storage format. `saveAsTextFile('repartitioned_data.txt')` creates a **directory** with that name, holding one `part-0000N` file per partition plus a `_SUCCESS` marker. Your repo shows this: 10 part files for 10 partitions, some of them empty. Running it again without deleting the old output fails with `FileAlreadyExistsException`.

### Narrow vs. wide transformations

Some transformations are much cheaper than others.

- **Narrow:** each partition can be processed independently and the results combined. `filter()` is narrow.
- **Wide:** the data has to be redistributed across the cluster. Computing a median is wide because it requires ordering *all* the data, not just each partition's piece. Moving data across the cluster this way is a **shuffle**, which is expensive.

The notebook's advice: keep transformations as simple as possible.

### Partitions

Partitions split data into pieces that can be computed in parallel, which can speed jobs up. Each partition lives on a single machine, and every node in a cluster holds one or more partitions. When running locally, Spark creates as many partitions as your system has CPU cores, unless you specified a different value when creating the SparkSession.

- `rdd.getNumPartitions()` tells you how many partitions an RDD has.
- `rdd.repartition(10)` changes it (to 10 here). Saving the result produces 10 output files.

**Heads up: why `local[1]` still gave 16 partitions.** In your run of the notebook, the cell builds a session with `.master("local[1]")`, yet `getNumPartitions()` prints 16. **(My addition)** My reading of this: `getOrCreate()` hands back the session that's already running instead of making a new one, so the `local[1]` setting never takes effect. The RDD was also created with the `sc` from the earlier session, whose default parallelism on your machine is 16. To actually change the master, call `spark.stop()` first and then build the new session. The same thing shows up in the cluster notebook (below).

### Set operations

```python
list1 = sc.parallelize(['cat', 'dog', 'baby'])
list2 = sc.parallelize(['giraffe', 'baby'])
list1.union(list2).collect()             # ['cat', 'dog', 'baby', 'giraffe', 'baby']
list1.union(list2).distinct().collect()  # ['cat', 'baby', 'giraffe', 'dog']
```

- **`union()`** combines two RDDs and does **not** remove duplicates.
- **`distinct()`** removes them, but it's expensive because it requires shuffling all the data over the network.
- **`intersection()`** keeps elements found in both: `['baby']`.
- **`subtract()`** keeps elements of the first RDD that aren't in the second: `['cat', 'dog']`.

### Pair RDDs (key/value pairs)

A Pair RDD holds key/value pairs, like a Python dictionary. The **key** is the field you plan to aggregate on. For example, to compute salary statistics by job title, the key is the title and the values are salaries. Pair RDDs are the tool for merging and aggregating data. You create one by applying `map()` so that it returns tuples:

```python
lines = sc.parallelize(['french fries', 'chicken burrito', 'Apache Spark', 'OpenAI ChatGPT'])
lines.map(lambda x: (x.split(" ")[0], x)).collect()
# [('french', 'french fries'), ('chicken', 'chicken burrito'), ...]
```

**`reduceByKey()`** runs a separate reduce for each key, in parallel. Spark combines values locally on each machine for each key *before* doing a global combine, which cuts down on expensive shuffling. **(My addition)** This is the same idea as the combiner in MapReduce.

```python
rdd = sc.parallelize([(1, 2), (3, 4), (3, 6), (-1, 10), (-1, 22)])
rdd.keys().collect()                          # [1, 3, 3, -1, -1]  -- keys() does NOT dedupe
rdd.reduceByKey(lambda x, y: x + y).collect() # [(1, 2), (3, 10), (-1, 32)]
rdd.reduceByKey(lambda x, y: x * y).collect() # [(1, 2), (3, 24), (-1, 220)]
```

**Word count revisited.** Now you can see the structure: `map()` creates the Pair RDD, and `reduceByKey()` is the reducer. The improved version first lowercases each line and replaces commas, periods, and hyphens with spaces, so `Spark` and `spark` count as one word. The top result becomes `(14, 'the')`, then `(13, 'spark')`.

**Bigrams.** A bigram is a pair of adjacent words, a common NLP task. The trick is to make the *pair of words* the key:

```python
bigrams = text.map(lambda x: x.split()) \
              .flatMap(lambda x: [((x[i], x[i+1]), 1) for i in range(0, len(x) - 1)]) \
              .reduceByKey(lambda x, y: x + y) \
              .map(lambda x: (x[1], x[0])) \
              .sortByKey(False)
```

The `flatMap` yields `(('A', 'frequent'), 1)`, `(('frequent', 'task'), 1)`, and so on. After that, it's the word count pattern exactly. In the example sentence every bigram appears once. The notebook's exercise is to add repeated bigrams and watch the counts change.

**Other Pair RDD operations covered:**

- **Partition parameter:** most Pair RDD operators take a number of partitions, which sets how much parallelism the operation gets: `reduceByKey(lambda x, y: x + y, 10)`.
- **Joins:** `join()` is an inner join. `leftOuterJoin()` keeps every record from the left RDD and matches from the right; `rightOuterJoin()` does the reverse.
- **Sorting:** `sortByKey(ascending=True, numPartitions=None, keyfunc=lambda x: str(x))`. The `keyfunc` lets you sort by a custom comparison; here, integers are sorted as strings.
- **Pair RDD actions:** every regular RDD operation still works, plus:
  - `countByKey()` counts per key: `{1: 1, 3: 2, 5: 3}` for `[(1,2),(3,4),(3,6),(5,1),(5,10),(5,100)]`
  - `lookup(3)` returns all values for key 3: `[4, 6]`
  - `collectAsMap()` returns the pairs as a dictionary

**Heads up:** the notebook's concept list also names `groupByKey()`, `combineByKey()`, `mapValues()`, `flatMapValues()`, `subtractByKey()`, `cogroup()`, and `groupWith()`, but none of them is demonstrated. Look these up if you expect to be tested on them.

### Spark's architecture on a cluster

A big benefit of Spark is that you can scale computation by adding machines and running in cluster mode. There are three roles:

- **Driver:** runs the program's `main()` method, converts the program into a logical DAG of operations and then into tasks, and schedules those tasks on the executors. It's the manager.
- **Executors** (on worker nodes): run the individual tasks. They launch when the application starts and run for its whole lifetime. They also provide in-memory (RAM) storage for RDDs, which is faster than disk.
- **Cluster manager:** the external service the application runs on. It manages resources across Spark applications and can queue work when demand exceeds the available executors. Spark ships with its own **Standalone** manager; others are **Hadoop YARN** and **Apache Mesos**.

Driver + workers = one **Spark application**.

**How a job actually runs** (from the notebook's step-by-step):

1. Spark breaks the dataset into **partitions**.
2. The driver builds a **DAG** of the entire computation.
3. Spark's query optimizer optimizes the DAG. The **DAG scheduler** then breaks the job into **stages** and **tasks**. A stage is a set of tasks that can run in parallel *without shuffling data*. **(My addition)** So stage boundaries fall wherever a shuffle happens.
4. The driver assigns tasks to executors on the worker nodes, preferring the node that already holds the partition a task needs, to minimize data movement.
5. Tasks run in parallel; each task processes one partition.
6. For actions that return results to the driver (like `collect()`), the results from the individual tasks are collected and aggregated.

**Job → stages → tasks.** The task is the smallest unit of work, and executors run tasks.

**The Spark Web UI.** Spark has a built-in web interface with tabs like *Jobs*, *Stages*, and *Executors* that show details about a running application, including the resources used at each stage. In local mode it's at `http://localhost:4040/jobs/`.

### Launching and configuring Spark

The course mostly runs code in notebooks. From the command line, `spark-submit` launches a Spark application:

```bash
spark-submit --master local python_scripts/textAnalysis1.py        # local, 1 core
spark-submit --master local[4] python_scripts/textAnalysis1.py     # local, 4 cores
spark-submit --master local[*] python_scripts/textAnalysis1.py     # local, all cores
spark-submit --master spark://host:7077 python_scripts/textAnalysis1.py   # Standalone cluster, default port
spark-submit --master spark://host:7077 --executor-memory 10g python_scripts/textAnalysis1.py
# generic form: spark-submit [options] <app jar | python file> [app options]
```

(The notebook uses Windows-style `bin\spark-submit` paths.)

**Parallelism** is the number of pieces of work done at the same time.

**The config comparison.** The notebook builds two sessions with the same settings (`spark.executor.cores = 4`, `spark.cores.max = 4`, 8g executor memory, 1g driver memory). The only difference is `.master("local")` in the first and `.master("local[*]")` in the second. The intended lesson: with plain `local`, the executor-core request doesn't take effect, because in local mode the master setting controls parallelism. You need `local[*]` (all cores) or `local[N]`.

**Heads up: your output doesn't actually show the difference.** Both configs printed `local[*]` and default parallelism 16. **(My addition)** Config 1 printed `WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect`, so, as in the partitions example, `getOrCreate()` reused a session that was already running, and `.master("local")` never applied. Also, `spark.conf.get("spark.executor.cores")` just echoes back the value you set, not what Spark is using. To see the real contrast, call `spark.stop()` before building Config 1.

**Packaging code.** PySpark runs Python on the worker machines, so you can manage packages with `pip`. You can also send libraries along with the `--py-files` argument to `spark-submit`.

### YARN, EC2, and EMR

- **YARN (Yet Another Resource Negotiator):** the cluster manager introduced in Hadoop 2.0. It allocates system resources to the applications running in a Hadoop cluster and schedules tasks on its nodes.
- **Amazon EC2 (Elastic Compute Cloud):** AWS's virtual servers. Spark includes a script, `spark-ec2`, that launches clusters on EC2. You need an AWS account and must export your access key ID and secret access key. By default the script launches one master and one worker.
- **AWS Free Tier:** AWS offers over 200 services; some are free under the Free Tier, which the course uses.
- **Amazon EMR (Elastic MapReduce):** a managed Hadoop framework for processing large amounts of data with parallel, distributed, elastic execution on AWS. EMR uses **S3** for storage.

### Reliability, scalability, and maintainability (reading)

**Heads up:** this reading (*Designing Data-Intensive Applications*, Ch. 1) isn't in the repo. `reading.md` only says you should be able to define **reliability, scalability, throughput, and latency**. The definitions below are my summary of the book's standard definitions, so check them against your copy.

- **Reliability:** the system keeps working correctly, doing the right thing at the expected performance, even when things go wrong. The book separates a **fault** (one component deviating from spec) from a **failure** (the system as a whole stops serving users). Reliable systems tolerate faults so they don't become failures. Faults come from hardware, software bugs, and human error.
- **Scalability:** the system's ability to cope with increased load. You describe load with *load parameters* (such as requests per second) and ask what happens to performance as they grow.
- **Throughput:** how much work gets done per unit of time, such as records processed per second. Batch systems usually care most about throughput.
- **Latency:** how long a single request takes. The book draws a finer line: *response time* is what the client sees in total, and *latency* is the time a request spends waiting to be handled. Online systems usually care most about response time.
- **Maintainability:** how easy the system is to operate, understand, and change over time. The book breaks it into *operability*, *simplicity*, and *evolvability*.

### Intro to Distributed Systems (lecture, from van Steen & Tanenbaum's *Distributed Systems*, 4th ed., Ch. 1)

**Brief computing history:**

- **1945:** the modern computer era begins.
- **1945–1985:** computers are large and expensive.
- **Mid-1980s:** two changes: powerful microprocessors, and high-speed computer networks. **Local-area networks (LANs)** connect thousands of machines in a building; **wide-area networks (WANs)** connect hundreds of millions of machines around the world, at speeds from tens of thousands to hundreds of millions of bits per second and more.
- **Now:** computers have shrunk (the smartphone is perhaps the most impressive result). A networked system can be anything from a handful of devices to millions of computers, and most can be reached from anywhere because they're on the Internet.

**Two definitions to get exactly right:**

- A **decentralized system** is a networked computer system in which processes and resources are **necessarily** spread across multiple computers.
- A **distributed system** is a networked computer system in which processes and resources are **sufficiently** spread across multiple computers.

**(My addition)** *Necessarily* means the spreading can't be avoided: the resources inherently live in different places. *Sufficiently* means the spreading is a design choice, made because spreading things out works better, for example for performance or reliability.

**Distributed vs. centralized.** A distributed system uses multiple computers on a network, and each node has its own memory. A centralized system is a client-server setup built around a single powerful server that less powerful nodes send requests to. It's easier to manage, but the entire system depends on that central server.

**Distributed vs. parallel.** Distributed computing uses multiple computers on a network, each with its own memory. Parallel computing usually uses one computer with multiple processors that share a **single memory unit**.

**Three examples of distributed systems:**

1. **Gmail:** close to 1.8 billion users as of 2025. To a user it seems to have just two servers, an inbox and an outbox. Behind the scenes, the whole service runs across many computers.
2. **Content Delivery Networks (CDNs):** a geographically distributed network of servers and their data centers. Content is copied across the CDN's servers, and when you visit a site you're redirected to a nearby server that holds it.
3. **Distributed databases, e.g. Amazon DynamoDB:** *scalable* (data and traffic are spread across many servers automatically to handle varying throughput and storage), *highly available and durable* (data is replicated across multiple Availability Zones within an AWS Region automatically), and *flexible* (a NoSQL model that supports both key-value and document data).

**Perspectives on distributed systems.** They're complex; the key concerns are:

- **Communication:** facilities for exchanging data
- **Coordination:** application-independent algorithms for getting parts to work together
- **Naming:** how you identify resources
- **Consistency and replication**
- **Fault tolerance:** keep running when parts of the system fail
- **Security:** make sure only authorized users reach resources

**Design goals:**

- **Support sharing of resources.**
- **Distribution transparency:** the lecture notes that the term is confusing. It means users don't realize the system is distributed, because the details are hidden. Formally: *the phenomenon by which a distributed system attempts to hide the fact that its processes and resources are physically distributed across multiple computers, possibly separated by large distances.* It's handled by many techniques in a **middleware** layer that sits between applications and operating systems.
- **Openness,** which includes extensibility.
- **Scalability.**

**Scalability, in three dimensions.** The lecture's observation is that developers call their systems "scalable" without making clear *why* they scale. There are at least three components:

- **Size scalability:** the number of users or processes.
- **Geographical scalability:** the maximum distance between nodes.
- **Administrative scalability:** the number of administrative domains (separately managed organizations or units) involved.

**Why centralized systems hit size limits.** Three root causes: computational capacity, limited by the CPUs; storage capacity, including the transfer rate between CPUs and disks; and the network between users and the central service.

**Techniques for scaling:**

- **Hide communication latency:** use asynchronous communication (send a request and keep working instead of waiting), with a separate handler for the response when it arrives. The problem is that not every application fits this model.
- **Move computation to the client:** the lecture's diagram has the client check a form as it's filled in instead of sending every field to the server to check. Java applets and scripts are the classic examples.
- **Partition data and computation across machines:** decentralized naming services (DNS) and decentralized information systems (the World Wide Web).
- **Replication and caching:** make copies of data available on different machines. Examples: replicated file servers and databases, mirrored websites, web caches in browsers and proxies (copies of web files stored on the user's device or intermediary servers), and file caching at servers and clients.

**The problem with replication.** Replication is easy except for one thing:

- Having multiple copies, cached or replicated, leads to **inconsistencies**: modifying one copy makes it different from the rest.
- Keeping every copy consistent, in a general way, requires **global synchronization** on every modification.
- Global synchronization rules out large-scale solutions.
- So if you can tolerate some inconsistency, you need less global synchronization. Whether you can is **application-dependent**. For some social media apps, inconsistency is fine: different users temporarily see different messages.

**(My addition)** This is the CAP tradeoff from Module 1 viewed from the scaling side.

**Pitfalls: the false (and often hidden) assumptions.** The lecture's observation: many distributed systems are needlessly complex because of mistakes that had to be patched later, and those mistakes come from false assumptions. **(My addition)** The one-line glosses are mine.

1. **The network is reliable.** Messages get lost, delayed, and duplicated.
2. **The network is secure.** Traffic can be read or tampered with.
3. **The network is homogeneous.** Real networks mix hardware, operating systems, and protocols.
4. **The topology does not change.** Nodes and links come and go.
5. **Latency is zero.** Every network call takes time, and far more than a local call.
6. **Bandwidth is infinite.** Moving data has a limit, which is why shuffles are expensive.
7. **Transport cost is zero.** Sending data costs time, CPU, and money.
8. **There is one administrator.** Parts of the system are managed by different people with different policies.

## Module 3: DataFrames & Spark SQL

The learning outcomes: apply DataFrames and Spark SQL to data analysis, decide when RDDs or DataFrames are the better choice, identify the benefits of columnar formats like Parquet, explain how Spark SQL uses the Catalyst Optimizer (concepts and stages), and understand how hot spots arise and how to fix them. The module intro stresses that data scientists spend most of their time understanding and preparing data, which is why this module matters. The folder also holds the *System Architecture* and *Unique ID Generator* lectures, covered near the end of this section.

### Spark SQL and DataFrames: the big picture

There are two main ways to work with structured big data in Spark: **Spark SQL** and **DataFrames**, and they work together. The reading notes that the DataFrame arrived in Spark 1.3 and made PySpark massively faster. It resembles a table in a relational database.

**SQL in ten seconds** (the notebook's joke title): SQL is the query language for relational databases. Its commands include CREATE, SELECT, UPDATE, ALTER, INSERT INTO, DROP, and DELETE. The course focuses on SELECT.

**What Spark SQL can do:**

- Load data from structured formats including JSON, Hive, and Parquet.
- Query data with SQL, either inside Spark or from outside tools that connect to Spark (like Tableau).
- Mix SQL with Python, Java, Scala, or R code; for example, you can join RDDs and SQL tables.

Spark SQL has been a heavy development area in each new release. With massive data, optimizing operations is valuable, which is where the Catalyst Optimizer (below) comes in.

### Dataset vs. DataFrame

- A **Dataset** is a distributed collection of data. It can be built from JVM objects and manipulated with functional transformations (`map()`, `flatMap()`, `filter()`, and so on).
- A **DataFrame** is a Dataset organized into **named columns**.

In practice, you'll think in DataFrames, not Datasets. They're similar to R and pandas data frames, but some operations are Spark-specific and more formal: adding a column uses `withColumn()`, for example. Compared with R and Python, Spark DataFrames also get much richer optimization under the hood. You can build one from structured data files, Hive tables, external databases, or existing RDDs. The API is available in Scala, Java, Python, and R.

### How DataFrames relate to RDDs

An RDD is just a distributed collection of objects split into partitions. A DataFrame adds three things on top: a **schema** (column names and types), a **query plan**, and the distributed data itself, still stored in partitions. The notebook's diagram shows the full stack:

```text
DataFrame API       df.select / filter / join / groupBy
      |
Spark SQL / Catalyst query optimizer
      |
Physical execution
      |
Distributed data: partitions P1, P2, P3  (RDD partitions)
      |
Executors E1, E2, E3
```

**(My addition)** The key idea: DataFrame code *describes* what you want. Catalyst decides *how* to compute it, and the work still runs as tasks on RDD partitions on the executors, exactly as in Module 2.

### RDDs or DataFrames?

Most work can be done with DataFrames. Use them for high-level expressions, SQL queries to explore data, and column access.

**Use a DataFrame when:**

- You think about the data by **field names**.
- You're doing machine learning or predictive modeling. You explore data column by column and build features from columns, and DataFrames are recommended for this.

**Use an RDD when:**

- You need low-level transformations and actions on **unstructured** data, such as filtering strings and other simple text transformations. Here you don't care about field names and there's no need to impose a schema.
- You want to manipulate data with functional programming constructs rather than domain-specific expressions.

### Schemas

In Spark, a **schema** defines the data's structure. For each field you give a 3-tuple: **(column name, data type, nullable)**. For example, two fields that can't be null:

```python
schema = StructType([StructField("author", StringType(), False),
                     StructField("pages", IntegerType(), False)])
```

(The notebook says you don't need to memorize the syntax.)

**Why give Spark the schema instead of letting it infer one:**

- It avoids Spark launching a separate job that reads a large fraction of the data just to guess the schema.
- You detect errors early if the data doesn't match the schema.
- Spark's inference can be wrong; it might decide all numeric data are strings.

**Not the same as a database schema.** A database schema is the logical view of an *entire database*: how data is organized and how the relations connect, implemented through tables, views, and integrity constraints. A Spark schema describes the columns of one DataFrame.

**Common Spark data types:** `ShortType`, `IntegerType`, `LongType`, `FloatType`, `DoubleType`, `StringType`, `BooleanType`.

**Heads up:** the notebook lists all five numeric types under "integer types, all `int` in Python." That's only true of `ShortType`, `IntegerType`, and `LongType`. `FloatType` and `DoubleType` are floating-point types and correspond to Python `float`.

### Creating DataFrames

Three ways:

1. **From an RDD with `toDF()`:** map each line to a `Row(...)` and call `.toDF()`.
2. **From data plus a schema with `createDataFrame()`:**

```python
data = [(0, "ChatGPT is all the rage"), (1, "Google released BARD to compete"),
        (2, "What does AWS think about this?")]
schema = StructType([StructField('id', IntegerType(), False),
                     StructField('sentence', StringType(), False)])
sentenceDataFrame = spark.createDataFrame(data, schema)
```

3. **From files, with a reader like `spark.read.json(...)` or `spark.read.csv(...)`.** This is the most common way. `spark.read.json("people.json")` gives three rows: Michael (age NULL), Andy (30), and Justin (19).

**Reading a plan: `df_json.explain("formatted")`.** For a plain file read, the physical plan is a single step, `Scan json`. Its details:

- `Output [2]: [age#15L, name#16]`: the scan outputs two columns.
- `#15` and `#16` are Spark's internal expression IDs for those columns. (The notebook's text says `#399/#400`, but your run shows `#15/#16`; the numbers change from session to session.)
- The `L` after the ID means the column is a `LongType` value. `ReadSchema: struct<age:bigint,name:string>` confirms Spark inferred `age` as a long (bigint).

The scan creates or reads data partitions, and Spark schedules one task per partition on the executors.

**Going back to an RDD** is simple: `df.rdd`. It returns an RDD of `Row` objects, like `Row(id=0, sentence='ChatGPT is all the rage')`.

### Selecting, filtering, and missing values

**Three ways to refer to a column** (the instructor's favorite is the dot operator):

```python
df.filter(df['age'] > 21).show()   # bracket operator
df.filter(df.age > 21).show()      # dot operator
df.filter(col('age') > 21).show()  # col(); needs: from pyspark.sql.functions import col
```

All three return Andy (30).

- **Combining conditions:** use `|` for or and `&` for and, with each condition in parentheses: `df.filter((df.name == "Andy") | (df.name == "Michael")).sort(asc("name"))`.
- **`where()` is identical to `filter()`.**
- **Nulls:** `col("age").isNull()` finds Michael; `col("age").isNotNull()` finds Andy and Justin.
- **Imputing:** `df.fillna(0)` replaces missing values with 0. The notebook does this only as an illustration and says it's not a great idea for this data (an age of 0 isn't real).
- **Summary stats:** `df.describe("age").show()` gives count 2 (the null is excluded), mean 24.5, stddev about 7.78, min 19, max 30.

### Spark SQL queries and temp views

To run SQL against a DataFrame, first register it as a **temp view**, then query the view by name:

```python
df.createOrReplaceTempView("people")
sqlDF = spark.sql("SELECT * FROM people WHERE name == 'Andy'")
sqlDF.show()
```

The result is itself a DataFrame. A temp view is **session-scoped**: only the session that created it can see it, and it's dropped when the session ends. It is never saved.

### Aggregation with `groupBy()`

SQL functions come from `pyspark.sql.functions` (conventionally `import ... as F`). The notebook reads a stock price file (`amzn_msft_prices.csv`: date, ticker, close, adjusted\_close, volume) with an explicit schema, checks it with `printSchema()`, and aggregates:

```python
agg_df = df_stx.groupBy("ticker").agg(F.min("close"), F.max("close"),
                                      F.min("volume"), F.max("volume"))
```

| ticker | min(close) | max(close) | min(volume) | max(volume) |
| --- | --- | --- | --- | --- |
| AMZN | 81.82 | 169.315 | 35,088,600 | 272,662,000 |
| MSFT | 214.25 | 315.41 | 9,200,800 | 86,102,000 |

This is **split-apply-combine**: split the rows by ticker, apply the functions to each group, and combine the results.

**Important:** do **not** use loops to aggregate. Loops run sequentially and throw away parallelization. `groupBy()` does the same job in parallel.

### Joins at scale

Joins are among the **most powerful** and **most expensive** operations in PySpark. To join, Spark must bring matching keys together, possibly reshuffle both datasets, handle memory pressure, avoid skew (see hotspots, below), and pick the best physical strategy.

**At scale, the cost driver is data movement: the shuffle.** A shuffle means data is (1) written to disk, (2) sent over the network, (3) read back, and (4) sorted. That slows jobs down and creates a network bottleneck.

There are three join strategies. The notebook says to consider them in this order:

**1. Broadcast Hash Join.** Works great when it's an option: when one side of the join is **small**.

- Spark **broadcasts** the whole small table to every executor. Broadcast data is read-only.
- Each partition of the large table then joins **locally**, so the large table is never shuffled.
- Use cases: small dimension tables, predictors kept in a separate table, lookup joins.

**2. Shuffle Hash Join (SHJ).**

- Shuffle **both** sides by the join key.
- In each partition, build a hash table from one side and **probe** it with the other side. No sorting.
- Works when no table is small enough to broadcast and the keys are evenly distributed.
- **Big weakness: it can blow up memory,** for example when one partition is too large for its hash table to fit.

**3. Sort Merge Join (SMJ): the default for large joins.**

- Spark's usual strategy for big joins. Stable and scalable, but expensive.
- Used when both datasets are large and broadcast isn't possible.
- Shuffle both sides, sort both sides, then merge them.

**Reading the broadcast join's plan.** The notebook joins the full stock table to the small aggregate table with a broadcast hint:

```python
from pyspark.sql.functions import broadcast
df_join_bhj = df_stx.join(broadcast(agg_df), "ticker")
df_join_bhj.explain("formatted")
```

```text
AdaptiveSparkPlan (11)
+- Project (10)
   +- BroadcastHashJoin Inner BuildRight (9)
      :- Filter (2)
      :  +- Scan csv (1)
      +- BroadcastExchange (8)
         +- HashAggregate (7)
            +- Exchange (6)
               +- HashAggregate (5)
                  +- Filter (4)
                     +- Scan csv (3)
```

Read it from the bottom up. The **left** branch, steps (1)–(2), scans the full stock CSV. The **right** branch, steps (3)–(8), builds `agg_df`: scan, filter, aggregate, then **`Exchange`**, which means **shuffle**. That shuffle happens on the right table before the broadcast. `BuildRight` means Spark builds the hash table from the right-hand side and broadcasts it (8). Each executor then processes its partitions of the left table, looking up matching keys in the broadcast hash table (9).

**(My addition)** Two details the notebook doesn't spell out:

- The two `HashAggregate` steps around the `Exchange` are a partial aggregate on each partition (5) followed by a final aggregate after the shuffle (7), the same combiner idea from Module 1.
- The `Filter` steps are most likely null checks on `ticker` that Spark adds on its own, since null keys can't match in an inner join. `AdaptiveSparkPlan` means Adaptive Query Execution is on, so Spark can adjust the plan at runtime.

### Reading and writing data; Parquet

Reading and writing examples (the notebook presents these as illustration only):

```python
# read with schema inference
adult_df = spark.read.format("com.spark.csv").option("header", "false") \
    .option("inferSchema", "true").load("dbfs:/databricks-datasets/adult/adult.data")
# generic load/save (Parquet is the default format)
df = spark.read.load("examples/src/main/resources/users.parquet")
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
# format given explicitly
df = spark.read.load("examples/src/main/resources/people.json", format="json")
df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
```

**Parquet** is a **columnar** file format that many data processing systems support.

- **(My addition)** *Columnar* means each column's values are stored together, rather than each row's. Reading 3 of 50 columns only touches those 3, and similar values sit next to each other, so they compress well.
- That makes it especially useful for analytics and ML, which usually need certain columns rather than whole rows.
- It stores **metadata about the columns**, which enables efficiencies (see the demo below).
- Files hold binary data.
- Reading and writing Parquet can be **much** faster in Spark.
- It has good compression and encoding options: a hybrid of **bit packing** and **run-length encoding**, switching to whichever compresses better.
  - **Bit packing:** an integer normally gets 32 or 64 bits of storage. Small integers don't need that much, so several are packed into the same space.
  - **Run-length encoding (RLE):** a run of duplicate values is stored as the single value plus how many times it occurs.

**Partition discovery.** Tables can be partitioned to make queries faster. For example, split the data by gender and country: an analyst who only cares about one country reads only that country's smaller table. In a partitioned table, data is stored in separate directories, with the partition column values encoded in the path:

```text
path/to/table/gender=male/country=US/data.parquet
path/to/table/gender=male/country=CN/data.parquet
path/to/table/gender=female/country=US/data.parquet
...
```

All of Spark's built-in file sources (Text, CSV, JSON, ORC, Parquet) discover and infer this partitioning automatically. To write data this way:

```python
df = df.withColumn('end_month', F.month('end_date'))
df = df.withColumn('end_year', F.year('end_date'))
df.write.partitionBy("end_year", "end_month").parquet("/tmp/sample_table")
```

The notebook's exercises: select AMZN records with Spark SQL; compute min, mean, and max `adjusted_close` per ticker; save three columns as Parquet; and read the file back to check it.

### The RLE and Parquet demo

This notebook shows RLE directly, then shows how Parquet uses it.

**RLE implementation:**

```python
def rle_encode(values):
    """Return [(value, run_length), ...]."""
    if not values:
        return []
    encoded = []
    current = values[0]
    count = 1
    for value in values[1:]:
        if value == current:          # same as the previous value: extend the run
            count += 1
        else:                         # new value: close the run, start another
            encoded.append((current, count))
            current = value
            count = 1
    encoded.append((current, count))  # close the final run
    return encoded

def rle_decode(encoded):
    """Reconstruct the original sequence."""
    values = []
    for value, count in encoded:
        values.extend([value] * count)
    return values
```

**Three test datasets, each 3,000 values:**

| Dataset | Pattern | RLE runs | Compression ratio |
| --- | --- | --- | --- |
| `repetitive` | 1,000 "US", then 1,000 "Spain", then 1,000 "France" | 3 | 1000:1 |
| `moderate` | "US", "US", "Spain", "Spain", "France", "France", repeated 500 times | 1,500 | 2:1 |
| `no_runs` | "US", "Canada", "UK", "France", cycling | 3,000 | 1:1 |

**Lesson 1: compression depends on adjacent repeats, not on how many unique values there are.** `no_runs` doesn't compress at all, even though it has only four unique values. RLE only cares whether equal values sit next to each other.

**Writing Parquet.** Each dataset is written with PyArrow using `row_group_size=1000` and `use_dictionary=True`, giving 3 **row groups** of 1,000 rows each. A row group is a horizontal chunk of the file. For each column in each row group, Parquet stores **statistics**: min, max, and null count. File sizes came out at 1,922, 2,027, and 2,030 bytes. **(My addition)** They're nearly identical because files this small are mostly fixed overhead.

**Encodings.** All three files show `('PLAIN', 'RLE', 'RLE_DICTIONARY')`. `RLE_DICTIONARY` first builds a dictionary that maps each string to an ID ("US" → 0, "Spain" → 1, "France" → 2), then run-length encodes the IDs. The *choice* of encoding is the same for every file; how *well* it works differs.

**Lesson 2 (the key insight from your write-up): compression and pruning are separate properties.**

- In `repetitive`, every row group has min == max (row group 0 is all "US", 1 is all "Spain", 2 is all "France"). A query that filters on the column can check the statistics and **skip** 2 of the 3 row groups without reading them. This is **min/max pruning**.
- In `moderate` and `no_runs`, every row group spans the full range of values, so the statistics never let a filter skip anything. That holds for `moderate` even though it still compresses well.
- **(My addition)** In practice, sorting or clustering data on a column you often filter by, before writing, is what makes its row-group statistics useful for pruning.

**The companion questions (your answers):**

- *Can you change which statistics Parquet stores (e.g., standard deviation)?* No. The format fixes the statistics per column chunk to min/max, null count, and distinct count, and query engines only read those for pruning. You could store other numbers as generic key-value metadata or in an external catalog, but no engine will use them to optimize.
- *Can you set where row groups start and end?* Not by editing an existing file; you'd rewrite it. At write time, PyArrow's `ParquetWriter.write_table()` makes each call its own row group, so you choose the rows. In Spark, `parquet.block.size` sets row-group size in bytes, which is less precise.
- *Can you inspect row-group statistics?* Yes, with PyArrow:

```python
import pyarrow.parquet as pq
pf = pq.ParquetFile("file.parquet")
col = pf.metadata.row_group(0).column(0)
col.statistics.min, col.statistics.max, col.statistics.null_count
```

### The Catalyst Optimizer

Spark SQL is one of Spark's most advanced modules and makes up most of its codebase. The **Catalyst optimizer** is at its heart, and it uses advanced features of the Scala language. Catalyst works on DataFrames too: Spark doesn't execute each DataFrame operation immediately. It starts with a **logical plan**, which Catalyst optimizes.

**Trees are the main data type.** Catalyst uses a general library for representing trees and applying rules to them.

- Trees are made of node objects. Each node has a type and zero or more children.
- Nodes are **immutable**: they're changed with functional transformations that produce new trees.
- Example: the expression `x + (1 + 2)` is built from three node classes: `Literal` (a constant value), `Attribute` (a column such as `x`), and `Add` (combines two expressions). The tree is `Add(Attribute(x), Add(Literal(1), Literal(2)))`.

**Rules** manipulate trees. A rule is a function that maps one tree to another. One transform call can match several patterns. A rule may need to run several times to fully transform a tree, so Catalyst groups rules into **batches** and runs each batch until the tree stops changing (a **fixed point**).

**Logical vs. physical plans.** A **logical plan** describes the computation on the datasets without saying how to carry it out. A **physical plan** defines how the computation will actually run.

**The four phases:**

1. **Analysis of the logical plan.** The plan starts as a relation from the SQL parser or the DataFrame API, with names that aren't resolved yet. In the lecture's example query, Catalyst has to answer: where is the table `prices`? Where is the column `close`, and what's its data type? Until it checks, `close` is an *unresolved attribute*. Catalyst resolves it by looking up the table in the **Catalog**.
2. **Logical optimization.** Rule-based rewrites of the logical plan:
   - **Constant folding:** evaluate constant expressions once at compile time instead of on every row at runtime. `seconds_in_day = 60 * 60 * 24` becomes `86400`; `x = (2 + 3) * y` becomes `x = 5 * y`.
   - **Predicate pushdown:** a *predicate* is a condition that returns true or false, usually in a WHERE clause. Pushing it down means filtering as early as possible, so fewer rows are retrieved. In the lecture's example, a query has two WHERE conditions, and the optimized plan applies them *before* the JOIN instead of after. By default, Spark automatically pushes valid WHERE clauses down to the database.
   - **Boolean expression simplification.**
3. **Physical planning.** Spark turns the optimized logical plan into physical operators that match the execution engine. It may generate several candidate plans, estimate each one's cost with a **cost model**, and pick the cheapest. Decisions made here include the join type and the order of filters. For aggregation, it chooses between **hash aggregation** and **sort-based aggregation**. Hash aggregation builds a hash map in memory and adds up each group's values as rows stream through, which works better when there are few groups. The lecture cites *A Cost Model for Spark SQL* (Golfarelli & Baldacci): it accounts for network, CPU, and I/O costs, and its runtime estimates averaged 14–20% error, which is accurate enough to be useful.
4. **Code generation.** Parts of the query are compiled to **Java bytecode**. Without code generation, an expression like `x + (1 + 2)` would be *interpreted* again for every row of data; with it, the expression becomes code that's compiled once. The strategy:
   1. Convert the SQL into an **abstract syntax tree (AST)**, a tree representation of code that is fundamental to compilers. It's "abstract" because surface details like spacing don't matter.
   2. Have Scala evaluate the AST.
   3. Compile and run the generated code.

   The tool that makes this work is Scala **quasiquotes**, strings prefixed with `q` that let a program build ASTs, which can be fed straight to the Scala compiler.

**Background the lecture gives for code generation:**

- **Bytecode** sits between human-readable source code and machine code, and it's platform-independent (runs on any OS).
- The **Java Virtual Machine (JVM)** takes bytecode and compiles it to machine code, which then runs on the CPU. (Machine code for a GPU needs a specialized compiler.) The JVM can run any language that compiles to bytecode, including Java, Scala, Kotlin, and Groovy.
- **Why Spark is written in Scala:** when Spark was built, its competitor was Hadoop (for running MapReduce), which was written in Java and runs on the JVM. Java can be very verbose. The developers chose Scala because many engineers could work in it, it takes less code, and it still compiles to bytecode for the JVM.
- **Compile time vs. runtime:** compile time is when a compiler turns source code into machine code (such as `1110101111111110`). Compilers are optimized for the hardware, and work done at this stage is often much faster than at runtime. Runtime is when the program actually runs, along with the external instructions it needs; it comes after compile time, and running code at this stage is slower. That's why constant folding and code generation pay off: they move work to compile time.

**The evaluation (from the Spark SQL paper, Armbrust et al.):**

- **Setup:** 6 EC2 i2.xlarge machines (one master, five workers), each with 4 cores, 30 GB memory, and 800 GB SSD, running HDFS 2.4 and Spark 1.3 (a very early version). The data was 110 GB after compression in columnar Parquet format.
- **Task:** 1 billion integer pairs `(a, b)` with 100,000 distinct values of `a`; compute the average of `b` for each `a`.
- **Results:**
  - **Python API:** the RDD version, which Catalyst doesn't optimize.
  - **Scala API:** also RDDs, and faster because Scala runs in the JVM. But the programmer supplies arbitrary Scala functions, and Spark has limited ability to understand and optimize what they do.
  - **DataFrame:** you tell Spark *what* you want, not *how* to do it, so Catalyst can fully optimize it. The DataFrame code is more concise **and** much faster, because the physical execution is compiled to JVM bytecode.

**Seeing the four plans yourself** (the activity question, with your answer):

```python
df_join_bhj.explain(mode="extended")
```

This prints all four stages:

- **Parsed Logical Plan:** the raw tree from parsing, with unresolved references and no type checking yet.
- **Analyzed Logical Plan:** everything resolved against the catalog. Types are concrete and the broadcast hint is attached, but nothing has been rewritten for performance.
- **Optimized Logical Plan:** rule-based rewrites applied (predicate pushdown, column pruning, constant folding). Still engine-agnostic.
- **Physical Plan:** the concrete execution strategy. This is where `BroadcastHashJoin`, `BroadcastExchange`, and shuffle (`Exchange`) nodes appear.

The throughline: the first two plans are about **correctness** (what does this query mean?), and the last two are about **execution strategy** (how do we run it fast?).

The other `explain()` modes: `"simple"` (the default) shows only the physical plan; `"formatted"` shows the physical plan as an outline plus a section of node details; `"cost"` shows the optimized logical plan with statistics.

### Data hotspots and salting

A **data hotspot** happens when one or a few keys have much more data than the others. One task ends up doing most of the work while the others sit idle. It's a data **imbalance** problem. **(My addition)** It's the same problem as hot spots on a consistent-hashing ring in Module 1, showing up at a different layer.

**Example 1.** The key `user1` has 10 click events, and `user2`, `user3`, and `user4` have 1 each. During `groupBy("user_id")`, `user1` becomes a hot partition: Spark assigns a single task to handle all of `user1`'s data, which slows the whole job.

**The fix: salting.** Add a small random "salt" (0–9) to each record's key so that the hot key's data spreads across multiple reducers. Aggregate on the salted key, then aggregate back to the original key:

```python
from pyspark.sql.functions import col, lit, concat, rand, expr, count

# 1. salt: user1 becomes user1_3, user1_5, user1_9, ...
salted = df.withColumn("salted_key",
                       concat(col("user_id"), lit("_"), (rand() * 10).cast("int")))

# 2. partial counts per salted key (the hot key's work is now split up)
salted_counts = salted.groupBy("salted_key").agg(count("*").alias("partial_count"))

# 3. strip the salt and sum the partial counts back to the original key
final_counts = salted_counts.groupBy(expr("split(salted_key, '_')[0]").alias("user_id")) \
                            .agg(expr("sum(partial_count)").alias("total_events"))
```

The final counts match the unsalted result: `user1` still totals 10.

**Example 2, with timing.** 999,999 rows, of which 990,000 (99%) belong to `hot_user`. The unsalted `groupBy` took 0.54 s; the salted version took 0.41 s, **23.1% faster**. Note the `count()` calls, which force Spark to actually compute so the timing is real.

**(My addition)** Treat those numbers as illustrative: it's a single run on one local machine, so timings that small are noisy. The mechanism is what matters.

### Configuring a SparkSession and sizing executors

The `SparkSession` is a unified conduit to all of Spark's operations and data; the notebook calls it an example of a *context manager*. A session with typical configs:

```python
spark = SparkSession.builder \
    .master("local[*]") \                        # use all cores on the local machine
    .appName("Python Spark SQL basic example") \ # name shown in the cluster manager
    .config("spark.executor.memory", '20g') \    # RAM per executor
    .config('spark.executor.cores', '5') \       # cores for EACH executor
    .config('spark.executor.instances', '17') \  # total number of executors
    .config("spark.driver.memory", '1g') \       # driver RAM; usually needs less than a worker
    .getOrCreate()
```

(The inline comments are only for reading. Python won't accept a comment after a line-continuation backslash.)

Spark sets these configs by default, but the defaults aren't always optimal, and it's best to put the settings in a function.

**Worked example: 6 nodes, each with 16 cores and 64 GB RAM.** First, the overheads:

- **O1:** on each node, 1 core and 1 GB of RAM go to the OS and Hadoop daemons, leaving **15 cores** and **63 GB** per node.
- **O2:** the resource manager (e.g., YARN) needs about 1 GB of RAM per node.
- **O3:** one executor is needed for the driver.

Then the calculation:

1. **Cores per executor:** more cores means more concurrent processing, but an application running more than 5 concurrent tasks generally doesn't perform well. Cap it: **`spark.executor.cores = 5`**.
2. **Executors per node:** 15 available cores ÷ 5 cores per executor = **3 executors per node**.
3. **Total executors:** 6 nodes × 3 = 18, minus 1 for the driver (O3) = **`spark.executor.instances = 17`**.
4. **Memory per executor:** 63 GB ÷ 3 executors = 21 GB, minus about 1 GB for the resource manager's overhead (O2) = **`spark.executor.memory = 20g`**.

By default, `spark.executor.cores` uses all cores, which is simpler but not always optimal.

### System architecture examples (lecture, from *Distributed Systems*, 4th ed., Ch. 2)

The lecture covers three powerful and common distributed system architectures.

**1. Three-tier architecture.** The diagram uses a housing search engine: a **user-interface level** (the page where you enter a search), a **processing level** (the application logic that builds the query and ranks results), and a **data level** (the database of listings). It's the same pattern as the three-tier diagram in Module 1.

**2. Client-server architecture.** A client sends a request for a service; a server processes it and replies.

**Background: TCP/IP, a connection-oriented protocol.** TCP/IP (Transmission Control Protocol / Internet Protocol) is the standard "language" of the internet and most modern networks. It defines how devices connect and send and receive data reliably, and nearly every internet application protocol runs on TCP/IP connections.

A **frame** is the unit of data used to move information across a local network. It usually contains:

- a **source MAC address** (who sent the data)
- a **destination MAC address** (who should receive it on the local network)
- a **payload**, usually an IP packet
- **error-checking** information

What each layer of the stack does:

| Layer | Role |
| --- | --- |
| Application | Network services to applications (HTTP, DNS, email) |
| Transport | End-to-end communication between applications: ports, reliability, ordering, flow control (TCP/UDP) |
| Internet | IP addressing and routing packets between networks |
| Network Access | Moving frames across the local network or link (Ethernet, Wi-Fi) |

Benefits of TCP/IP:

- **Reliability:** TCP breaks data into packets, numbers them, and makes sure they all arrive in order. Lost or corrupted packets are retransmitted automatically.
- **Interoperability:** every major OS, router, and application supports it.
- **Scalability:** it scales to billions of devices.
- **Flexibility:** it supports many application protocols (HTTP(S), FTP, SSH, email, streaming).

**How a connection works:** before the client requests a service, it sets up a connection with the server. The server sends its reply over the same connection. When communication is finished, the connection is torn down.

**Without a connection:** (1) the client packages a message naming the service it wants plus the input data; (2) it sends the message to the server; (3) the server waits for requests, processes each one, and sends the results back in a reply message. When this works, it's efficient. But if no reply comes back, the client can't tell which of two things happened:

1. The original request was lost, so the client should resend it.
2. The request arrived but the response was lost, so resending might repeat the work.

A transaction that can safely be repeated is **idempotent**. Asking for a bank account balance is idempotent: it's safe to resend. Transferring $10K out of an account is not.

**3. Cloud computing.** The cloud model can be viewed as four layers, from the bottom up:

1. **Hardware:** processors, routers, power, and cooling systems. Customers normally never see these.
2. **Infrastructure:** virtualization. This layer allocates and manages virtual storage devices and virtual servers (e.g., Amazon EC2).
3. **Platform:** higher-level abstractions for storage and similar services. Example: Amazon S3 offers an API for organizing and storing (locally created) files in so-called **buckets**.
4. **Application:** the actual applications, such as office suites (text processors, spreadsheets) and GenAI services. The lecture compares them to the suite of apps that ships with an operating system.

**(My addition)** These layers line up with the usual cloud service labels: infrastructure is **IaaS**, platform is **PaaS**, and application is **SaaS**.

### Designing a unique ID generator (lecture)

**The problem.** A database table is partitioned and placed on multiple servers (a distributed system), and you need to generate and store unique IDs, such as session IDs or user IDs.

**Why auto-increment fails.** Databases have an auto-increment function that generates sequential numbers. With the data distributed, each server counts on its own, so the same numbers get issued on different servers and you end up with duplicates.

**UUIDs: one potential solution.** A **universally unique identifier (UUID)** is a 128-bit number used to identify information in computer systems.

- It's **very unlikely** to create duplicates.
- UUIDs can be generated **independently, with no coordination** between servers. In the lecture's diagram, every web server has its own ID generator.
- Format: hexadecimal (characters 0–9 and a–f), with each character representing 4 bits, so a UUID takes 32 characters.
- **Anatomy:** a UUID is made of fields. The lecture's diagram names `time_low`, which holds the first 32 bits of a timestamp, for example. The other fields are `time_mid`, `time_hi_and_version`, `clock_seq_hi_and_reserved`, and `node`.

**SQL example** (from the lecture): UUID is a subtype of the string type, and here it's part of the primary key.

```sql
CREATE TABLE users (
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  city STRING NOT NULL,
  name STRING NULL,
  address STRING NULL,
  credit_card STRING NULL,
  CONSTRAINT "primary" PRIMARY KEY (city ASC, id ASC),
  FAMILY "primary" (id, city, name, address, credit_card)
);
```

**Heads up:** the slide labels this a PostgreSQL example, but `STRING` columns and the `FAMILY` clause aren't PostgreSQL syntax; they look like CockroachDB, a distributed database that uses a PostgreSQL-compatible dialect. `gen_random_uuid()` exists in both. I'm fairly confident of this, but check before relying on it in writing.

### Module 3 lab: commercial data analysis

The lab uses a large JSON dataset of business listings (on Rivanna, in `/standard/ds7200-apt4c/large_datasets/`). Each record is one business location, with nested fields such as `address`, `hours`, `menu`, `reviews`, `urls`, and `webpage`. `spark.read.json` reads the gzipped file directly. It's 15 one-point questions. Most of the difficulty is understanding the data rather than Spark syntax.

**Techniques and findings:**

- **Navigating nesting:** reach into structs with dot notation (`df.select('address.street_number')`), and use `printSchema(n)` to show the tree to *n* levels deep. `col()` is needed to call methods like `.isNotNull()` on a nested field.
- **Q3, street addresses:** `address.street_address` and `address.full_address` are mostly null (and one non-null `street_address` is "Cooper Contracting", a business name). You rebuilt usable addresses from `street_number`, `street`, and `street_type`, filtering on `col('address.street').isNotNull()`.
- **Q4:** 762 records have city Phoenix.
- **Q5, closing at 8pm Thursday:** checking `length(col('hours.thursday_close'))` showed both 4- and 6-character values, meaning times are stored as both HHMM and HHMMSS. Matching both with `.isin(['2000', '200000'])` gives **3,313** records.
- **Q6:** Phoenix *and* 8pm Thursday close: **12**.
- **Q7, price range ≥ 2:** `menu.price_range` is a string holding a digit (1–4), not dollar signs. Casting with `col('menu.price_range').cast('int') >= 2` gives **1,135**, and the nulls cause no problem.
- **Q8, headquarters:** `groupBy('address.is_headquarters').count()` gives NULL 87,625, true 318, false 66,736. That's one pass instead of three separate counts, and the three groups add up to 154,679 records in total.
- **Q9, Spark SQL:** `createOrReplaceTempView('businesses')` (the older `registerTempTable` is deprecated), then `SELECT webpage.title FROM businesses WHERE webpage.url = 'Target.com'` returns "Target : Expect More. Pay Less."
- **Q10, rating arrays:** `reviews` is an array of structs, so `reviews.stars` pulls the stars out of every element and returns an **array per row**. Grouping on it, the most common arrays are NULL (74,679), `[]` (42,419), `[5]` (4,258), `[NULL]` (3,067), and `[5, 5]` (1,610).
- **Q11, average rating per business:**
  - Select `id` and `reviews.stars`.
  - `withColumn('stars', explode('stars'))` gives one row per rating: **600,082** rows.
  - `dropna(subset=['stars'])` leaves **538,241** non-null ratings.
  - `groupBy('id').agg(avg('stars')).sort('id')` puts `000136e65d50c3b7` (average 4.0) at the top, as the hint said it would.

**(My addition)** One subtlety in your output: after `explode`, businesses whose `stars` was NULL or `[]` disappear entirely. `0000821a1394916e` is in the table before the explode and gone after. `explode()` drops rows whose array is null or empty. That was fine for this question, but if you ever need to keep those businesses (say, to report them as "no ratings"), `explode_outer()` keeps them with a null value instead.
