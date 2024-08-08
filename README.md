## Page-wise Oblivious K-V Database in Rust

This library implements a basic key-value database where both keys and values are byte vectors. For each operation, the page access pattern is oblivious, meaning that an adversary cannot discern which key is accessed based on which disk page is accessed. The library is intended to run in a trusted execution environment (TEE). Compared to other oblivious map implementations, this library offers the following features:

1. **Low Latency**: The Rust library is suitable for latency-sensitive applications, such as private block builders. *(Benchmark details to be added.)*
2. **Flexible Key and Value Sizes**: The library does not require padding entries to a fixed size. Instead, it accepts keys and values of varying sizes and can dynamically tune itself for optimal performance.
3. **Auto-scaling**: There's no need to predefine a maximum database size before execution. The database automatically scales when full, and this scaling operation is fully de-amortized, ensuring no operation is blocked due to scaling.
4. **Enclave-friendly**: A cache size can be configured based on the secure enclave memory space. Most data can be stored encrypted in external memory (e.g., SSD or HDD), with the library minimizing page swaps with insecure memory.

### High-Level Overview of the Architecture

1. **`db.rs`**: Database interface.
2. **`flexomap.rs`**: Oblivious Key-Value Map implementation using a cuckoo hash map to maintain the position of database entries.
3. **`flexoram.rs`**: Non-recursive ORAM implementation for entries of varied sizes, storing actual database entries using a path ORAM eviction strategy.
4. **`cuckoo.rs`**: Cuckoo hash map implementation that stores entries in two hash tables.
5. **`recoram.rs`**: Recursive ORAM implementation for fixed-size entries, used in the cuckoo hash map when the hash table cannot fit within the cache.
6. **`fixedoram.rs`**: Non-recursive ORAM for fixed-size entries, also using a path ORAM eviction strategy. It is more efficient than flexoram due to the absence of fragmentation issues.
7. **`dynamictree.rs`**: A multi-way ORAM tree implementation that scales dynamically. Each node in the tree is a page, and the tree's fan-out adjusts based on the number of entries each page can hold.
8. **`segvec.rs`**: Implements a vector to store a level of the dynamic tree. When doubling the vector size, a new segment is allocated for the second half, avoiding the need to copy original data. Each new entry is initialized lazily on the next write operation for de-amortization.
9. **`encvec.rs`**: Handles the encryption and decryption of each segment in the `segvec`.
10. **`params.rs`**: Global parameters.