# simple-kv
A toy KV store with pmdk

## interface

```c
Value get(key)
vector<value> history(key)
put(key, value)
remove(key)
```
