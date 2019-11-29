# `eat`: A hungry [tac](https://linux.die.net/man/1/tac)

Reads a file backwards line-by-line and continually truncates it.
Does a million lines/s or 200 MB/s on my laptop.

**Why?** It's a poor man's job queue!

```
~ $ wc -l lines                            
 72236934 lines

~ $ eat -n2236934 lines | wc -l
  2236934

~ $ wc -l lines                                       
 70000000 lines
```

Appending to the file while running `eat` is undefined behavior!
