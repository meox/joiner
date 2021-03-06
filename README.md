# Joiner

## Prepare the file

Generate 300 files of 100MB each (random)

```shell
time ./joiner --generate 300 --prefix /stank/test_dat/foo --size 104857600
```

## Serial Join

```shell
time ./joiner --join 300 --prefix /stank/test_dat/foo --out /tank/foo_all_ser.dat --preallocate

SERIAL, file_size=/tank/foo_all_ser.dat, total_size=31457280000

*Results*: 0.27s user 12.25s system 12% cpu 1:39.91 total
```

## Parallel Join

```shell
time ./joiner --join 300 --prefix /stank/test_dat/foo --out /tank/foo_all_par.dat --parallel --max-threads 8

PARALLEL file=/tank/foo_all_par.dat, file_size=31457280000
spawned 32 threads
```

### Results

- With 4 threads: 0.19s user 13.13s system 12% cpu 1:47.71 total
- With 8 threads: 0.16s user 26.69s system 26% cpu 1:39.99 total
- With 16 threads: 0.11s user 31.95s system 31% cpu 1:41.82 total
- With 32 threads: 0.13s user 26.52s system 26% cpu 1:41.62 total

## Verify

9f2a1d6eaa68f63ffd3941c741472cd5  foo_all_par.dat
9f2a1d6eaa68f63ffd3941c741472cd5  foo_all_ser.dat
