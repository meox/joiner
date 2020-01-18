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

>>> 0.36s user 15.35s system 12% cpu 2:02.46 total
```

## Parallel Join

```shell
time ./joiner --join 300 --prefix /stank/test_dat/foo --out /tank/foo_all_par.dat --parallel --max-thread 32

PARALLEL file=/tank/foo_all_par.dat, file_size=31457280000
spawned 32 threads
```

- With 4 threads: 0.19s user 13.25s system 8% cpu 2:33.96 total
- With 8 threads: 0.20s user 13.13s system 9% cpu 2:26.23 total
- With 32 threads: 0.30s user 13.62s system 3% cpu 6:54.18 total

## Verify

9f2a1d6eaa68f63ffd3941c741472cd5  foo_all_par.dat
9f2a1d6eaa68f63ffd3941c741472cd5  foo_all_ser.dat
