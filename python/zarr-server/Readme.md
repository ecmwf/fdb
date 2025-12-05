Setup
=====

Call `start_caddy.sh` with the `-c` flag and supply an existing folder for the creation of the
server data folder (where the Caddyfile and the schema, etc. are stored).

This also creates an `db_store` folder in the `server_data` folder.

Use the following MARS requests to fetch data from MARS:

```
RETRIEVE,
    CLASS      = EA,
    TYPE       = AN,
    STREAM     = OPER,
    EXPVER     = 0001,
    REPRES     = SH,
    LEVTYPE    = PL,
    LEVELIST   = 50/100/150/200/300/400/500/600/700/850/925/1000,
    PARAM      = q/t/u/v/w,
    DATE       = 20240101/to/20240131,
    TIME       = 0600/1800,
    STEP       = 0,
    DOMAIN     = G,
    TARGET     = "output"

RETRIEVE,
    CLASS      = EA,
    TYPE       = AN,
    STREAM     = OPER,
    EXPVER     = 0001,
    REPRES     = SH,
    LEVTYPE    = SFC,
    PARAM      = 10u/10v/2d/2t/lsm/msl/sdor/skt/slor/sp/tcw/z,
    DATE       = 20240101/to/20240131,
    TIME       = 0600/1800,
    STEP       = 0,
    DOMAIN     = G,
    TARGET     = "output"
```

Save the `output` GRIB file on you local machine and import this via setting up a temporary FDB. This
can be done as follows:

```

```


