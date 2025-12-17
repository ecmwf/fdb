Reproducer for fdb-server (catalogue) segfault
==============================================

1. Run init_reproducer.py, this will create configs for store, catalogue and remote fdb.
2. Install multitail from brew
3. run start_server
4. run fdb-write in a new shell and import into fdb (use --config remote_config_file.yaml)
5. run fdb-inspect with: "class=d1,type=fc,stream=clte,levtype=hl,date=20141231,time=0000,dataset=climate-dt,expver=0001,levelist=100,param=131,activity=baseline,experiment=hist,generation=2,model=ifs-fesom,realization=1,resolution=high"
6. Observe core dump in catalogue server
