***********************
Architecture and Design
***********************

.. mermaid::
   :align: center

   %%{config: {'block': {'useMaxWidth': true}}}%%
   block-beta
   columns 1
     a["z3fdb\n[Pure Python]"]
     b["pychunked_data_view\n[Pure Python]"]
     c["chunked_data_view_bindings.cpython.so\n[Native Python bindings]"]
     d["libchunked_data_view.so\n[Chunked access of data in view]"]
     e["libfdb5.so"]
     style a fill:#cfc
     style b fill:#cfc
     style c fill:#fcc
     style d fill:#fcc
     style e fill:#ccf
