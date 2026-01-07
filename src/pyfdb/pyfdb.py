# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from pathlib import Path
from typing import Generator, List

import pyfdb._internal as _interal
from pyfdb import (
    URI,
    DataHandle,
)
from pyfdb._internal.pyfdb_internal import ConfigMapper, FDBToolRequest
from pyfdb.pyfdb_iterator import (
    ControlElement,
    IndexAxis,
    ListElement,
    MoveElement,
    PurgeElement,
    StatsElement,
    StatusElement,
    WipeElement,
)
from pyfdb.pyfdb_type import (
    ControlIdentifier,
    ControlAction,
    Identifier,
    MarsSelection,
    WildcardMarsSelection,
)


class FDB:
    def __init__(
        self,
        config: str | dict | Path | None = None,
        user_config: str | dict | Path | None = None,
    ) -> None:
        """
        Constructor for FDB object.

        Parameters
        ----------
        :`config` : `str` | `dict` | `Path` | `None`, *optional*
            Config object for setting up the FDB.
        :`user_config` : `str` | `dict` | `Path` | `None`, *optional*
            Config object for setting up user specific options, e.g., enabling sub-TOCs.

        Returns
        -------
        :returns: FDB object

        Note
        ----
        Every config parameter but is converted accordingly depending on its type:
            - `str` is used as a yaml representation to parse the config
            - `dict` is interpreted as hierarchical format to represent a config, see example
            - `Path` is interpreted as a location of the config and read as a YAML file
            - `None` is the fallback. The default config in `$FDB5_HOME` is loaded

        FDB and its methods are thread-safe. However the caller needs to be aware that flush acts on all archive calls,
        including archived messages from other threads. An call to flush will persist all archived messages regardless
        from which thread the message has been archived. In case the caller wants a finer control it is advised to
        instantiate one FDB object per thread to ensure only messages are flushed that have been archived on the same FDB
        object.

        Examples
        --------
        >>> fdb = pyfdb.FDB(fdb_config_path)
        >>> config = {
        ...     "type":"local",
        ...     "engine":"toc",
        ...     "schema":"<schema_path>",
        ...     "spaces":[
        ...         {
        ...             "handler":"Default",
        ...             "roots":[
        ...                 {"path": "<db_store_path>"},
        ...             ],
        ...         }
        ...     ],
        ... }
        >>> fdb = pyfdb.FDB(config)
        """

        _interal.init_bindings()

        # Convert to JSON if set
        config = ConfigMapper.to_json(config)
        user_config = ConfigMapper.to_json(user_config)

        if config is not None and user_config is not None:
            internal_config = _interal.Config(config, user_config)
            self.FDB = _interal._FDB(internal_config)
        elif config is not None:
            internal_config = _interal.Config(config, None)
            self.FDB = _interal._FDB(internal_config)
        else:
            self.FDB = _interal._FDB()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.flush()

    def archive(self, data: bytes, identifier: Identifier | None = None):
        """
        Archive binary data into the underlying FDB.
        *No constistency checks are applied. The caller needs to ensure the provided identifier matches metadata present in data.*

        Parameters
        ----------
        `data`: `bytes`
            The binary data to be archived.
        `identifier` : `Identifier` | None, optional
            A unique identifier for the archived data.
            - If provided, the data will be stored under this identifier.
            - If None, the data will be archived without an explicit identifier.

        Note
        ----
        Sometimes an identifier is also referred to as a Key.

        Returns
        -------
        None

        Examples
        --------
        >>> fdb = pyfdb.FDB()
        >>> filename = data_path / "x138-300.grib"
        >>> fdb.archive(data=filename.read_bytes()) # Archive
        >>> fdb.archive(identifier=Key([("key-1", "value-1")]), data=filename.read_bytes())
        >>> fdb.flush() # Sync the archive call
        """
        if identifier is None:
            self.FDB.archive(data, len(data))
        else:
            self.FDB.archive(str(identifier), data, len(data))

    def flush(self):
        """
        Flush all buffers and close all data handles of the underlying FDB into a consistent DB state.
        *Always safe to call*

        Parameters
        ----------
        None

        Returns
        -------
        None

        Examples
        --------
        >>> fdb = pyfdb.FDB()
        >>> filename = data_path / "x138-300.grib"
        >>> fdb.archive(bytes=filename.read_bytes()) # Archive
        >>> fdb.flush() # Data is synced
        """
        self.FDB.flush()

    def retrieve(self, mars_selection: MarsSelection) -> DataHandle:
        """
        Retrieve data which is specified by a MARS selection.

        Parameters
        ----------
        `mars_selection`
            MARS selection which describes the data which should be retrieved

        Returns
        -------
        DataHandle
            A data handle which can be read like a bytesLike object.

        Examples
        --------
        >>> mars_selection = {"key-1": "value-1", ...}
        >>> data_handle = pyfdb.retrieve(mars_selection)
        >>> data_handle.open()
        >>> data_handle.read(4)
        >>> data_handle.close()

        Or leveraging the context manager:

        >>> with pyfdb.retrieve(selection) as data_handle:
        >>>     assert data_handle
        >>>     assert data_handle.read(4) == b"GRIB"
        """
        mars_request = _interal.MarsRequest.from_selection(mars_selection)
        return DataHandle._from_raw(self.FDB.retrieve(mars_request.request))

    def list(
        self,
        selection: MarsSelection | WildcardMarsSelection,
        duplicates: bool = False,
        level: int = 3,
    ) -> Generator[ListElement, None, None]:
        """
        List data present at the underlying fdb archive and which can be retrieved.

        Parameters
        ----------
        `selection` : `MarsSelection` | `WildcardMarsSelection`
            A MARS selection which describes the data which can be listed.
        `duplicates` : bool, *optional*
            If True, the returned iterator lists duplicates, if False the elements are unique.
        `level` : int, *optional*
            Specifies the FDB schema level of the elements which are matching the selection.
            A level of 1 means return a level 1 key which is matching the MARS selection.

        Returns
        -------
        datahandle
            a data handle which can be read like a byteslike object.

        Note
        ----
        *this call lists duplicate elements.*

        Examples
        --------
        >>> selection = MarsSelection(
        >>>     {
        >>>         "type": "an",
        >>>         "class": "ea",
        >>>         "domain": "g",
        >>>         "expver": "0001",
        >>>         "stream": "oper",
        >>>         "date": "20200101",
        >>>         "levtype": "sfc",
        >>>         "step": "0",
        >>>         "time": "1800",
        >>>     },
        >>> )
        >>> list_iterator = pyfdb.list(selection) # level == 3
        >>> elements = list(list_iterator)
        >>> print(elements[0])

        {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
        {type=an,levtype=sfc}
        {step=0,param=131},
        tocfieldlocation[uri=uri[scheme=file,name=<location>],offset=10732,length=10732,remapkey={}],
        length=10732,
        timestamp=176253515

        >>> list_iterator = pyfdb.list(selection, level=2)
        >>> elements = list(list_iterator)
        >>> print(elements[0])

        {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
        {type=an,levtype=sfc},
        length=0,
        timestamp=0

        >>> list_iterator = pyfdb.list(selection, level=1)
        >>> elements = list(list_iterator)
        >>> print(elements[0])

        {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g},
        length=0,
        timestamp=0
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        iterator = self.FDB.list(fdb_tool_request.tool_request, not duplicates, level)
        while True:
            try:
                yield ListElement._from_raw(next(iterator))
            except StopIteration:
                return

    def inspect(
        self, mars_selection: MarsSelection
    ) -> Generator[ListElement, None, None]:
        """
        Inspects the content of the underlying FDB and returns a generator of list elements
        describing which field was part of the MARS selection.

        Parameters
        ----------
        `mars_selection` : `MarsSelection`
            An MARS selection for which the inspect should be executed

        Note
        ----
        *If multiple values for a key are specified, only the first one is respected and returned.*

        Returns
        -------
        Generator[ListElement, None, None]
            A generator for `ListElement` describing FDB entries containing data of the MARS selection


        Examples
        --------
        >>> selection = {
        >>>         "type": "an",
        >>>         "class": "ea",
        >>>         "domain": "g",
        >>>         "expver": "0001",
        >>>         "stream": "oper",
        >>>         "date": "20200101",
        >>>         "levtype": "sfc",
        >>>         "step": "0",
        >>>         "param": "167",
        >>>         "time": "1800",
        >>>     }
        >>> list_iterator = pyfdb.inspect(selection)
        >>> elements = list(list_iterator) # single element in iterator
        >>> elements[0]
        {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g}
        {type=an,levtype=sfc}
        {param=167,step=0},
        TocFieldLocation[
            uri=URI[scheme=<location>],
            offset=0,
            length=10732,
            remapKey={}
        ],
        length=10732,
        timestamp=1762537447
        """
        mars_request = _interal.MarsRequest.from_selection(mars_selection)
        iterator = self.FDB.inspect(mars_request.request)

        while iterator is not None:
            try:
                yield ListElement._from_raw(next(iterator))
            except StopIteration:
                return

    def status(
        self, selection: MarsSelection | WildcardMarsSelection
    ) -> Generator[StatusElement, None, None]:
        """
        List the status of all FDB entries with their control identifiers, e.g., whether a certain
        database was locked for retrieval.

        Parameters
        ----------
        `selection` : `MarsSelection` | `WildcardMarsSelection`
            An MARS selection which specifies the queried data

        Returns
        -------
        Generator[StatusElement, None, None]
            A generator for `StatusElement` describing FDB entries and their control identifier


        Examples
        --------
        >>> selection = {
        >>>         "type": "an",
        >>>         "class": "ea",
        >>>         "domain": "g",
        >>>     },
        >>> )
        >>> status_iterator = pyfdb.status(selection)
        >>> elements = list(status_iterator)
        >>> elements[0]
        ControlIdentifiers[],
        {class=ea,expver=0001,stream=oper,date=20200101,time=0000,domain=g},
        URI[
            scheme=toc,
            name=<location>
        ]
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        iterator = self.FDB.status(fdb_tool_request.tool_request)
        while True:
            try:
                yield StatusElement._from_raw(next(iterator))
            except StopIteration:
                return

    def wipe(
        self,
        selection: MarsSelection | WildcardMarsSelection,
        doit: bool = False,
        porcelain: bool = False,
        unsafe_wipe_all: bool = False,
    ) -> Generator[WipeElement, None, None]:
        """
        Wipe data from the database.

        Delete FDB databases and the data therein contained. Use the passed
        selection to identify the database to delete. This is equivalent to a UNIX rm command.
        This function deletes either whole databases, or whole indexes within databases

        Parameters
        ----------
        `selection` : `MarsSelection` | `WildcardMarsSelection`
            An MARS selection which specifies the affected data
        `doit` : `bool`, *optional*
            If true the wipe command is executed, per default there are only dry-run
        `porcelain` : `bool`, *optional*
            Restricts the output to the wiped files
        `unsafe_wipe_all` : `bool`, *optional*
            Flag for disabling all security checks and force a wipe

        Returns
        -------
        Generator[WipeElement, None, None]
            A generator for `WipeElement`

        Note
        ----
        Wipe elements are not directly corresponding to the wiped files. This can be a cause for confusion.
        The individual wipe elements strings of the wipe output.

        Examples
        --------
        >>> fdb = pyfdb.FDB(fdb_config_path)
        >>> wipe_iterator = fdb.wipe({"class": "ea"})
        >>> wiped_elements = list(wipe_iterator)
        ...
        Toc files to delete:
        <path_to_database>/toc
        ...
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        iterator = self.FDB.wipe(
            fdb_tool_request.tool_request, doit, porcelain, unsafe_wipe_all
        )
        while True:
            try:
                yield WipeElement._from_raw(next(iterator))
            except StopIteration:
                return

    def move(
        self, selection: MarsSelection, destination: URI
    ) -> Generator[MoveElement, None, None]:
        """
        Move content of one FDB database to a new URI.

        This locks the source database, make it possible to create a second
        database in another root, duplicates all data.
        Source data is moved.

        Parameters
        ----------
        `selection` : `MarsSelection`
            A MARS selection specifies the affected data
        `destination` : `URI`
            A new FDB root to which a database should be moved

        Returns
        -------
        Generator[MoveElement, None, None]
            A generator for `MoveElement`

        Note
        ----
        The destination `URI` must be a known root to the `FDB`, meaning it must be stated in
        the `FDB` config file.

        **The last element in the move_iterator contains a move element specifying the root of the
        database entry with a destination of '/'.**

        Examples
        --------
        >>> selection = {
        >>>         "class": "ea",
        >>>         "domain": "g",
        >>>         "expver": "0001",
        >>>         "stream": "oper",
        >>>         "date": "20200101",
        >>>         "time": "1800",
        >>>     },
        >>> )
        >>> fdb = pyfdb.FDB(fdb_config_path)
        >>> move_iterator = fdb.move(
        >>>     selection,
        >>>     URI.from_str(<new_root>),
        >>> )
        >>> print(list(move_iterator)[0])
        FileCopy(
            src=<db_store>/ea:0001:oper:20200101:1800:g/an:sfc.20251107.181626.???.???.309280594984980.data,
            dest=<new_db>/ea:0001:oper:20200101:1800:g/an:sfc.20251107.181626.???.???.309280594984980.data,
            sync=0
        )
        ...
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        iterator = self.FDB.move(fdb_tool_request.tool_request, destination._uri)
        while True:
            try:
                yield MoveElement._from_raw(next(iterator))
            except StopIteration:
                return

    def purge(
        self,
        selection: MarsSelection | WildcardMarsSelection,
        doit: bool = False,
        porcelain: bool = False,
    ) -> Generator[PurgeElement, None, None]:
        """
        Remove duplicate data from the database.

        Purge duplicate entries from the database and remove the associated data if the data is owned and not adopted.
        Data in the FDB5 is immutable. It is masked, but not removed, when overwritten with new data using the same key.
        Masked data can no longer be accessed. Indexes and data files that only contains masked data may be removed.

        If an index refers to data that is not owned by the FDB (in particular data which has been adopted from an
        existing FDB5), this data will not be removed.

        Parameters
        ----------
        `selection` : `MarsSelection` | `WildcardMarsSelection`
            A MARS selection which describes the data which is purged.
        `doit` : `bool`, *optional*
            If true the wipe command is executed, per default there are only dry-run
        `porcelain` : `bool`, *optional*
            Restricts the output to the wiped files

        Returns
        -------
        Generator[PurgeElement, None, None]
            A generator for `PurgeElement`

        Examples
        --------
        >>> fdb = pyfdb.FDB(fdb_config_path)
        >>> purge_iterator = fdb.purge({"class": "ea"}), doit=True)
        >>> purged_elements = list(purge_iterator)
        >>> print(purged_elements[0])
        {class=ea,expver=0001,stream=oper,date=20200104,time=1800,domain=g}
        {type=an,levtype=sfc}
        {step=0,param=167},
        TocFieldLocation[
            uri=URI[
                scheme=file,
                name=<location>
            ],
            offset=32196,
            length=10732,
            remapKey={}
        ],
        length=10732,
        timestamp=176253976
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        iterator = self.FDB.purge(fdb_tool_request.tool_request, doit, porcelain)
        while True:
            try:
                yield PurgeElement._from_raw(next(iterator))
            except StopIteration:
                return

    def stats(
        self, selection: MarsSelection | WildcardMarsSelection
    ) -> Generator[StatsElement, None, None]:
        """
        Print information about FDB databases, aggregating the
        information over all the databases visited into a final summary.

        Parameters
        ----------
        `selection` : `MarsSelection` | `WildcardMarsSelection`
            A MARS selection which specifies the affected data.

        Returns
        -------
        Generator[StatsElement, None, None]
            A generator for `StatsElement`

        Examples
        --------
        >>> fdb = pyfdb.FDB(fdb_config_path)
        >>> stats_iterator = fdb.stats(selection)
        >>> for el list(stats_iterator):
        >>>     print(el)
        Index Statistics:
        Fields                          : 3
        Size of fields                  : 32,196 (31.4414 Kbytes)
        Reacheable fields               : 3
        Reachable size                  : 32,196 (31.4414 Kbytes)

        DB Statistics:
        Databases                       : 1
        TOC records                     : 2
        Size of TOC files               : 2,048 (2 Kbytes)
        Size of schemas files           : 228 (228 bytes)
        TOC records                     : 2
        Owned data files                : 1
        Size of owned data files        : 32,196 (31.4414 Kbytes)
        Index files                     : 1
        Size of index files             : 131,072 (128 Kbytes)
        Size of TOC files               : 2,048 (2 Kbytes)
        Total owned size                : 165,544 (161.664 Kbytes)
        Total size                      : 165,544 (161.664 Kbytes)
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        iterator = self.FDB.stats(fdb_tool_request.tool_request)
        while True:
            try:
                yield StatsElement._from_raw(next(iterator))
            except StopIteration:
                return

    def control(
        self,
        selection: MarsSelection | WildcardMarsSelection,
        control_action: ControlAction,
        control_identifiers: List[ControlIdentifier],
    ) -> Generator[ControlElement, None, None]:
        """
        Enable certain features of FDB databases, e.g., disables or enables retrieving, list, etc.

        Parameters
        ----------
        `selection` : `MarsSelection` | `WildcardMarsSelection`
            A MARS selection which specifies the affected data.
        `control_action` : `ControlAction`
            Which action should be modified, e.g., ControlAction.RETRIEVE
        `control_identifier` : `ControlIdentifier`
            Should an action be enabled or disabled, e.g., ControlIdentifier.ENABLE or ControlIdentifier.DISABLE

        Returns
        -------
        Generator[ControlElement, None, None]
            A generator for `ControlElement`

        Note
        ----
        Disabling of an ControlAction, e.g., ControlAction.RETRIEVE leads to the creation
        of a `retrieve.lock` in the corresponding FDB database. This is true for all actions.
        The file is removed after the Action has been disabled.

        **It's important to consume the iterator, otherwise the lock file isn't deleted which
        can cause unexpected behavior. Also, due to internal reuse of databases, create a new FDB
        object before relying on the newly set control_identifier, to propagate the status.**

        Examples
        --------
        >>> fdb = pyfdb.FDB(fdb_config_path)
        >>> selection = {
        >>>         "class": "ea",
        >>>         "domain": "g",
        >>>         "expver": "0001",
        >>>         "stream": "oper",
        >>>         "date": "20200101",
        >>>         "time": "1800",
        >>> }
        >>> control_iterator = fdb.control(
        >>>     selection,
        >>>     ControlAction.DISABLE,
        >>>     [ControlIdentifier.RETRIEVE],
        >>> )
        >>> elements = list(control_iterator)
        >>> print(elements[0])
        ControlIdentifiers[2],
        {class=ea,expver=0001,stream=oper,date=20200101,time=1800,domain=g},
        URI[
            scheme=toc,
            name=<location_fdb_database>
        ]
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        raw_control_identifiers = [
            control_identifier._to_raw() for control_identifier in control_identifiers
        ]
        iterator = self.FDB.control(
            fdb_tool_request.tool_request,
            control_action._to_raw(),
            raw_control_identifiers,
        )
        while True:
            try:
                yield ControlElement._from_raw(next(iterator))
            except StopIteration:
                return

    def axes(
        self, selection: MarsSelection | WildcardMarsSelection, level: int = 3
    ) -> IndexAxis:
        """
        Return the 'axes' and their extent of a MARS selection for a given level of the schema in
        an IndexAxis object.

        If a key isn't specified the entire extent (all values) are returned.

        Parameters
        ----------
        `selection` : `MarsSelection` | `WildcardMarsSelection`
            A MARS selection which specifies the affected data.
        `level` : `int`
            Level of the FDB Schema. Only keys of the given level are returned.

        Returns
        -------
        IndexAxis
            A map containing Key-Value pairs of the axes and their extent

        Examples
        --------
        >>> fdb = pyfdb.FDB(fdb_config_path)
        >>> selection = {
        ...         "type": "an",
        ...         "class": "ea",
        ...         "domain": "g",
        ...         "expver": "0001",
        ...         "stream": "oper",
        ...         "levtype": "sfc",
        ...         "step": "0",
        ...         "time": "1800",
        ... }
        >>> index_axis: IndexAxis = fdb.axes(selection) # level == 3
        >>> for k, v in index_axis.items():
        ...     print(f"k={k} \t| v={v}")
        k=class    | v=['ea']
        k=date     | v=['20200101', '20200102', '20200103', '20200104']
        k=domain   | v=['g']
        k=expver   | v=['0001']
        k=levelist | v=['']
        k=levtype  | v=['sfc']
        k=param    | v=['131', '132', '167']
        k=step     | v=['0']
        k=stream   | v=['oper']
        k=time     | v=['1800']
        k=type     | v=['an']
        """
        fdb_tool_request = FDBToolRequest.from_mars_selection(selection)
        return IndexAxis._from_raw(self.FDB.axes(fdb_tool_request.tool_request, level))

    def enabled(self, control_identifier: ControlIdentifier) -> bool:
        """
        Check whether a specific control identifier is enabled

        Parameters
        ----------
        `control_identifier` : `ControlIdentifier`
            A given control identifier

        Returns
        -------
        `bool`
            `True` if the given control identifier is set, `False` otherwise.

        Examples
        --------
        >>> fdb_config = yaml.safe_load(fdb_config_path)
        >>> fdb_config["writable"] = False
        >>> fdb = pyfdb.FDB(fdb_config)
        >>> fdb.enabled(ControlIdentifier.NONE) # == True
        >>> fdb.enabled(ControlIdentifier.LIST) # == True
        >>> fdb.enabled(ControlIdentifier.RETRIEVE) # == True
        >>> fdb.enabled(ControlIdentifier.ARCHIVE) # == False, default True
        >>> fdb.enabled(ControlIdentifier.WIPE) # == False, default True
        >>> fdb.enabled(ControlIdentifier.UNIQUEROOT) # == True

        """
        return self.FDB.enabled(control_identifier._to_raw())

    def needs_flush(self):
        """
        Return whether a flush of the FDB is needed.

        Parameters
        ----------
        None

        Returns
        -------
        `bool`
            `True` if an archive happened and a flush is needed, `False` otherwise.


        Examples
        --------
        >>> fdb_config = Config(config_file.read())
        >>> fdb = FDB(fdb_config)
        >>> filename = <data_path>
        >>> fdb.archive(open(filename, "rb").read())
        >>> fdb.needs_flush()                         # == True
        >>> fdb.flush()
        >>> fdb.needs_flush()                         # == False

        """
        return self.FDB.dirty()

    def config(self) -> str:
        return str(self.FDB.config())

    def __repr__(self) -> str:
        return str(self.FDB)
