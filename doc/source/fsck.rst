fsck
====

The fsck is used to query Crail internals and perform management operations.


Reference
-----------------

.. list-table::
   :header-rows: 1

   * - Argument
     - Default
     - Experiment type
     - Description
   * - :code:`-t <experiment>`
     - *-*
     - N/A
     -  * :code:`getLocations`
        * :code:`directoryDump`
        * :code:`namenodeDump`
        * :code:`blockStatistics`
        * :code:`ping`
        * :code:`createDirectory`
   * - :code:`-f <path>`
     - /tmp.dat
     -  * :code:`getLocations`
        * :code:`directoryDump`
        * :code:`namenodeDump`
        * :code:`blockStatistics`
        * :code:`createDirectory`
     - Path to perform operation with
   * - :code:`-y <offset>`
     - 0
     -  * :code:`getLocations`
     - Offset into file
   * - :code:`-l <length>`
     - 1
     -  * :code:`getLocations`
     - Length starting from offset (-y)
   * - :code:`-c <storage_class>`
     - 0
     - * :code:`createDirectory`
     - Storage class of directory
   * - :code:`-p <location_class>`
     - 0
     - * :code:`createDirectory`
     - Location class of directory
