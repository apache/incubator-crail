.. Licensed under the Apache License, Version 2.0 (the "License"); you may not
.. use this file except in compliance with the License. You may obtain a copy of
.. the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
.. WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
.. License for the specific language governing permissions and limitations under
.. the License.

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
