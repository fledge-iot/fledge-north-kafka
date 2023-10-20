================================
Fledge "Kafka" C++ North Plugin
================================

This is a simple Fledge north plugin for sending data to Apache Kafka.

Fledge acts as a Kafka producer, sending reading data to Kafka. This
implementation is a simplified producer that sends all data on a single
Kafka topic. Each message contains an asset name, timestamp and set of
readings values as a JSON document.

`See documentation <docs/index.rst>`_

Copyright
---------

This plugin buils upon the open source librdkafka library and as such portions
of ths work are copyright Magnbus Edenhill.

librdkafka - Apache Kafka C driver library

Copyright (c) 2012-2018, Magnus Edenhill
All rights reserved.

Build
-----

The plugin is built on top of the librdkafka library. This must be downloaded, built
and installed before the plugin can be built.

.. code-block:: console

  $ git clone https://github.com/edenhill/librdkafka.git
  $ cd librdkafka
  $ ./configure
  $ make
  $ sudo make install

To build Fledge "Kafka" C++ north plugin:

.. code-block:: console

  $ mkdir build
  $ cd build
  $ cmake ..

- By default the Fledge develop package header files and libraries
  are expected to be located in /usr/include/fledge and /usr/lib/fledge
- If **FLEDGE_ROOT** env var is set and no -D options are set,
  the header files and libraries paths are pulled from the ones under the
  FLEDGE_ROOT directory.
  Please note that you must first run 'make' in the FLEDGE_ROOT directory.

You may also pass one or more of the following options to cmake to override 
this default behaviour:

- **FLEDGE_SRC** sets the path of a Fledge source tree
- **FLEDGE_INCLUDE** sets the path to Fledge header files
- **FLEDGE_LIB sets** the path to Fledge libraries
- **FLEDGE_INSTALL** sets the installation path of Random plugin

NOTE:
 - The **FLEDGE_INCLUDE** option should point to a location where all the Fledge 
   header files have been installed in a single directory.
 - The **FLEDGE_LIB** option should point to a location where all the Fledge
   libraries have been installed in a single directory.
 - 'make install' target is defined only when **FLEDGE_INSTALL** is set

Examples:

- no options

  $ cmake ..

- no options and FLEDGE_ROOT set

  $ export FLEDGE_ROOT=/some_fledge_setup

  $ cmake ..

- set FLEDGE_SRC

  $ cmake -DFLEDGE_SRC=/home/source/develop/Fledge  ..

- set FLEDGE_INCLUDE

  $ cmake -DFLEDGE_INCLUDE=/dev-package/include ..
- set FLEDGE_LIB

  $ cmake -DFLEDGE_LIB=/home/dev/package/lib ..
- set FLEDGE_INSTALL

  $ cmake -DFLEDGE_INSTALL=/home/source/develop/Fledge ..

  $ cmake -DFLEDGE_INSTALL=/usr/local/fledge ..
