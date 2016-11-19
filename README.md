# BobbyTables

[![Build Status](https://travis-ci.org/rossdylan/bobbytables.svg?branch=master)](https://travis-ci.org/rossdylan/bobbytables)

Do you need to increment integers? Do you need to increment them quickly? Then
this is the hashtable for you. Built on Patented "My Friend Rob Made it Up"
technology.  BobbyTables uses rusts builtin support for atomics to provide a
threadsafe hashtable that can be used for storing integers using 16 byte keys.
Based on the original implementation made by [robgssp](https://github.com/robgssp) for [UVB](https://github.com/rossdylan/uvb-server).
