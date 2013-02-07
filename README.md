#Catena

### Overview

Catena is a transactional, column-oriented storage engine.  It is
designed to be used as part of a larger, data management system.  The
motivation behind Catena extends to the larger system, so it makes
sense to talk a bit about that here.

The vision is to build a complete data management system that is
highly conducive to research and experimentation.  To accomplish this,
we employ 2 simple ideas.

Firstly, provide a clear separation of concerns by defining simple,
generalized interfaces among the compiler, optimizer and storage
engine compnents.  This is so these components can be composed,
interchanged and injected dynamically.

Secondly, burn performance analysis into every layer of the system.

### Core Concepts

Catena is a column-oriented storage engine, designed to optimize
execution of workloads that scan a large number of rows from small
number of columns.

##### Architecture

Catena is composed of a hierarchy of storage abstractions.  At the highest
level of abstraction are arrays.  Arrays are comprised of 1 or more
segments.

A segment is in turn comprised of pages, the lowest level abstraction.
Pages contain binary data that is read and written to and from
segments by arrays.

Pages are completely transient structures that are encapsulated by
segments.  Each segment corresponds to an on-disk file.  The on-disk
meta data for a segment consists of the segment's type, page count and
size.

Each array resides in its own directory on the local filesystem.  Each
array directory consists of an array descriptor and 0 or more segments
files.  The array descriptor file, which is created at the time of
array creation, represents the ondisk meta data for the array.  It
consists of the array type, length and unique identifier.  When an
array is created a corresponding descriptor is also created.

##### Caching

Segments are designed to be loaded into memory as a unit.  We call
this process pinning a segment.  A segment is pinned by pinning each
of its pages, and a segment is unpinned by unpinning each of its
pages.

To pin a segment we query the cache for its pages.  A page query
always returns a pinned page.  A cache-miss causes the page to be
loaded into memory.  Both cache-misses and cache-hits increment the
pin count and update the statistics.

##### Segments and Keys

When an array and its descriptor are created, a master key is also
created and passed to the descriptor.  The master key is a
concatenation of the base directory and the array name.

The first segment of an array is associated with a composite key whose
first component is the master key and whose second component is 0.  As
elements are added to the end of the array, eventually, the
append-split threshold will be crossed and a new segment will be
created.  Segments are always created on an even object boundary - an
element of an array never spans more than one segment.  The new
segment will have a composite key that consists of the master key as
the first component and the next natural number as the second
component.

The array's descriptor also maintains an update-split threshold such
that updates to variable length types will never cause the array's
size to become unmanageable.  

Composite keys are hierarchically collated.  When an update-split
occurs, Catena creates a new segment between the other segments.  To
keep track of this an additional component, governed by a natural
ordering, is appended.  Consider the following keys:

<pre>
Segment0Key = MasterKey:0
Segment2Key = MasterKey:0:1
Segment1Key = MasterKey:1
</pre>

The collation defined by the composite key defines the correct
ordering for searches.

##### Registry

A registry manages the meta data of arrays.  At startup time, the
registry is passed the path to the base directory of the array
system, and the registry walks the directory loading the array
descriptors and locating their segments.

Deltas are defined as inserts, updates and deletes.  Depending on the
consistency level, deltas are either automatically committed or are
manually committed.  When an array is committed all of its deltas and
the meta data associated with those are flushed to disk (in the future
configuration settings will determine whether commits are flushed to
disk or replicated to other nodes in the cluster).

Transactions
-------------

MVCC

Excecutor Examples
-------------------
