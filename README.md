Catena
========

Overview
---------


High-level design
------------------

Catena is a column-oriented storage system.  It is composed of a
hierarchy of storage abstractions.  At the Highest level of
abstraction are the vectors.  Vectors are comprised of 1 or more
segments.

Segments are in turn comprised of pages, the lowest level abstraction.
Pages contain binary data that is read and written to and from
segments by vectors.

Pages are completely transient structures that are encapsulated by
segments.  Each segment corresponds to an on-disk file.  The on-disk
meta data for a segment consists of the segment's type, page count and
size.

Segments are designed to be loaded into memory as a unit.  We call
this process pinning a segment.  A segment is pinned by pinning each
of its pages, and a segment is unpinned by unpinning each of its
pages.

To pin a segment we query the cache for its pages.  A page query
always returns a pinned page.  A cache-miss causes the page to be
loaded into memory.  Both cache-misses and cache-hits increment the
pin count and update the statistics.

Each vector resides in its own directory on the local filesystem.
Each vector directory consists of an vector descriptor and 0 or more
segments files.

The vector descriptor file represents the ondisk meta data for the
vector.  It consists of the vector type, length and unique identifier.
When an vector is created a descriptor is also created.  The
descriptor is passed the master key.  The master key is a CompositeKey
whose first component is the base directory and the vector name
concatenated together.

The first segment of an vector is associated with a CompositeKey whose
first component is the master key and whose second component is 0.  As
elements are added to the end of the vector at some point the vector
append split boundary is crossed a new segment will be created.

Segments are always created on an even object boundary - an element of
a vector never spans more than one segment.  The new segment will have
a CompositeKey that consists of the master key as the first component
and the next natural number as the second component.

VectorDescriptors also maintain an update split boundary such that
updates to variable length types keep a vector to a manageable size.
When the update split threshold is surpassed the vector is split and
new keys are created.

CompositeKeys are hierarchically collated.  When a split occurs as the
result of an update, Catena creates a new segment between the other
segments.  To keep track of this a 3rd component is added to the key.
This component follows the same natural ordering as the second
component.  Consider the following keys:

MasterKey component0:basdir+name
Segment0Key component0:MasterKey component1:0
Segement1Key component0:MasterKey component1:0 component:0
Segement2Key component0:MasterKey component1:0 component:1
Segement3Key component0:MasterKey component1:1

The collation defined by the CompositeKey enforces the correct
ordering in searches.

A registry manages the meta data of vectors.  At startup time, the
registry is passed the path to the base directory of the vector
system, and the registry walks the directory loading the vector
descriptors and locating their segments.

Deltas are defined as inserts, updates and deletes.  Depending on the
consistency level, deltas are either automatically committed or are
manually committed.  When a vector is committed all of its deltas and
the meta data associated with those are flushed to disk (in the future
configuration settings will determine whether commits are flushed to
disk or replicated to other nodes in the cluster).

Transactions
-------------

MVCC: To be completed

Excecutor
----------


Examples
---------