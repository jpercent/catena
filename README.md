#Catena

### Overview

Catena is a transactional, column-oriented storage engine.  It is
designed to be used as part of a larger, more complete data management
system.  

The motivation behind Catena extends to the larger system, so it makes
sense to talk a bit about that here.  The vision is to create a data
management system that can be easily extended and analyzed.  Pretty
simple goals.

##### The Vision

Most modern data management systems consist of 3 components: an
interpreter or compiler, an optimizer, and a storage engine.  It is
convenient to view these components as a stack.

At the bottom of the stack is the compiler.  It transforms data
management requests into a objects for execution.  The next layer is
the optimizer. It generates a new plan that is [hopefully] optimized.
Finally, the optimized object is executed against the storage engine.

Basically, the vision is to build a data management system that is
highly conducive to research and experimentation.  To accomplish this,
we employ 2 ideas.

Firstly, we provide a clear separation of concerns by defining simple,
generalized interfaces between the 3 major components, so that these
components can be composed, interchanged and injected together at
run-time.

For example, image you want to create an optimizer that is composed of
two existing optimizers and uses a decision engine to choose which
optimizer to use.  We can think of fabulously innovative software
compositions using kind of computing paradigm.

Secondly, automated, performance analysis is burned into every layer
of the system.  The idea is to provide a holistic view of micro and
macro metrics across the system.  For example, suppose a developer
tests a new algorthim in the optimizer.  The vision is to provide the
developer with feedback, across a wide range of characteristic
workloads, by incorporating automation that employs standard analysis
at each layer and from the system as a whole.  

### Core Concepts

Catena is a column-oriented storage engine.  It provides a simple,
generalized executor interface which supports defining, querying and
mutating data sets.  Catena is designed to optimize workloads that
scan a large number of rows from small number of columns.  

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

Each array resides in its own directory on the local filesystem.
Each array directory consists of an array descriptor and 0 or more
segments files.

The array descriptor file represents the ondisk meta data for the
array.  It consists of the array type, length and unique identifier.
When an array is created a descriptor is also created.

##### Caching

Segments are designed to be loaded into memory as a unit.  We call
this process pinning a segment.  A segment is pinned by pinning each
of its pages, and a segment is unpinned by unpinning each of its
pages.

To pin a segment we query the cache for its pages.  A page query
always returns a pinned page.  A cache-miss causes the page to be
loaded into memory.  Both cache-misses and cache-hits increment the
pin count and update the statistics.

##### Keys

When an array is created an ArrayDescriptor and CompositeKey, the
master key, are also created.  The descriptor is passed the master
key.  The master key is a CompositeKey whose first component is the
base directory and the array name concatenated together.

As elements are added to the end of the array at some point the array
append split boundary is crossed a new segment will be created.

Segments are always created on an even object boundary - an element of
an array never spans more than one segment.  The new segment will have
a CompositeKey that consists of the master key as the first component
and the next natural number as the second component.

ArrayDescriptors also maintain an update split boundary such that
updates to variable length types keep an array to a manageable size.
When the update split threshold is surpassed the array is split and
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

MVCC: To be completed

Excecutor Examples
-------------------
