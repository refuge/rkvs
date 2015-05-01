

# Module rkvs #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-fold_options">fold_options()</a> ###



<pre><code>
fold_options() = [{start_key, <a href="#type-key">key()</a>} | {end_key, <a href="#type-key">key()</a>} | {gt, <a href="#type-key">key()</a>} | {gte, <a href="#type-key">key()</a>} | {lt, <a href="#type-key">key()</a>} | {lte, <a href="#type-key">key()</a>} | {max, integer()} | {fill_cache, true | false}]
</code></pre>





### <a name="type-key">key()</a> ###



<pre><code>
key() = term()
</code></pre>





### <a name="type-keys">keys()</a> ###



<pre><code>
keys() = [<a href="#type-key">key()</a>]
</code></pre>





### <a name="type-kvs">kvs()</a> ###



<pre><code>
kvs() = [{<a href="#type-key">key()</a>, <a href="#type-value">value()</a>}]
</code></pre>





### <a name="type-ops_kvs">ops_kvs()</a> ###



<pre><code>
ops_kvs() = [{put, <a href="#type-key">key()</a>, <a href="#type-value">value()</a>} | {delete, <a href="#type-key">key()</a>, <a href="#type-value">value()</a>}]
</code></pre>





### <a name="type-value">value()</a> ###



<pre><code>
value() = term()
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#clear-2">clear/2</a></td><td>delete the value associated to the key.</td></tr><tr><td valign="top"><a href="#clear_range-4">clear_range/4</a></td><td>delete all K/Vs in a range.</td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td>close a storage.</td></tr><tr><td valign="top"><a href="#contains-2">contains/2</a></td><td>is the key exists in the storage.</td></tr><tr><td valign="top"><a href="#destroy-1">destroy/1</a></td><td>close a storage and remove all the data.</td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td>fold all K/Vs with a function
Additionnaly you can pass the following options:
<ul>
<li>'gt', (greater than), 'gte' (greather than or equal): define the lower
bound of the range to fold. Only the records where the key is greater (or
equal to) will be given to the function.</li>
<li>'lt' (less than), 'lte' (less than or equal): define the higher bound
of the range to fold. Only the records where the key is less than (or equal
to) will be given to the function</li>
<li>'start_key', 'end_key', legacy to 'gte', 'lte'</li>
<li>'max' (default=0), the maximum of records to fold before returning the
resut</li>
<li>'fill_cache' (default is true): should be the data cached in
memory?</li>
</ul></td></tr><tr><td valign="top"><a href="#fold_keys-4">fold_keys/4</a></td><td>fold all keys with a function
same parameters as in the fold function.</td></tr><tr><td valign="top"><a href="#get-2">get/2</a></td><td>get the value associated to the key.</td></tr><tr><td valign="top"><a href="#is_empty-1">is_empty/1</a></td><td>Returns true if this backend contains any values; otherwise returns false.</td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td>open a storage, amd pass options to the backend.</td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td>store the value associated to the key.</td></tr><tr><td valign="top"><a href="#scan-4">scan/4</a></td><td>retrieve a list of Key/Value in a range.</td></tr><tr><td valign="top"><a href="#write_batch-2">write_batch/2</a></td><td>do multiple operations on the backend.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="clear-2"></a>

### clear/2 ###


<pre><code>
clear(Engine::<a href="#type-engine">engine()</a>, Key::<a href="#type-key">key()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

delete the value associated to the key
<a name="clear_range-4"></a>

### clear_range/4 ###


<pre><code>
clear_range(Engine::<a href="#type-engine">engine()</a>, Start::<a href="#type-key">key()</a>, End::<a href="#type-key">key()</a>, Max::integer()) -&gt; ok | {error, term()}
</code></pre>
<br />

delete all K/Vs in a range
<a name="close-1"></a>

### close/1 ###


<pre><code>
close(Engine::<a href="#type-engine">engine()</a>) -&gt; ok | {error, any()}
</code></pre>
<br />

close a storage
<a name="contains-2"></a>

### contains/2 ###


<pre><code>
contains(Engine::<a href="#type-engine">engine()</a>, Key::<a href="#type-key">key()</a>) -&gt; true | false
</code></pre>
<br />

is the key exists in the storage
<a name="destroy-1"></a>

### destroy/1 ###


<pre><code>
destroy(Engine::<a href="#type-engine">engine()</a>) -&gt; ok | {error, any()}
</code></pre>
<br />

close a storage and remove all the data
<a name="fold-4"></a>

### fold/4 ###


<pre><code>
fold(Engine::<a href="#type-engine">engine()</a>, Fun::function(), Acc0::any(), Opts::<a href="#type-fold_options">fold_options()</a>) -&gt; any() | {error, term()}
</code></pre>
<br />

fold all K/Vs with a function
Additionnaly you can pass the following options:

* 'gt', (greater than), 'gte' (greather than or equal): define the lower
bound of the range to fold. Only the records where the key is greater (or
equal to) will be given to the function.

* 'lt' (less than), 'lte' (less than or equal): define the higher bound
of the range to fold. Only the records where the key is less than (or equal
to) will be given to the function

* 'start_key', 'end_key', legacy to 'gte', 'lte'

* 'max' (default=0), the maximum of records to fold before returning the
resut

* 'fill_cache' (default is true): should be the data cached in
memory?


<a name="fold_keys-4"></a>

### fold_keys/4 ###


<pre><code>
fold_keys(Engine::<a href="#type-engine">engine()</a>, Fun::function(), Acc0::any(), Opts::<a href="#type-fold_options">fold_options()</a>) -&gt; any() | {error, term()}
</code></pre>
<br />

fold all keys with a function
same parameters as in the fold function.
<a name="get-2"></a>

### get/2 ###


<pre><code>
get(Engine::<a href="#type-engine">engine()</a>, Key::<a href="#type-key">key()</a>) -&gt; any() | {error, term()}
</code></pre>
<br />

get the value associated to the key
<a name="is_empty-1"></a>

### is_empty/1 ###


<pre><code>
is_empty(Engine::<a href="#type-engine">engine()</a>) -&gt; boolean() | {error, term()}
</code></pre>
<br />

Returns true if this backend contains any values; otherwise returns false.
<a name="open-2"></a>

### open/2 ###


<pre><code>
open(Name::binary(), Options::list()) -&gt; {ok, <a href="#type-engine">engine()</a>} | {error, any()}
</code></pre>
<br />

open a storage, amd pass options to the backend.
<a name="put-3"></a>

### put/3 ###


<pre><code>
put(Engine::<a href="#type-engine">engine()</a>, Key::<a href="#type-key">key()</a>, Value::<a href="#type-value">value()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

store the value associated to the key.
<a name="scan-4"></a>

### scan/4 ###


<pre><code>
scan(Engine::<a href="#type-engine">engine()</a>, Start::<a href="#type-key">key()</a>, End::<a href="#type-key">key()</a>, Max::integer()) -&gt; <a href="#type-kvs">kvs()</a> | {error, term()}
</code></pre>
<br />

retrieve a list of Key/Value in a range
<a name="write_batch-2"></a>

### write_batch/2 ###


<pre><code>
write_batch(Engine::<a href="#type-engine">engine()</a>, Ops::<a href="#type-ops_kvs">ops_kvs()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

do multiple operations on the backend.
