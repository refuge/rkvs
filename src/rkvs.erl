-module(rkvs).

-export([open/2,
         close/1,
         destroy/1,
         contains/2,
         get/2,
         put/3,
         clear/2,
         write_batch/2,
         scan/4,
         clear_range/4,
         fold/4,
         fold_keys/4,
         is_empty/1]).

-include("rkvs.hrl").

-type key() ::  term().
-type keys() :: [key()].
-type value() :: term().
-type kvs() :: [{key(), value()}].
-type ops_kvs() :: [{put, key(), value()}
                    | {delete, key(), value()}].
-type fold_options() :: [{start_key, key()} |
                         {end_key, key()} |
                         {gt, key()} | {gte, key()} |
                         {lt, key()} | {lte, key()} |
                         {max, integer()} |
                         {fill_cache, true | false}].

-export_type([key/0, keys/0,
             value/0,
             kvs/0,
             ops_kvs/0]).

%% @doc open a storage, amd pass options to the backend.
%% The following optinos can be used:
%% <ul>
%% <li>'backend': default is rkvs_ets, folowin backend are provided in rkvs:
%% rkvs_ets, rkvs_leveldb, rkvs_rocksdb, rkvs_hanoidb, rvs_bitcask.</li>
%% <li>'db_opts': backend options, refers to the backend doc for them</li>
%% <li>'key_encoding': to encode the key. defautl is raw (binary). can be
%% term, {term, Opts} or sext</li>
%% <li>'value_encoding': to encode the value. defautl is term. can be
%% term, {term, Opts} or sext</li>
%% </ul>
-spec open(Name::binary(), Options::list()) ->
    {ok, engine()} | {error, any()}.
open(Name, Options) ->
    Mod = proplists:get_value(backend, Options, rkvs_ets),
    Mod:open(Name, Options).


%% @doc close a storage
-spec close(engine()) -> ok | {error, any()}.
close(#engine{mod=Mod}=Engine) ->
    Mod:close(Engine).

%% @doc close a storage and remove all the data
-spec destroy(engine()) -> ok | {error, any()}.
destroy(#engine{mod=Mod}=Engine) ->
    Mod:destroy(Engine).

%% @doc is the key exists in the storage
-spec contains(engine(), key()) -> true | false.
contains(#engine{mod=Mod}=Engine, Key) ->
    Mod:contains(Engine, Key).

%% @doc get the value associated to the key
-spec get(engine(), key()) -> any() | {error, term()}.
get(#engine{mod=Mod}=Engine, Key) ->
    Mod:get(Engine, Key).

%% @doc store the value associated to the key.
-spec put(engine(), key(), value()) -> ok | {error, term()}.
put(#engine{mod=Mod}=Engine, Key, Value) ->
    Mod:put(Engine, Key, Value).

%% @doc delete the value associated to the key
-spec clear(engine(), key()) -> ok | {error, term()}.
clear(#engine{mod=Mod}=Engine, Key) ->
    Mod:clear(Engine, Key).

%% @doc do multiple operations on the backend.
-spec write_batch(engine(), ops_kvs()) -> ok | {error, term()}.
write_batch(#engine{mod=Mod}=Engine, Ops) ->
    Mod:write_batch(Engine, Ops).

%% @doc retrieve a list of Key/Value in a range
-spec scan(engine(), key(), key(), integer()) -> kvs() | {error, term()}.
scan(#engine{mod=Mod}=Engine, Start, End, Max) ->
    Mod:scan(Engine, Start, End, Max).

%% @doc delete all K/Vs in a range
-spec clear_range(engine(), key(), key(), integer()) -> ok | {error, term()}.
clear_range(#engine{mod=Mod}=Engine, Start, End, Max) ->
    Mod:clear_range(Engine, Start, End, Max).

%% @doc fold all keys with a function
%% same parameters as in the fold function.
-spec fold_keys(engine(), fun(), any(), fold_options()) -> any() | {error, term()}.
fold_keys(#engine{mod=Mod}=Engine, Fun, Acc0, Opts) ->
    Mod:fold_keys(Engine, Fun, Acc0, Opts).

%% @doc fold all K/Vs with a function
%% Additionnaly you can pass the following options:
%% <ul>
%% <li>'gt', (greater than), 'gte' (greather than or equal): define the lower
%% bound of the range to fold. Only the records where the key is greater (or
%% equal to) will be given to the function.</li>
%% <li>'lt' (less than), 'lte' (less than or equal): define the higher bound
%% of the range to fold. Only the records where the key is less than (or equal
%% to) will be given to the function</li>
%% <li>'start_key', 'end_key', legacy to 'gte', 'lte'</li>
%% <li>'max' (default=0), the maximum of records to fold before returning the
%% resut</li>
%% <li>'fill_cache' (default is true): should be the data cached in
%% memory?</li>
%% </ul>
-spec fold(engine(), function(), any(), fold_options()) ->
    any() | {error, term()}.
fold(#engine{mod=Mod}=Engine, Fun, Acc0, Opts) ->
    Mod:fold(Engine, Fun, Acc0, Opts).

%% @doc Returns true if this backend contains any values; otherwise returns false.
-spec is_empty(engine()) -> boolean() | {error, term()}.
is_empty(#engine{mod=Mod}=Engine) ->
    Mod:is_empty(Engine).
