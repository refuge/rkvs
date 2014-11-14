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
         fold_keys/4]).

-include("rkvs.hrl").

-type engine() :: #engine{}.
-type key() ::  term().
-type keys() :: [key()].
-type value() :: term().
-type kvs() :: [{key(), value()}].
-type ops_kvs() :: [{put, key(), value()}
                    | {delete, key(), value()}].
-type fold_options() :: [{start_key, key()} |
                         {end_key, key()} |
                         {max, integer()}].
-export_type([engine/0,
             key/0, keys/0,
             value/0,
             kvs/0,
             ops_kvs/0]).


%% @doc open a storage, amd pass options to the backend.
-spec open(Name::binary(), Options::list()) ->
    {ok, engine()} | {error, any()}.
open(Name, Options) ->
    Mod = proplists:get_value(backend, Options, rkvs_leveldb),
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
-spec fold_keys(engine(), fun(), any(), fold_options()) -> any() | {error, term()}.
fold_keys(#engine{mod=Mod}=Engine, Fun, Acc0, Opts) ->
    Mod:fold_keys(Engine, Fun, Acc0, Opts).

%% @doc fold all K/Vs with a function
-spec fold(engine(), function(), any(), fold_options()) ->
    any() | {error, term()}.
fold(#engine{mod=Mod}=Engine, Fun, Acc0, Opts) ->
    Mod:fold(Engine, Fun, Acc0, Opts).
