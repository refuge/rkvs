%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(rkvs_storage_backend).

-include("rkvs.hrl").

-type engine() :: #engine{}.
-type write_ops() :: [{put, Key :: binary(), Value :: any()} |
                      {delete, Key :: binary()} |
                      clear].

-type kv() :: {Key:: binary(), Value::binary()}.
-type kvs() :: [kv()].

-type fold_options() :: [{start_key, binary()} |
                         {end_key, binary()} |
                         {max, integer()}].


-export_type([engine/0,
              write_ops/0,
              kv/0,
              kvs/0,
              fold_options/0]).

-callback open(Name :: binary(), Options :: list()) ->
    {ok, Engine :: engine()}
    | {error, Reason :: any()}.

-callback close(Engine :: engine()) ->
    ok
    | {error, Reason :: any()}.

-callback destroy(Engine :: engine()) ->
    ok
    | {error, Reason :: any()}.


-callback contains(Engine :: engine(), Key :: binary()) ->
    true
    | false.


-callback get(Engine :: engine(), Key :: binary()) ->
    {ok, Value :: any()}
    | {error, Reason :: any()}.

-callback put(Engine :: engine(), Key :: binary(), Value :: any()) ->
    ok
    | {error, Reason :: any()}.


-callback clear(Engine :: engine(), Key :: binary()) ->
    ok
    | {error, Reason :: any()}.


-callback write_batch(Engine :: engine(), Ops :: write_ops()) ->
    ok
    | {error, Reason :: any()}.


-callback scan(Engine :: engine(), Start :: binary(), End :: binary(),
               Max :: integer()) ->
    KeyValues :: kvs()
    | {error, Reason :: any()}.

-callback clear_range(Engine :: engine(), Start :: binary(), End :: binary(),
                      Max :: integer()) ->
    ok
    | {error, Reason :: any()}.


-callback fold_keys(Engine :: engine(), Fun :: function(), AccIn :: any(),
                    Options :: fold_options()) ->
    AccOut :: any()
    | {error, Reason :: any()}.

-callback fold(Engine :: engine(), Fun :: function(), AccIn :: any(),
                    Options :: fold_options()) ->
    AccOut :: any()
    | {error, Reason :: any()}.
