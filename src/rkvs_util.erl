%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(rkvs_util).

-include("rkvs.hrl").

-export([fold_options/2]).

fold_options([], Options) ->
    Options;
fold_options([{start_key, Start} | Rest], Options)
        when is_binary(Start) or (Start =:= first) ->
    fold_options(Rest, Options#fold_options{start_key=Start});
fold_options([{end_key, End} | Rest], Options)
        when is_binary(End) or (End == nil) ->
    fold_options(Rest, Options#fold_options{end_key=End});
fold_options([{max, Max} | Rest], Options) ->
    fold_options(Rest, Options#fold_options{max=Max});
fold_options([_ | Rest], Options) ->
    fold_options(Rest, Options).
