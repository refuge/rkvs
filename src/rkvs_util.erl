%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(rkvs_util).

-include("rkvs.hrl").

-export([fold_options/2]).
-export([enc/3, dec/3]).

fold_options([], Options) ->
    Options;
fold_options([{start_key, Start} | Rest], Options)
  when is_binary(Start) or (Start =:= first) ->
    fold_options(Rest, Options#fold_options{gte=Start});
fold_options([{end_key, End} | Rest], Options)
  when is_binary(End) or (End == nil) ->
    fold_options(Rest, Options#fold_options{lte=End});
fold_options([{gt, GT} | Rest], Options)
  when is_binary(GT) or (GT =:= first) ->
    fold_options(Rest, Options#fold_options{gt=GT});
fold_options([{gte, GT} | Rest], Options)
  when is_binary(GT) or (GT =:= first) ->
    fold_options(Rest, Options#fold_options{gte=GT});
fold_options([{lt, LT} | Rest], Options)
  when is_binary(LT) or (LT == nil) ->
    fold_options(Rest, Options#fold_options{lt=LT});
fold_options([{lte, LT} | Rest], Options)
  when is_binary(LT) or (LT == nil) ->
    fold_options(Rest, Options#fold_options{lte=LT});
fold_options([{max, Max} | Rest], Options) ->
    fold_options(Rest, Options#fold_options{max=Max});
fold_options([_ | Rest], Options) ->
    fold_options(Rest, Options).

enc(_, V, raw) -> V;
enc(_, V, term) -> term_to_binary(V);
enc(_, V, {term, Opts}) -> term_to_binary(V, Opts);
enc(_, V, sext) -> sext:encode(V);
enc(T, V, E) -> erlang:error({cannot_encode, [T, V, E]}).

dec(_, V, raw) -> V;
dec(_, V, term) -> binary_to_term(V);
dec(_, V, {term, _}) -> binary_to_term(V);
dec(_, V, sext) -> sext:decode(V);
dec(T, V, E) -> erlang:error({cannot_decode, [T, V, E]}).
