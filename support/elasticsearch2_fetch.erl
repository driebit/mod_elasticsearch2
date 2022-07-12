%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc HTTP interface to Elastic, low level functions.
%% @enddoc

%% Copyright 2022 Driebit BV
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(elasticsearch2_fetch).

-export([
    request/5,
    get/3,
    index/4,
    delete/3,
    delete_index/2,
    bulk/2
    ]).

%% Total request timeout (1h)
-define(HTTPC_TIMEOUT, 3600000).

%% Connect timeout, server has to respond before this (10s)
-define(HTTPC_TIMEOUT_CONNECT, 10000).


%% @doc Fetch the data from the path with the given payload.
%% Useful to execute queries or put mappings.
-spec request(Connection, Method, Path, QArgs, Payload) -> Result when
    Connection :: elasticsearch2:connection(),
    Method :: get|put|post|delete|head,
    Path :: [ binary() | string() ],
    QArgs :: [ {binary()|string(), binary()|string()} ],
    Payload :: binary() | map(),
    Result :: elasticsearch2:result().
request(Connection, Method, Path, QArgs, Payload) when is_list(Path) ->
    Path1 = case Path of
        [] -> "/";
        _ -> [ [ $/, z_url:url_encode(P) ] || P <- Path ]
    end,
    Url = make_url(Connection, Path1, QArgs),
    Result = case with_body(Method) of
        false when Payload =:= <<>> ->
            httpc:request(Method,
                          {Url, []},
                          http_options(),
                          options(),
                          profile());
        false ->
            {error, method_payload};
        true ->
            httpc:request(Method,
                          {Url, [], "application/json", bin(Payload)},
                          http_options(),
                          options(),
                          profile())
    end,
    result(Result, Url).

with_body(get) -> false;
with_body(head) -> false;
with_body(delete) -> false;
with_body(_) -> true.


%% @doc Get a document from the index
-spec get(Connection, Index, DocId) -> Result when
    Connection :: elasticsearch2:connection(),
    Index :: elasticsearch2:index(),
    DocId :: elasticsearch2:doc_id(),
    Result :: elasticsearch2:result().
get(Connection, Index, DocId) ->
    Path = iolist_to_binary([
            $/,
            z_url:url_encode(z_convert:to_binary(Index)),
            <<"/_doc/">>,
            z_url:url_encode(z_convert:to_binary(DocId))]),
    Url = make_url(Connection, Path, []),
    Result = httpc:request(get,
                          {Url, []},
                          http_options(),
                          options(),
                          profile()),
    result(Result, Url).

%% @doc Add or update a document in the index.
-spec index(Connection, Index, DocId, Doc) -> Result when
    Connection :: elasticsearch2:connection(),
    Index :: elasticsearch2:index(),
    DocId :: elasticsearch2:doc_id(),
    Doc :: map() | binary(),
    Result :: elasticsearch2:result().
index(Connection, Index, DocId, Doc) ->
    case z_convert:to_binary(DocId) of
        <<>> ->
            {error, doc_id};
        DocId1 ->
            Path = iolist_to_binary([
                    $/,
                    z_url:url_encode(z_convert:to_binary(Index)),
                    <<"/_doc/">>,
                    z_url:url_encode(DocId1)]),
            Url = make_url(Connection, Path, []),
            Result = httpc:request(put,
                                  {Url, [], "application/json", bin(Doc)},
                                  http_options(),
                                  options(),
                                  profile()),
            result(Result, Url)
    end.

%% @doc Delete a document.
-spec delete(Connection, Index, DocId) -> Result when
    Connection :: elasticsearch2:connection(),
    Index :: elasticsearch2:index(),
    DocId :: elasticsearch2:doc_id(),
    Result :: elasticsearch2:result().
delete(Connection, Index, DocId) ->
    case z_convert:to_binary(DocId) of
        <<>> ->
            {error, doc_id};
        DocId1 ->
            Path = iolist_to_binary([
                    $/,
                    z_url:url_encode(z_convert:to_binary(Index)),
                    <<"/_doc/">>,
                    z_url:url_encode(DocId1)]),
            Url = make_url(Connection, Path, []),
            Result = httpc:request(delete,
                                  {Url, []},
                                  http_options(),
                                  options(),
                                  profile()),
            result(Result, Url)
    end.

%% @doc Delete an index.
-spec delete_index(Connection, Index) -> Result when
    Connection :: elasticsearch2:connection(),
    Index :: elasticsearch2:index(),
    Result :: elasticsearch2:result().
delete_index(Connection, Index) ->
    Path = iolist_to_binary([
            $/,
            z_url:url_encode(z_convert:to_binary(Index))]),
    Url = make_url(Connection, Path, []),
    Result = httpc:request(delete,
                          {Url, []},
                          http_options(),
                          options(),
                          profile()),
    result(Result, Url).


%% @doc Bulk index or delete documensts.
-spec bulk(Connection, Commands) -> Result when
    Connection :: elasticsearch2:connection(),
    Commands :: elasticsearch2:bulkcmd(),
    Result :: elasticsearch2:result().
bulk(Connection, Bulk) ->
    Url = make_url(Connection, <<"_bulk">>, []),
    Commands = lists:map(fun bulkcmd/1, Bulk),
    Lines = lists:map(fun jsx:encode/1, lists:flatten(Commands)),
    Body = [ [ Line, 10 ] || Line <- Lines ],
    Result = httpc:request(put,
                  {Url, [], "application/x-ndjson", iolist_to_binary(Body)},
                  http_options(),
                  options(),
                  profile()),
    result(Result, Url).


bulkcmd({index, Index, Id, Doc}) when is_map(Doc) ->
    case z_convert:to_binary(Id) of
        <<>> ->
            [];
        DocId ->
            [
                #{
                    <<"index">> => #{
                        <<"_index">> => z_convert:to_binary(Index),
                        <<"_id">> => DocId
                    }
                },
                Doc
            ]
    end;
bulkcmd({delete, Index, Id}) ->
    case z_convert:to_binary(Id) of
        <<>> ->
            [];
        DocId ->
            [
                #{
                    <<"delete">> => #{
                        <<"_index">> => z_convert:to_binary(Index),
                        <<"_id">> => DocId
                    }
                }
            ]
    end.


bin(B) when is_binary(B) -> B;
bin(JSON) -> jsx:encode(JSON).


result({error, _} = Error, Url) ->
    lager:error("Error on Elastic request ~s: ~p", [ Url, Error ]),
    Error;
result({ok, {{_Http, 201, _Reason}, _Hs, _Body}}, _Url) ->
    {ok, #{}};
result({ok, {{_Http, 204, _Reason}, _Hs, _Body}}, _Url) ->
    {ok, #{}};
result({ok, {{_Http, 200, _Reason}, _Hs, <<>>}}, _Url) ->
    {ok, #{}};
result({ok, {{_Http, 200, _Reason}, _Hs, Body}}, _Url) ->
    {ok, jsx:decode(Body)};
result({ok, {{_Http, Code, _Reason}, _Hs, Body}}, Url) ->
    lager:error("Error on Elastic request ~s, http ~p: ~s", [ Url, Code, Body ]),
    case Code of
        400 -> {error, request};
        401 -> {error, eacces};
        403 -> {error, eacces};
        404 -> {error, enoent};
        410 -> {error, enoent};
        500 -> {error, internal};
        503 -> {error, overload};
        527 -> {error, ratelimit};
        _ -> {error, http}
    end.

-spec ensure_profiles() -> ok.
ensure_profiles() ->
    case inets:start(httpc, [{profile, elasticsearch2}]) of
        {ok, _} ->
            ok = httpc:set_options([
                {max_sessions, 100},
                {max_keep_alive_length, 100},
                {keep_alive_timeout, 20000},
                {cookies, enabled}
            ], elasticsearch2),
            ok;
        {error, {already_started, _}} ->
            ok
    end.


make_url(#{ host := Host, port := Port }, Path, QArgs) ->
    Url = iolist_to_binary([
            <<"http://">>, Host, $:, integer_to_binary(Port), Path
        ]),
    Url1 = case QArgs of
        [] ->
            Url;
        _ ->
            Qs = lists:map(
                fun({A,V}) ->
                    [
                        z_url:url_encode(z_convert:to_binary(A)),
                        $=,
                        z_url:url_encode(z_convert:to_binary(V))
                    ]
                end,
                QArgs),
            Qs1 = z_utils:combine($&, Qs),
            iolist_to_binary([ Url, $?, Qs1 ])
    end,
    binary_to_list(Url1).

-spec profile() -> atom().
profile() ->
    ensure_profiles(),
    elasticsearch2.

options() ->
    [
        {body_format, binary}
    ].

http_options() ->
    [
        {autoredirect, false},
        {relaxed, true},
        {timeout, ?HTTPC_TIMEOUT},
        {connect_timeout, ?HTTPC_TIMEOUT_CONNECT}
    ].
