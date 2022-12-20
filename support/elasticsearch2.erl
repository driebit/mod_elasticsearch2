%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc Main interface functions for fetching and indexing documents with
%% Elasticsearch.
%% @end

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

-module(elasticsearch2).
-author("Driebit <tech@driebit.nl>").

-export([
    connection/1,
    index/1,
    bulk/2,
    get_doc/2,
    get_doc/3,
    put_doc/2,
    put_doc/3,
    put_doc/4,

    put_bulkcmd/2,
    put_bulkcmd/3,
    put_bulkcmd/4,
    delete_bulkcmd/2,
    delete_bulkcmd/3,

    put_mapping/2,
    put_mapping/3,
    delete_doc/2,
    delete_doc/3,

    fold/4,
    fold/5
]).

-type connection() :: #{ host := binary(), port := non_neg_integer() }.
-type doc_id() :: binary() | string() | integer().
-type index() :: binary() | string().
-type bulkcmd() :: {index, index(), doc_id(), Doc::map()}
                 | {delete, index(), doc_id()}.
-type result() :: {ok, map()} | {error, reason()}.
-type reason() :: request
                | eacces
                | eacces
                | enoent
                | enoent
                | internal
                | overload
                | ratelimit
                | term().

-export_type([ connection/0, doc_id/0, index/0, bulkcmd/0, result/0, reason/0 ]).

-include("zotonic.hrl").
-include("../include/elasticsearch2.hrl").

%% @doc Get Elasticsearch connection params from config
-spec connection(Context) -> Connection when
    Context :: z:context(),
    Connection :: connection().
connection(Context) ->
    Host = z_convert:to_binary(m_config:get_value(mod_elasticsearch2, host, Context)),
    case z_utils:is_empty(Host) of
        true ->
            #{
                host => z_config:get(elasticsearch2_host, <<"127.0.0.1">>),
                port => z_config:get(elasticsearch2_port, 9200)
            };
        false ->
            Port = case z_convert:to_integer(m_config:get_value(mod_elasticsearch2, port, Context)) of
                undefined -> 9200;
                0 -> 9200;
                P -> P
            end,
            #{
                host => Host,
                port => Port
            }
    end.

%% @doc Get Elasticsearch index name from config; defaults to site name with the environment.
%% For example: mysite_production
-spec index(Context) -> Index when
    Context :: z:context(),
    Index :: index().
index(Context) ->
    case z_convert:to_binary(m_config:get_value(mod_elasticsearch2, index, Context)) of
        <<>> ->
            SiteName = z_convert:to_binary(z_context:site(Context)),
            Environment = z_convert:to_binary(m_site:environment(Context)),
            <<SiteName/binary, $_, Environment/binary>>;
        Value ->
            Value
    end.

%% @doc Fetch a single document by Id from the default index.
-spec get_doc( DocId, Context ) -> Result when
    DocId :: doc_id(),
    Context :: z:context(),
    Result :: result().
get_doc(DocId, Context) ->
    get_doc(index(Context), DocId, Context).

%% @doc Fetch a single document by Id from the index.
-spec get_doc( Index, DocId, Context ) -> Result when
    Index :: index(),
    DocId :: doc_id(),
    Context :: z:context(),
    Result :: result().
get_doc(Index, DocId, Context) ->
    case elasticsearch2_fetch:get(connection(Context), Index, DocId) of
        {ok, #{ <<"_source">> := Source } = Doc} ->
            % Set the "_type" field from the 'es_type' source field. This is for compatiblity
            % with templates that expect the _type field to be set. An example is mod_ginger_collection
            % where the _type field is used to store the database name.
            {ok, Doc#{
                <<"_type">> => maps:get(<<"es_type">>, Source, <<>>)
            }};
        {ok, Doc} ->
            {ok, Doc};
        {error, _} = Error ->
            Error
    end.

%% @doc Create Elasticsearch mapping
-spec put_mapping( Doc, Context ) -> Result when
    Doc :: map(),
    Context :: z:context(),
    Result :: result().
put_mapping(Doc, Context) ->
    put_mapping(index(Context), Doc, Context).

-spec put_mapping( Index, Doc, Context ) -> Result when
    Index :: index(),
    Doc :: map(),
    Context :: z:context(),
    Result :: result().
put_mapping(Index, Mapping, Context) ->
    elasticsearch2_fetch:request(connection(Context), put, [ Index, <<"_mapping">> ], [], Mapping).

%% @doc Save a Zotonic resource to Elasticsearch
-spec put_doc( RscId, Context ) -> Result when
    RscId :: m_rsc:resource(),
    Context :: z:context(),
    Result :: result().
put_doc(RscId, Context) ->
    put_doc(index(Context), RscId, Context).

%% @doc Put a Zotonic resource in the index.
-spec put_doc( Index, RscId, Context ) -> Result when
    Index :: index(),
    RscId :: m_rsc:resource(),
    Context :: z:context(),
    Result :: result().
put_doc(Index, RscId, Context) ->
    case m_rsc:rid(RscId, Context) of
        undefined ->
            {error, enoent};
        RId ->
            case m_rsc:exists(RId, Context) of
                true ->
                    % All resource properties
                    Props = elasticsearch2_mapping:map_rsc(RId, Context),
                    put_doc(Index, RId, Props, Context);
                false ->
                    {error, enoent}
            end
    end.

%% @doc Put a document in the index.
-spec put_doc( Index, DocId, Doc, Context ) -> Result when
    Index :: index(),
    DocId :: doc_id(),
    Doc :: map() | binary(),
    Context :: z:context(),
    Result :: result().
put_doc(Index, DocId, Data, Context) ->
    Type = maps:get(<<"es_type">>, Data, maps:get(es_type, Data, <<>>)),
    Data1 = z_notifier:foldl(#elasticsearch_put{ index = Index, type = Type, id = DocId }, Data, Context),
    elasticsearch2_fetch:index(connection(Context), Index, DocId, Data1).

%% @doc Map a resource put to a elasticsearch bulk command.
-spec put_bulkcmd(RscId, Context) -> Result when
    RscId :: m_rsc:resource(),
    Context :: z:context(),
    Result :: {ok, bulkcmd()} | {error, enoent}.
put_bulkcmd(RscId, Context) ->
    put_bulkcmd(index(Context), RscId, Context).

%% @doc Map a resource put to a elasticsearch bulk command.
-spec put_bulkcmd(Index, RscId, Context) -> Result when
    Index :: index(),
    RscId :: m_rsc:resource(),
    Context :: z:context(),
    Result :: {ok, bulkcmd()} | {error, enoent}.
put_bulkcmd(Index, RscId, Context) ->
    case m_rsc:rid(RscId, Context) of
        undefined ->
            {error, enoent};
        RId ->
            case m_rsc:exists(RId, Context) of
                true ->
                    % All resource properties
                    Doc = elasticsearch2_mapping:map_rsc(RId, Context),
                    Type = maps:get(<<"es_type">>, Doc, maps:get(es_type, Doc, <<>>)),
                    Doc1 = z_notifier:foldl(#elasticsearch_put{ index = Index, type = Type, id = RId }, Doc, Context),
                    {ok, {index, Index, RId, Doc1}};
                false ->
                    {error, enoent}
            end
    end.

-spec put_bulkcmd(Index, DocId, Doc, Context) -> Result when
    Index :: index(),
    DocId :: doc_id(),
    Doc :: map() | binary(),
    Context :: z:context(),
    Result :: {ok, bulkcmd()}.
put_bulkcmd(Index, DocId, Doc, Context) ->
    Type = maps:get(<<"es_type">>, Doc, maps:get(es_type, Doc, <<>>)),
    Doc1 = z_notifier:foldl(#elasticsearch_put{ index = Index, type = Type, id = DocId }, Doc, Context),
    {ok, {index, Index, DocId, Doc1}}.

%% @doc Map a resource delete to a bulk command on the default index.
-spec delete_bulkcmd(DocId, Context) -> {ok, bulkcmd()} when
    DocId :: doc_id(),
    Context :: z:context().
delete_bulkcmd(DocId, Context) ->
    delete_bulkcmd(index(Context), DocId, Context).

%% @doc Map a delete to a bulk command.
-spec delete_bulkcmd(Index, DocId, Context) -> {ok, bulkcmd()} when
    Index :: index(),
    DocId :: doc_id(),
    Context :: z:context().
delete_bulkcmd(Index, DocId, _Context) ->
    {ok, {delete, Index, DocId}}.


%% @doc Delete a document from the default index.
-spec delete_doc( DocId, Context ) -> Result when
    DocId :: doc_id(),
    Context :: z:context(),
    Result :: result().
delete_doc(DocId, Context) ->
    delete_doc(index(Context), DocId, Context).

%% @doc Delete a document from the index.
-spec delete_doc(Index, DocId, Context ) -> Result when
    Index :: index(),
    DocId :: doc_id(),
    Context :: z:context(),
    Result :: result().
delete_doc(Index, DocId, Context) ->
    elasticsearch2_fetch:delete(connection(Context), Index, DocId).

%% @doc Bulk insert/delete operation.
-spec bulk(Commands, Context) -> Result when
    Commands :: [ bulkcmd() ],
    Context :: z:context(),
    Result :: result().
bulk(Commands, Context) ->
    Connection = connection(Context),
    elasticsearch2_fetch:bulk(Connection, Commands).


%% @doc Using the scrolling API, loop over all documents in an index.
%% The Accumulator is passed to the function, and the end-accumulator
%% is returned.
-spec fold(Index, Fun, Accumulator, Context) -> Result when
    Index :: index(),
    Fun :: fun( (Doc, Accumulator1, Context) -> any() )
         | fun( (Doc, Context) -> any() ),
    Doc :: map(),
    Context :: any(),
    Result :: {ok, Accumulator2} | {error, term()},
    Accumulator :: any(),
    Accumulator1 :: any(),
    Accumulator2 :: any().
fold(Index, Fun, Acc, Context) ->
    Query = #{
        <<"size">> => 100,
        <<"sort">> => [
            <<"_doc">>
        ]
    },
    fold(Index, Query, Fun, Acc, Context).

-spec fold(Index, Query, Fun, Accumulator, Context) -> Result when
    Index :: index(),
    Query :: map(),
    Fun :: fun( (Doc, Accumulator1, Context) -> any() )
         | fun( (Doc, Context) -> any() ),
    Doc :: map(),
    Context :: any(),
    Result :: {ok, Accumulator2} | {error, term()},
    Accumulator :: any(),
    Accumulator1 :: any(),
    Accumulator2 :: any().
fold(Index, Query, Fun, Acc, Context) ->
    Result = search_scroll(Index, Query, <<"1h">>, Context),
    fold_loop(Result, Fun, Acc, Context).

fold_loop({ok, #{
        <<"hits">> := #{
            <<"hits">> := []
        }
    }}, _Fun, Acc, _Context) ->
    {ok, Acc};
fold_loop({ok, #{
        <<"_scroll_id">> := ScrollId,
        <<"hits">> := #{
            <<"hits">> := Docs,
            <<"total">> := _Total
        }
    }}, Fun, Acc, Context) ->
    % Handle Docs
    lager:debug("fetched ~p docs", [ length(Docs) ]),
    Acc1 = lists:foldl(
        fun(Doc, DocAcc) ->
            if
                is_function(Fun, 2) ->
                    Fun(Doc, Context),
                    DocAcc;
                is_function(Fun, 3) ->
                    Fun(Doc, DocAcc, Context)
            end
        end,
        Acc,
        Docs),
    Query = #{
        <<"scroll">> => <<"10m">>,
        <<"scroll_id">> => ScrollId
    },
    Next = search_scroll_next(Query, Context),
    fold_loop(Next, Fun, Acc1, Context);
fold_loop({ok, Other}, _Fun, _Acc, _Context) ->
    lager:warning("Scroll returned unexpected: ~p", [ Other ]),
    {error, Other};
fold_loop({error, Reason} = Error, _Fun, _Acc, _Context) ->
    lager:error("Scroll returned error: ~p", [ Reason ]),
    Error.

-spec search_scroll(binary(), map(), string()|binary(), z:context()) -> {ok, map()} | {error, term()}.
search_scroll(Index, Query, Timeout, Context) ->
    lager:debug("mod_elasticsearch2 (~s): ~s", [ Index, Query ]),
    % io:format("~p:~p: (~s) ~n~p~n~n", [ ?MODULE, ?LINE, Index, ElasticQuery2 ]),
    Connection = elasticsearch2:connection(Context),
    QArgs = [ {<<"scroll">>, z_convert:to_binary(Timeout)} ],
    elasticsearch2_fetch:request(Connection, post, [ Index, <<"_search">> ], QArgs, Query).

search_scroll_next(Query, Context) ->
    Connection = elasticsearch2:connection(Context),
    elasticsearch2_fetch:request(Connection, post, [ <<"_search">>, <<"scroll">> ], [], Query).
