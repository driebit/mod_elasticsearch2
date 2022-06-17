%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc Main interface functions for fetching and indexing documents with
%% Elasticsearch.
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

-module(elasticsearch2).
-author("Driebit <tech@driebit.nl>").

-export([
    connection/1,
    index/1,
    get_doc/2,
    get_doc/3,
    put_doc/2,
    put_doc/3,
    put_doc/4,
    put_mapping/2,
    put_mapping/3,
    delete_doc/2,
    delete_doc/3
]).

-type connection() :: #{ host := binary(), port := non_neg_integer() }.
-type doc_id() :: binary() | string() | integer().
-type index() :: binary() | string().
-type bulkcmd() :: {index, Index::binary(), DocId::binary(), Doc::map()}
                 | {delete, Index::binary(), DocId::binary()}.
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

%% @doc Get Elasticsearch index name from config; defaults to site name
-spec index(Context) -> Index when
    Context :: z:context(),
    Index :: index().
index(Context) ->
    SiteName = z_context:site(Context),
    IndexName = case m_config:get_value(mod_elasticsearch2, index, Context) of
        undefined -> SiteName;
        <<>> -> SiteName;
        Value -> Value
    end,
    z_convert:to_binary(IndexName).

%% Fetch a single document by type and Id
-spec get_doc( DocId, Context ) -> Result when
    DocId :: doc_id(),
    Context :: z:context(),
    Result :: result().
get_doc(DocId, Context) ->
    get_doc(index(Context), DocId, Context).


%% Fetch a single document by type and Id
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
            Doc#{
                <<"_type">> => maps:get(<<"es_type">>, Source, <<>>)
            };
        {ok, Doc} ->
            Doc;
        {error, _} = Error ->
            Error
    end.


%% Create Elasticsearch mapping
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

%% Save a resource to Elasticsearch
-spec put_doc( RscId, Context ) -> Result when
    RscId :: m_rsc:resource(),
    Context :: z:context(),
    Result :: result().
put_doc(RscId, Context) ->
    put_doc(index(Context), RscId, Context).

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

-spec delete_doc( DocId, Context ) -> Result when
    DocId :: doc_id(),
    Context :: z:context(),
    Result :: result().
delete_doc(DocId, Context) ->
    delete_doc(index(Context), DocId, Context).

-spec delete_doc(Index, DocId, Context ) -> Result when
    Index :: index(),
    DocId :: doc_id(),
    Context :: z:context(),
    Result :: result().
delete_doc(Index, DocId, Context) ->
    elasticsearch2_fetch:delete(connection(Context), Index, DocId).

