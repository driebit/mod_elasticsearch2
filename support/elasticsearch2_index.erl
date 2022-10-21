%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc Manages Elasticsearch indexes and index mappings
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

-module(elasticsearch2_index).

-export([
    delete_recreate/4,
    upgrade/4,
    ensure_index/2,
    update_mapping/3,
    find_aliased_index/2,
    copy_to_new_index/3,
    drop_index_hash/1
]).

%% @doc Delete the current index and recreate a new empty index.
-spec delete_recreate(Index, Mapping, Hash, Context) -> Result when
    Index :: elasticsearch2:index(),
    Mapping :: map(),
    Hash :: binary(),
    Context :: z:context(),
    Result :: elasticsearch2:result().
delete_recreate(Index, Mapping, Version, Context) when is_map(Mapping) ->
    VersionedIndex = versioned_index(Index, Version),
    case index_exists(VersionedIndex, Context) of
        true ->
            delete_index(VersionedIndex, Context);
        false ->
            ok
    end,
    upgrade(Index, Mapping, Version, Context).


%% @doc Upgrade mappings for an index
%%      This:
%%      1. creates the new index
%%      2. creates the new mappings in it
%%      3. reindex the new index with data from the old one
%%      4. updates the alias so it points to the new index.
-spec upgrade(Index, Mapping, Hash, Context) -> Result when
    Index :: elasticsearch2:index(),
    Mapping :: map(),
    Hash :: binary(),
    Context :: z:context(),
    Result :: elasticsearch2:result().
upgrade(Index0, Mapping, Version, Context) ->
    Index = z_convert:to_binary(Index0),
    VersionedIndex = versioned_index(Index, Version),
    case index_exists(VersionedIndex, Context) of
        true ->
            {ok, #{}};
        false ->
            {ok, _} = create_index(VersionedIndex, Mapping, Context),
            copy_to_new_index(Index, VersionedIndex, Context)
    end.

copy_to_new_index(Index, VersionedIndex, Context) ->
    lager:info("mod_elasticsearch2: moving data from ~s to ~s", [ Index, VersionedIndex ]),
    PrevVersionedIndex = find_aliased_index(Index, Context),
    maybe_reindex(Index, VersionedIndex, Context),
    Result = update_alias(Index, VersionedIndex, PrevVersionedIndex, Context),
    delete_index(PrevVersionedIndex, Context),
    Result.

%% Create index only if it doesn't yet exist
-spec ensure_index(elasticsearch2:index(), z:context()) -> elasticsearch2:result().
ensure_index(Index, Context) ->
    case index_exists(Index, Context) of
        true ->
            {ok, #{}};
        false ->
            create_index(Index, Context)
    end.

%% @doc Update the mapping of an Elasticsearch index.
-spec update_mapping(Index, Mapping, Context) -> Result when
    Index :: elasticsearch2:index(),
    Mapping :: map(),
    Context :: z:context(),
    Result :: elasticsearch2:result().
update_mapping(Index, Mapping, Context) ->
    lager:info("mod_elasticsearch2: updating index ~s with mapping", [Index]),
    Connection = elasticsearch2:connection(Context),
    elasticsearch2_fetch:request(Connection, put, [ Index, <<"_mapping">> ], [], Mapping).


%% @doc Populate the new index with data from the previous index, but only if
%%      that previous index exists.
-spec maybe_reindex(Alias, NewIndex, Context) -> Result when
    Alias :: elasticsearch2:index(),
    NewIndex :: elasticsearch2:index(),
    Context :: z:context(),
    Result :: elasticsearch2:result().
maybe_reindex(Alias, NewIndex, Context) ->
    case index_exists(Alias, Context) of
        false ->
            %% No alias yet, so no reindex necessary
            {ok, #{}};
        true ->
            %% Previous version of (aliased) index exists, so reindex from that
            Body = #{
                <<"source">> => #{
                    <<"index">> => Alias
                },
                <<"dest">> => #{
                    <<"index">> => NewIndex
                },
                <<"conflicts">> => <<"proceed">>
            },
            Connection = elasticsearch2:connection(Context),
            elasticsearch2_fetch:request(Connection, post, [ <<"_reindex">> ], [], Body)
    end.

%% @doc Point index alias (e.g. sitename) to versioned index (e.g. sitename_af578ebcdf)
-spec update_alias(Alias, VersionedIndex, PrevVersionedIndex, Context) -> Result when
    Alias :: elasticsearch2:index(),
    VersionedIndex :: elasticsearch2:index(),
    PrevVersionedIndex :: elasticsearch2:index() | undefined,
    Context :: z:context(),
    Result :: elasticsearch2:result().
update_alias(Alias, VersionedIndex, undefined, Context) ->
    Body = #{
        <<"actions">> => [
            #{
                <<"add">> => #{
                    <<"index">> => VersionedIndex,
                    <<"alias">> => Alias
                }
            }
        ]
    },
    Connection = elasticsearch2:connection(Context),
    elasticsearch2_fetch:request(Connection, post, [ <<"_aliases">> ], [], Body);
update_alias(Alias, VersionedIndex, PrevVersionedIndex, Context) ->
    Body = #{
        <<"actions">> => [
            #{
                <<"remove">> => #{
                    <<"index">> => PrevVersionedIndex,
                    <<"alias">> => Alias
                }
            },
            #{
                <<"add">> => #{
                    <<"index">> => VersionedIndex,
                    <<"alias">> => Alias
                }
            }
        ]
    },
    Connection = elasticsearch2:connection(Context),
    elasticsearch2_fetch:request(Connection, post, [ <<"_aliases">> ], [], Body).

%% @doc Find the index pointed to by the alias.
find_aliased_index(Alias, Context) ->
    Connection = elasticsearch2:connection(Context),
    {ok, As} = elasticsearch2_fetch:request(Connection, get, [ <<"_aliases">> ], [], <<>>),
    find_alias(Alias, maps:to_list(As)).

find_alias(_Alias, []) ->
    undefined;
find_alias(Alias, [ {<<".", _/binary>>, _} | As ]) ->
    find_alias(Alias, As);
find_alias(Alias, [ {Index, #{ <<"aliases">> := Aliases }} | As ]) ->
    case maps:is_key(Alias, Aliases) of
        true -> Index;
        false -> find_alias(Alias, As)
    end.

%% @doc Get name of index appended with version number
versioned_index(Index, Version) ->
    <<Index/binary, "_", Version/binary>>.

%% @doc Create Elasticsearch index without a mapping.
-spec create_index(Index, Context) -> Result when
    Index :: elasticsearch2:index(),
    Context :: z:context(),
    Result :: elasticsearch2:result().
create_index(Index, Context) ->
    lager:info("mod_elasticsearch2: creating index ~s", [Index]),
    Connection = elasticsearch2:connection(Context),
    elasticsearch2_fetch:request(Connection, put, [ Index ], [], #{}).

%% @doc Create Elasticsearch index with an initial mapping.
-spec create_index(Index, Mapping, Context) -> Result when
    Index :: elasticsearch2:index(),
    Mapping :: map(),
    Context :: z:context(),
    Result :: elasticsearch2:result().
create_index(Index, Mapping, Context) ->
    lager:info("mod_elasticsearch2: creating index ~s", [Index]),
    Connection = elasticsearch2:connection(Context),
    Body = #{
        <<"mappings">> => Mapping
    },
    elasticsearch2_fetch:request(Connection, put, [ Index ], [], Body).

%% @doc Delete Elasticsearch index.
-spec delete_index(Index, Context) -> Result when
    Index :: undefined | elasticsearch2:index(),
    Context :: z:context(),
    Result :: elasticsearch2:result().
delete_index(undefined, _Context) ->
    {ok, #{}};
delete_index(Index, Context) ->
    lager:info("mod_elasticsearch2: deleting index ~s", [Index]),
    Connection = elasticsearch2:connection(Context),
    elasticsearch2_fetch:delete_index(Connection, Index).

%% @doc Check if index exists.
-spec index_exists(binary(), z:context()) -> boolean().
index_exists(Index, Context) ->
    url_exists([ Index ], Context).

url_exists(Path, Context) ->
    Connection = elasticsearch2:connection(Context),
    case elasticsearch2_fetch:request(Connection, head, Path, [], <<>>) of
        {ok, _} ->
            true;
        {error, _} ->
            false
    end.

%% @doc Check the index name, optionally drop the hash of the mapping from
%% the index name.
-spec drop_index_hash(Index) -> Index1 when
    Index :: binary(),
    Index1 :: binary().
drop_index_hash(Index) ->
    case binary:split(Index, <<"_">>, [ global ]) of
        [_] ->
            Index;
        Parts ->
            Hash = lists:last(Parts),
            case is_hash(Hash) of
                true ->
                    I1 = lists:reverse(tl(lists:reverse(Parts))),
                    iolist_to_binary(lists:join($_, I1));
                false ->
                    Index
            end
    end.

is_hash(Hash) when size(Hash) =:= 40 ->
    L = binary_to_list(Hash),
    lists:all(
        fun(C) ->
                   (C >= $0 andalso C =< $9)
            orelse (C >= $a andalso C =< $f)
        end,
        L);
is_hash(_) ->
    false.
