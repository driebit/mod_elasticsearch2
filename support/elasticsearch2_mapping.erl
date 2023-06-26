%% @author Driebit <tech@driebit.nl>
%% @copyright 2022-2023 Driebit BV
%% @doc Map resources to Elastic Search documents.
%% @end

%% Copyright 2022-2023 Driebit BV
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

-module(elasticsearch2_mapping).
-author("Driebit <tech@driebit.nl>").

-include("zotonic.hrl").

-export([
    default_mapping/2,
    hash/1,
    map_rsc/2,
    dynamic_language_mapping/1,
    default_translation/2
]).


%% @doc Return the default Elasticsearch mapping for Zotonic resources.
-spec default_mapping(MappingType, Context) -> {Hash, Mapping} when
    MappingType :: atom(),
    Context :: z:context(),
    Hash :: binary(),
    Mapping :: map().
default_mapping(resource, Context) ->
    Mapping = #{
        <<"properties">> => #{
            <<"es_type">> => #{
                <<"type">> => <<"keyword">>
            },
            <<"geolocation">> => #{
                <<"type">> => <<"geo_point">>
            },
            <<"blocks">> => #{
                <<"type">> => <<"nested">>
            },
            <<"incoming_edges">> => #{
                <<"type">> => <<"nested">>
            },
            <<"outgoing_edges">> => #{
                <<"type">> => <<"nested">>
            },
            <<"date_start">> => #{
                <<"type">> => <<"date">>
            },
            <<"date_end">> => #{
                <<"type">> => <<"date">>
            },
            <<"suggest">> => #{
                <<"type">> => <<"completion">>,
                <<"analyzer">> =>  <<"simple">>
            },
            <<"pivot_title">> => #{
                <<"type">> => <<"keyword">>
            }
        },
        <<"dynamic_templates">> => [
            #{<<"strings">> => #{
                <<"match_mapping_type">> => <<"string">>,
                <<"match">> => <<"title*">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>
                }
            }},
            #{<<"strings">> => #{
                <<"match_mapping_type">> => <<"string">>,
                <<"match">> => <<"short_title*">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>
                }
            }},
            #{<<"strings">> => #{
                <<"match_mapping_type">> => <<"string">>,
                <<"match">> => <<"dc:title*">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>
                }
            }},
            #{<<"strings">> => #{
                <<"match_mapping_type">> => <<"string">>,
                <<"match">> => <<"dcterms:title*">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>
                }
            }},
            #{<<"strings">> => #{
                <<"match_mapping_type">> => <<"string">>,
                <<"match">> => <<"schema:title*">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>
                }
            }},
            #{<<"strings">> => #{
                <<"match_mapping_type">> => <<"string">>,
                <<"match">> => <<"body*">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>
                }
            }},
            #{<<"strings">> => #{
                <<"match_mapping_type">> => <<"string">>,
                <<"match">> => <<"summary*">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>
                }
            }}
            | dynamic_language_mapping(Context)
        ]
    },
    {hash(Mapping), Mapping}.

%% @doc Generate unique SHA1-based hash for a mapping
-spec hash(map()) -> binary().
hash(Map) ->
    z_string:to_lower(z_utils:hex_encode(crypto:hash(sha, jsx:encode(Map)))).


%% @doc Map a Zotonic resource to an Elastic Search document.
-spec map_rsc(m_rsc:resource_id(), z:context()) -> map().
map_rsc(Id, Context) ->
    RscProps = m_rsc:get(Id, Context),
    Props = maps:merge(
        map_location(Id, Context),
        map_properties(RscProps)
    ),
    Props#{
        es_type => <<"resource">>,
        category => m_rsc:is_a(Id, Context),
        pivot_title => default_translation(proplists:get_value(title, RscProps), Context),
        pivot_rtsv => map_rtsv(m_edge:objects(Id, Context)),
        incoming_edges => incoming_edges(Id, Context),
        outgoing_edges => outgoing_edges(Id, Context)
    }.

map_rtsv(ObjectIds) ->
    lists:map(
        fun(Id) ->
            <<" zpo", (integer_to_binary(Id))/binary>>
        end,
        ObjectIds).

%% @doc Get dynamic language mappings, based on available languages.
-spec dynamic_language_mapping(z:context()) -> list( map() ).
dynamic_language_mapping(Context) ->
    lists:map(
        fun({LangCode, _}) ->
            #{
                LangCode => #{
                    <<"match">> => <<"*_", (z_convert:to_binary(LangCode))/binary>>,
                    <<"match_mapping_type">> => <<"string">>,
                    <<"mapping">> => #{
                        <<"type">> => <<"text">>,
                        <<"analyzer">> => get_analyzer(LangCode)
                    }
                }
            }
        end,
        m_translation:language_list_enabled(Context)).

%% @doc Get default translation for a property.
%%      Default is the (global) site language, not the Context's language.
-spec default_translation(Text, Context) -> TextOut when
    Text :: {trans, list({atom(), binary()})}
          | binary()
          | undefined,
    Context :: z:context(),
    TextOut :: binary().
default_translation({trans, _} = Text, Context) ->
    z_trans:lookup_fallback(Text, z_trans:default_language(Context), Context);
default_translation(undefined, _Context) ->
    <<>>;
default_translation(Text, _Context) ->
    Text.


%% @doc Map the resource location to a ES compatible geolocation. Suppress incomplete locations.
-spec map_location(m_rsc:resource_id(), z:context()) -> map().
map_location(Id, Context) ->
    case {m_rsc:p_no_acl(Id, pivot_location_lat, Context), m_rsc:p_no_acl(Id, pivot_location_lng, Context)} of
        {_, undefined} ->
            #{};
        {undefined, _} ->
            #{};
        {Lat, Lng} ->
            try
                #{
                    geolocation => #{
                        lat => z_convert:to_float(Lat),
                        lon => z_convert:to_float(Lng)
                    }
                }
            catch
                _:_ ->
                    #{}
            end
    end.

-spec map_properties(proplists:proplist()) -> map().
map_properties(Properties) ->
    lists:foldl(
        fun(Prop, Acc) ->
            map_property(Prop, Acc)
        end,
        #{},
        without_ignored(Properties)).

without_ignored(Properties) ->
    lists:filter(
        fun({Key, _Value}) ->
            not lists:member(Key, ignored_properties())
        end,
        Properties
    ).

-spec map_property(tuple(), map()) -> map().
map_property({Key, {trans, Translations}}, Acc) ->
    lists:foldl(
        fun({LangCode, Translation}, LangAcc) ->
            %% Change keys to contain language code: title_en etc.
            LangKey = get_language_property(Key, LangCode),

            % Strip HTML tags from translated props. This is
            % only needed for body, but won't hurt the other
            % translated props.
            LangAcc#{
                LangKey => z_html:strip(Translation)
            }
        end,
        Acc,
        Translations
    );
map_property({blocks, Blocks}, Acc) when is_list(Blocks) ->
    Blocks1 = lists:map(fun map_properties/1, Blocks),
    Acc#{ blocks => Blocks1 };
map_property({blocks, _}, Acc) ->
    Acc;
map_property({Key, Value}, Acc) ->
    Acc#{ Key => map_value(Value) }.

% Make Zotonic resource value Elasticsearch-friendly
map_value({{Y, M, D}, {H, I, S}} = DateTime) when
    is_integer(Y), is_integer(M), is_integer(D),
    is_integer(H), is_integer(I), is_integer(S) ->
    case date_to_iso8601(DateTime) of
        undefined ->
            %% e.g., ?ST_JUTTEMIS or invalid dates
            null;
        Value ->
            z_convert:to_binary(Value)
    end;
map_value({{_, _, _}, {_, _, _}}) ->
    %% Invalid date
    null;
map_value(undefined) ->
    null;
map_value(<<>>) ->
    %% Map empty string to null as empty strings interfere with ordering the search results.
    null;
map_value(Value) when is_binary(Value); is_number(Value); is_boolean(Value); is_list(Value) ->
    Value;
map_value(_) ->
    null.

incoming_edges(Id, Context) ->
    lists:flatmap(
        fun(Predicate) ->
            lists:map(fun map_edge/1, m_edge:subject_edge_props(Id, Predicate, Context))
        end,
        m_edge:subject_predicates(Id, Context)
    ).

outgoing_edges(Id, Context) ->
    lists:flatmap(
        fun(Predicate) ->
            lists:map(fun map_edge/1, m_edge:object_edge_props(Id, Predicate, Context))
        end,
        m_edge:object_predicates(Id, Context)
    ).

map_edge(Edge) ->
    #{
        predicate_id => proplists:get_value(predicate_id, Edge),
        subject_id => proplists:get_value(subject_id, Edge),
        object_id => proplists:get_value(object_id, Edge),
        created => map_value(proplists:get_value(created, Edge))
    }.


%% Get analyzer for a language, depending on which languages are supported by
%% Elasticsearch
get_analyzer(LangCode) ->
    Language = z_string:to_lower(iso639:lc2lang(z_convert:to_list(LangCode))),
    case is_supported_lang(Language) of
        true  -> Language;
        false -> <<"standard">>
    end.

%% Get property in the form title_en, body_nl etc.
-spec get_language_property(Prop::atom(), Language::atom()) -> binary().
get_language_property(Prop, Lang) ->
    iolist_to_binary([ z_convert:to_binary(Prop), $_, z_convert:to_binary(Lang) ]).

%% Does Elasticsearch ship an analyzer for the language?
-spec is_supported_lang(binary()) -> boolean().
is_supported_lang(Language) ->
    lists:member(Language, [
        <<"arabic">>,
        <<"armenian">>,
        <<"basque">>,
        <<"brazilian">>,
        <<"bulgarian">>,
        <<"catalan">>,
        <<"chinese">>,
        <<"czech">>,
        <<"danish">>,
        <<"dutch">>,
        <<"english">>,
        <<"finnish">>,
        <<"french">>,
        <<"galician">>,
        <<"german">>,
        <<"greek">>,
        <<"hindi">>,
        <<"hungarian">>,
        <<"indonesian">>,
        <<"irish">>,
        <<"italian">>,
        <<"japanese">>,
        <<"korean">>,
        <<"kurdish">>,
        <<"norwegian">>,
        <<"persian">>,
        <<"portuguese">>,
        <<"romanian">>,
        <<"russian">>,
        <<"spanish">>,
        <<"swedish">>,
        <<"turkish">>,
        <<"thai">>
    ]).

%% @doc Map date to ISO 8601 representation.
%%      Can be removed when https://github.com/zotonic/z_stdlib/pull/30 is merged
date_to_iso8601({{Y, _, _}, _} = DateTime) ->
    case z_datetime:undefined_if_invalid_date(DateTime) of
        undefined ->
            undefined;
        DateTime ->
            case z_dateformat:format(DateTime, "-m-d\\TH:i:s\\Z", en) of
                undefined ->
                    %% Can be undefined even if undefined_if_invalid_date is not.
                    undefined;
                Formatted ->
                    <<(year_to_iso8601(Y))/binary, Formatted/binary>>
            end
    end.

year_to_iso8601(Y) when Y < 0 ->
    iolist_to_binary(io_lib:format("-~4..0B", [abs(Y)]));
year_to_iso8601(Y) ->
    iolist_to_binary(io_lib:format("~4..0B", [Y])).


%% @doc Properties that don't need to be added to the search index.
-spec ignored_properties() -> [atom()].
ignored_properties() ->
    [
        crop_center,
        feature_show_address,
        feature_show_geodata,
        geocode_qhash,
        location_lng,
        location_lat,
        hasusergroup,
        installed_by,
        menu,
        pivot_geocode_qhash,
        pivot_location_lat,
        pivot_location_lng,
        managed_props,
        location_zoom_level,
        seo_noindex,
        tz,
        visible_for,
        %% possible custom props that will collide and we must therefore exclude:
        geolocation
    ].
