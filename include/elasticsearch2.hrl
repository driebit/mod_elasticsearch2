-record(elasticsearch_put, {
    index :: elasticsearch2:index(),
    type = <<>> :: binary(),   %% WILL BE REMOVED - for compat with older mod_elasticsearch.
    id :: elasticsearch2:doc_id()
}).

-record(elasticsearch_fields, {
    query :: binary() | map()
}).

%% @doc Search options
%%      fallback: whether to fall back to PostgreSQL for non full-text searches.
-record(elasticsearch_options, {
    fallback = true :: boolean()
}).
