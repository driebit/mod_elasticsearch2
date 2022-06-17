-record(elasticsearch_put, {
    index :: elasticsearch2:index(),
    type = <<>> :: binary(),            % <<"resource">> for resources
    id :: elasticsearch2:doc_id()       % integer for resources, otherwise binary
}).

-record(elasticsearch_fields, {
    query :: binary() | map()
}).

%% @doc Search options
%%      fallback: whether to fall back to PostgreSQL for non full-text searches.
-record(elasticsearch_options, {
    fallback = true :: boolean()
}).
