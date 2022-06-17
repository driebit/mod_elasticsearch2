Updating to ES8
---------------

ES8 deleted the `type` from the indices.

This `type` was used extensively and also part of the library API

Documentation what needs to be changed:

https://www.elastic.co/guide/en/elasticsearch/reference/6.8/removal-of-types.html


Library fork that deletes the type from the API:

https://github.com/hiroeorz/erlastic_search/tree/type_delete


Changes needed:

elasticsearch2_mapping:map_rsc
    - add `es_type => <<"resource">>` field


put_mapping
    - needs an union of all fields for all types


Replace references to `_type` with `type`


