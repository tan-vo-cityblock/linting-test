

{% macro create_event(source, subject, verb, timestamp,
                      direct_object={
                        'type': 'STRING(NULL)',
                        'key': 'STRING(NULL)',
                        'display': 'STRING(NULL)'},
                      indirect_object={
                        'type': 'STRING(NULL)',
                        'key': 'STRING(NULL)'},
                      prepositional_object={
                        'type': 'STRING(NULL)',
                        'key': 'STRING(NULL)'},
                      purposes=[{
                        'type': 'STRING(NULL)',
                        'key': 'STRING(NULL)'}],
                      outcomes=[{
                        'type': 'STRING(NULL)',
                        'key': 'STRING(NULL)'}]) %}

  {{ map_to_struct(source) }} as source,
  {{ map_to_struct(subject) }} as subject,
  {{ verb }} as verb,
  {{ map_to_struct(direct_object) }} as directObject,
  {{ map_to_struct(indirect_object) }} as indirectObject,
  {{ map_to_struct(prepositional_object) }} as prepositionalObject,
  {{ map_to_struct(timestamp) }} as timestamp,
  {{ list_to_struct_array(purposes) }} as purposes,
  {{ list_to_struct_array(outcomes) }} as outcomes

{% endmacro %}
