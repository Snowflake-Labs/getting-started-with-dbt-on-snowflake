{% macro create_schema_if_not_exists(schema_name) %}
  {% set sql %}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
  {% endset %}
  
  {% do run_query(sql) %}
  {% do log("Schema " ~ schema_name ~ " created or already exists", info=True) %}
{% endmacro %}
