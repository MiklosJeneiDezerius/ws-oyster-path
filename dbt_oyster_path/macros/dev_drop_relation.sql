{# Development-only reset for protected relations (e.g. full_refresh=false ledgers).
   Usage: dbt run-operation dev_drop_relation --args '{schema: dev_bronze, identifier: bz_oys_transaction_ledger}' --target dev #}
{% macro dev_drop_relation(schema, identifier) %}
    {% if target.name != 'dev' %}
        {{ exceptions.raise_compiler_error("dev_drop_relation is allowed only on target 'dev', got '" ~ target.name ~ "'.") }}
    {% endif %}
    {% if not schema.startswith('dev_') %}
        {{ exceptions.raise_compiler_error("dev_drop_relation only drops relations in dev_* schemas, got '" ~ schema ~ "'.") }}
    {% endif %}
    {% set relation = adapter.get_relation(database=target.database, schema=schema, identifier=identifier) %}
    {% if relation is none %}
        {{ log("Relation " ~ schema ~ "." ~ identifier ~ " does not exist; nothing to drop.", info=true) }}
    {% else %}
        {% do adapter.drop_relation(relation) %}
        {{ log("Dropped " ~ relation, info=true) }}
    {% endif %}
{% endmacro %}
