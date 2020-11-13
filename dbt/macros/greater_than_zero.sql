{% macro test_greater_than_zero(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select count(*)
from (
    select
      {{ column_name }}    

    from {{ model }}

    where {{ column_name }} <= 0

) validation_errors

{% endmacro %}
