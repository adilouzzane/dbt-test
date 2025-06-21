{% macro avg_score(strength_level, speed_level, intelligence, decimal_places=2) %}

    CAST(
        round(
                (
                    cast(strength_level as decimal) +
                    cast(speed_level as decimal) +
                    cast(intelligence as decimal)
                )::numeric / 3.0,
                {{ decimal_places }}
            )
        AS NUMERIC(10, 2)
    )

{% endmacro %}