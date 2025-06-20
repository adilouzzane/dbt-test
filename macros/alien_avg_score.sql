{% macro avg_score(strength_level, speed_level, intelligence, decimal_places=2) %}

    round(
            (
                cast(strength_level as decimal) +
                cast(speed_level as decimal) +
                cast(intelligence as decimal)
            ) / 3,
            {{ decimal_places }}
        )

{% endmacro %}