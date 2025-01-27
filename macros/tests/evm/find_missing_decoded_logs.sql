{% test find_missing_decoded_logs(model, fact_logs_model, wrapped_asset_address) %}
SELECT
    l.block_number,
    l.fact_event_logs_id
FROM
    {{ fact_logs_model }}
    l
    LEFT JOIN {{ model }}
    d
    ON d.ez_decoded_event_logs_id = l.fact_event_logs_id
WHERE
    l.contract_address = '{{ wrapped_asset_address }}'
    AND l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer
    AND d.ez_decoded_event_logs_id IS NULL
{% endtest %}