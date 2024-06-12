WITH pred AS (
    SELECT 
        last_datetime_used, 
        CASE 
            WHEN DAYOFWEEK(last_datetime_used) = 6 THEN DATE_ADD(last_datetime_used, INTERVAL 3 DAY)
            ELSE DATE_ADD(last_datetime_used, INTERVAL 1 DAY)
        END AS predicted_date,
        AVG(CASE 
                WHEN next_value_predicted LIKE '[%' THEN CAST(REPLACE(REPLACE(next_value_predicted, '[', ''), ']', '') AS DECIMAL(20, 10))
                ELSE CAST(next_value_predicted AS DECIMAL(20, 10))
            END) AS predicts_avg
    FROM `output`.TTFM1_D1_PREDICTIONS
    GROUP BY last_datetime_used
),
stock AS (
    SELECT 
        `Date`, 
        `Open`,
        LAG(`Open`) OVER (ORDER BY `Date`) AS open_d_1
    FROM `input`.T_FACT_STOCK_DATA_TTFM1
),
combined AS (
    SELECT 
        pred.*,
        stock.`Open`,
        stock.open_d_1,
        CASE 
            WHEN ABS(predicts_avg - `Open`) > 3 AND predicts_avg - `Open` > 0 THEN 'BUY'
            WHEN ABS(predicts_avg - `Open`) > 3 AND predicts_avg - `Open` < 0 THEN 'SELL'
        END AS prediction
    FROM pred 
    LEFT JOIN stock ON pred.last_datetime_used = stock.`Date`
),
final AS (
    SELECT 
        combined.*,
        LEAD(CASE 
                 WHEN combined.`Open` > combined.open_d_1 THEN 'BUY'
                 WHEN combined.`Open` < combined.open_d_1 THEN  'SELL'
             END) OVER (ORDER BY combined.last_datetime_used) AS `open_vs_open_d+1`
    FROM combined
)
SELECT 
    final.last_datetime_used,
    final.predicted_date,
    final.predicts_avg,
    final.`Open`,
    final.open_d_1,
    final.prediction,
    final.`open_vs_open_d+1`,
    CONCAT(
        FORMAT(
            (SELECT SUM(CASE WHEN `open_vs_open_d+1` = prediction THEN 1 ELSE 0 END) * 100.0 / COUNT(*) 
             FROM final 
             WHERE `open_vs_open_d+1` IS NOT NULL AND prediction IS NOT NULL),
            2
        ), 
        '%'
    ) AS accuracy_overall,
    CAST(
        (SELECT COUNT(prediction) 
         FROM final 
         WHERE prediction IS NOT NULL) AS SIGNED
    ) AS predict_n,
    CAST(
        (SELECT COUNT(`open_vs_open_d+1`) 
         FROM final 
         WHERE `open_vs_open_d+1` IS NOT NULL) AS SIGNED
    ) AS observation_n
FROM final
ORDER BY predicted_date DESC;

