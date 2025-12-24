WITH latest_per_gu AS (
    -- 인덱스 활용: (GU_CD, DATA_CLCT_TM DESC)
    SELECT 
        gu_cd,
        MAX(data_clct_tm) AS max_clct_tm
    FROM {{ source('raw_data', 'list_rainfall_service') }}
    GROUP BY gu_cd
),
ranked_data AS (
    SELECT
        s.rf_cd,
        s.rf_nm,
        s.gu_cd,
        s.gu_nm,
        s.rn_10m,
        s.data_clct_tm,
        s.updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY s.gu_cd
            ORDER BY
                s.data_clct_tm DESC,    -- 1순위: 각 구별로 가장 최신 시간 데이터를 선택
                s.rn_10m DESC,          -- 2순위: 같은 시간이라면 강우량이 높은 것 선택
                s.rf_cd ASC             -- 3순위: 나머지 경우 코드 순
        ) AS rn
    FROM {{ ref('stg_list_rainfall_service') }} s
    INNER JOIN latest_per_gu l 
        ON s.gu_cd = l.gu_cd 
        AND s.data_clct_tm = l.max_clct_tm
)
SELECT
    rf_cd,          -- 강우량계 코드
    rf_nm,          -- 강우량계명
    gu_cd,          -- 구청 코드
    gu_nm,          -- 구청명
    rn_10m,         -- 10분우량
    data_clct_tm,   -- 자료수집 시각
    updated_at
FROM ranked_data
WHERE rn = 1