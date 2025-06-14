-- ===================================================================================
-- SCRIPT DE ANÁLISES AVANÇADAS DE DADOS DE OCUPAÇÃO HOSPITALAR
-- Autor: Miqueias
-- Dialeto SQL: PostgreSQL (com funções majoritariamente portáteis)
--
-- NOTA: Este script assume que a tabela de hospitais se chama 'hospital' e possui
-- as colunas 'id_hospital' e 'cnes'. Ajuste os JOINs se os nomes forem diferentes.
-- ===================================================================================


-- ===================================================================================
-- ANÁLISE 1: Média Móvel e Volatilidade da Ocupação de UTI
-- Objetivo: Avaliar a tendência de ocupação de leitos de UTI usando uma média móvel
-- de 7 dias e medir a volatilidade diária em relação a essa tendência.
-- ===================================================================================

WITH ocupacao_diaria_uti AS (
    -- Etapa 1: Agregar a ocupação total de UTI por dia.
    -- Esta CTE transforma os registros individuais em uma série temporal diária.
    SELECT
        CAST(data_notificacao AS DATE) AS dia,
        SUM(
            COALESCE(ocupacao_suspeito_uti, 0) +
            COALESCE(ocupacao_confirmado_uti, 0) +
            COALESCE(ocupacao_covid_uti, 0)
        ) AS total_leitos_uti
    FROM
        registro_ocupacao
    GROUP BY
        dia
)
-- Etapa 2: Calcular a média móvel e a variação em relação à média.
SELECT
    dia,
    total_leitos_uti,
    -- Média móvel de 7 dias (incluindo o dia atual) para suavizar a tendência.
    ROUND(AVG(total_leitos_uti) OVER (ORDER BY dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS media_movel_7d_uti,
    -- Variação percentual do dia atual em relação à média móvel.
    -- Isso indica se o dia está significativamente acima ou abaixo da tendência recente.
    ROUND(
        (total_leitos_uti::decimal / NULLIF(AVG(total_leitos_uti) OVER (ORDER BY dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) - 1) * 100.0, 2
    ) AS variacao_vs_media_movel_pct
FROM
    ocupacao_diaria_uti
ORDER BY
    dia;


-- ===================================================================================
-- ANÁLISE 2: Variação e Aceleração de Óbitos
-- Objetivo: Calcular a variação diária no número de óbitos e analisar a tendência
-- dessa variação (aceleração ou desaceleração).
-- ===================================================================================

WITH obitos_diarios AS (
    -- Etapa 1: Agregar o total de óbitos por dia.
    SELECT
        CAST(data_notificacao AS DATE) AS dia,
        SUM(COALESCE(saida_suspeita_obitos, 0) + COALESCE(saida_confirmada_obitos, 0)) AS total_obitos
    FROM
        registro_ocupacao
    GROUP BY
        dia
),
variacao_diaria_obitos AS (
    -- Etapa 2: Calcular a variação percentual diária usando LAG.
    SELECT
        dia,
        total_obitos,
        LAG(total_obitos, 1, 0) OVER (ORDER BY dia) AS obitos_dia_anterior,
        -- Cálculo da variação percentual, evitando divisão por zero.
        ROUND(
            ( (total_obitos - LAG(total_obitos, 1, 0) OVER (ORDER BY dia))::decimal / NULLIF(LAG(total_obitos, 1, 0) OVER (ORDER BY dia), 0) ) * 100.0, 2
        ) AS variacao_percentual
    FROM
        obitos_diarios
)
-- Etapa 3: Calcular a média móvel da variação para entender a aceleração.
SELECT
    dia,
    total_obitos,
    variacao_percentual,
    -- Média móvel da variação. Se > 0 e subindo, indica aceleração no crescimento.
    -- Se < 0 e caindo, indica aceleração na queda.
    ROUND(AVG(variacao_percentual) OVER (ORDER BY dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS media_movel_7d_variacao_pct
FROM
    variacao_diaria_obitos
ORDER BY
    dia;


-- ===================================================================================
-- ANÁLISE 3: Análise de Performance de Municípios com Percentil
-- Objetivo: Rankear os municípios pela média de altas médicas e classificá-los em
-- percentis para entender sua posição relativa no universo de dados.
-- ===================================================================================

WITH performance_municipios AS (
    -- Etapa 1: Calcular as métricas de performance por município.
    SELECT
        l.municipio,
        l.estado,
        AVG(COALESCE(ro.saida_suspeita_altas, 0) + COALESCE(ro.saida_confirmada_altas, 0)) AS media_diaria_altas,
        SUM(COALESCE(ro.saida_suspeita_altas, 0) + COALESCE(ro.saida_confirmada_altas, 0)) AS total_altas_periodo,
        COUNT(DISTINCT CAST(ro.data_notificacao AS DATE)) AS dias_com_registro
    FROM
        registro_ocupacao ro
    JOIN
        localidade l ON ro.id_local = l.id_local
    WHERE
        ro.data_notificacao BETWEEN '2022-01-01' AND '2022-03-31'
    GROUP BY
        l.municipio, l.estado
    HAVING
        -- Filtro de ruído: considera apenas municípios com média superior a 1 alta/dia.
        AVG(COALESCE(ro.saida_suspeita_altas, 0) + COALESCE(ro.saida_confirmada_altas, 0)) > 1
)
-- Etapa 2: Aplicar funções de ranking e percentil.
SELECT
    municipio,
    estado,
    ROUND(media_diaria_altas, 2) AS media_diaria_altas,
    total_altas_periodo,
    dias_com_registro,
    -- Ranking formal da posição do município.
    RANK() OVER (ORDER BY media_diaria_altas DESC) AS ranking_nacional,
    -- Classifica os municípios em 100 grupos (percentis).
    -- NTILE(100) = 1 significa que está no top 1% de performance.
    NTILE(100) OVER (ORDER BY media_diaria_altas DESC) AS percentil
FROM
    performance_municipios
ORDER BY
    media_diaria_altas DESC;


-- ===================================================================================
-- ANÁLISE 4: Análise de Qualidade dos Dados por Origem de Envio
-- Objetivo: Auditar a qualidade dos dados enviados por cada origem.
-- ===================================================================================

SELECT
    se.origem,
    COUNT(ro._id) AS total_de_registros,
    -- A cláusula FILTER é uma forma mais limpa e moderna de fazer agregações condicionais em Postgres.
    COUNT(ro._id) FILTER (WHERE se.validado = true) AS registros_validados,
    COUNT(ro._id) FILTER (WHERE se.excluido = true) AS registros_excluidos,
    -- Registros que não foram marcados nem como validados nem como excluídos.
    COUNT(ro._id) FILTER (WHERE se.validado = false AND se.excluido = false) AS registros_pendentes,
    -- Percentual de conformidade e não conformidade.
    ROUND((COUNT(ro._id) FILTER (WHERE se.validado = true))::decimal * 100 / COUNT(ro._id), 2) AS percentual_validado,
    ROUND((COUNT(ro._id) FILTER (WHERE se.excluido = true))::decimal * 100 / COUNT(ro._id), 2) AS percentual_excluido
FROM
    registro_ocupacao ro
JOIN
    status_envio se ON ro.id_status = se.id_status
GROUP BY
    se.origem
ORDER BY
    total_de_registros DESC;


-- ===================================================================================
-- ANÁLISE 5: Análise de Contribuição Relativa de Óbitos por Hospital
-- Objetivo: Identificar os hospitais mais críticos, não só em números absolutos,
-- mas também por sua participação percentual no total de óbitos do período.
-- ===================================================================================

WITH obitos_por_hospital AS (
    -- Etapa 1: Calcular o total de óbitos para cada hospital no período.
    SELECT
        h.cnes AS cnes_hospital, -- Alias mais preciso
        SUM(COALESCE(ro.saida_suspeita_obitos, 0) + COALESCE(ro.saida_confirmada_obitos, 0)) AS total_obitos
    FROM
        registro_ocupacao ro
    JOIN
        hospital h ON ro.id_hospital = h.id_hospital
    WHERE
        ro.data_notificacao BETWEEN '2022-01-01' AND '2022-01-31'
    GROUP BY
        h.cnes
)
-- Etapa 2: Calcular a participação percentual e rankear.
SELECT
    cnes_hospital,
    total_obitos,
    -- Calcula a participação de cada hospital no total de óbitos do período.
    ROUND(
      (total_obitos::decimal * 100) / (SELECT SUM(total_obitos) FROM obitos_por_hospital), 2
    ) AS participacao_no_total_pct,
    -- Adiciona um ranking formal para clareza.
    RANK() OVER (ORDER BY total_obitos DESC) AS ranking
FROM
    obitos_por_hospital
ORDER BY
    total_obitos DESC
LIMIT 100;


-- ===================================================================================
-- CONSIDERAÇÕES DE PERFORMANCE
-- Para garantir a execução rápida destas queries em um grande volume de dados,
-- considere criar índices (CREATE INDEX) nas seguintes colunas:
--
--   - registro_ocupacao(data_notificacao)
--   - registro_ocupacao(id_hospital)
--   - registro_ocupacao(id_local)
--   - registro_ocupacao(id_status)
--   - E nas colunas de chave primária/estrangeira das tabelas de dimensão
--     (hospital, localidade, status_envio).
-- ============================ FIM DO SCRIPT ========================================
