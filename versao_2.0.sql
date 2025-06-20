-- 1. View: vw_ocupacao_diaria_detalhada (Corrigida e verificada para PostgreSQL)
-- Esta view mostra o último registro de ocupação para cada hospital e localidade por dia,
-- incluindo informações detalhadas do hospital, local e status do envio.
CREATE OR REPLACE VIEW vw_ocupacao_diaria_detalhada AS
WITH RankedOccupacao AS (
    SELECT
        ro.id_hospital,
        ro.id_local,
        ro.id_status,
        ro.atualizado_em,
        ro.ocupacao_suspeito_cli,
        ro.ocupacao_suspeito_uti,
        ro.ocupacao_confirmado_cli,
        ro.ocupacao_confirmado_uti,
        ro.ocupacao_covid_cli,
        ro.ocupacao_covid_uti,
        ro.ocupacao_hospitalar_cli,
        ro.ocupacao_hospitalar_uti,
        ro.saida_suspeita_obitos,
        ro.saida_suspeita_altas,
        ro.saida_confirmada_obitos,
        ro.saida_confirmada_altas,
        ROW_NUMBER() OVER (PARTITION BY ro.id_hospital, ro.id_local, CAST(ro.atualizado_em AS DATE) ORDER BY ro.atualizado_em DESC) as rn
    FROM
        registro_ocupacao ro
    JOIN
        status_envio se ON ro.id_status = se.id_status
    WHERE
        se.excluido = FALSE
)
SELECT
    ro.id_hospital,
    h.cnes AS hospital_cnes,
    ro.id_local,
    CASE
        WHEN LOWER(l.estado) = 'goias' THEN 'Goiás'
        WHEN LOWER(l.estado) = 'sao paulo' THEN 'São Paulo'
        WHEN LOWER(l.estado) = 'parana' THEN 'Paraná'
        WHEN LOWER(l.estado) = 'amapa' THEN 'Amapá'
        WHEN LOWER(l.estado) = 'rondonia' THEN 'Rondônia'
        WHEN LOWER(l.estado) = 'roraima' THEN 'Roraima'
        WHEN LOWER(l.estado) = 'maranhao' THEN 'Maranhão'
        WHEN LOWER(l.estado) = 'para' THEN 'Pará'
        WHEN LOWER(l.estado) = 'paraiba' THEN 'Paraíba'
        WHEN LOWER(l.estado) = 'piaui' THEN 'Piauí'
        WHEN LOWER(l.estado) = 'espirito santo' THEN 'Espírito Santo'
        WHEN LOWER(l.estado) = 'rio de janeiro' THEN 'Rio de Janeiro'
        WHEN LOWER(l.estado) = 'mato grosso' THEN 'Mato Grosso'
        WHEN LOWER(l.estado) = 'mato grosso do sul' THEN 'Mato Grosso do Sul'
        WHEN LOWER(l.estado) = 'minas gerais' THEN 'Minas Gerais'
        WHEN LOWER(l.estado) = 'ceara' THEN 'Ceará'
        WHEN LOWER(l.estado) = 'pernambuco' THEN 'Pernambuco'
        WHEN LOWER(l.estado) = 'sergipe' THEN 'Sergipe'
        WHEN LOWER(l.estado) = 'alagoas' THEN 'Alagoas'
        WHEN LOWER(l.estado) = 'bahia' THEN 'Bahia'
        WHEN LOWER(l.estado) = 'santa catarina' THEN 'Santa Catarina'
        WHEN LOWER(l.estado) = 'rio grande do sul' THEN 'Rio Grande do Sul'
        WHEN LOWER(l.estado) = 'rio grande do norte' THEN 'Rio Grande do Norte'
        WHEN LOWER(l.estado) = 'acre' THEN 'Acre'
        WHEN LOWER(l.estado) = 'amazonas' THEN 'Amazonas'
        WHEN LOWER(l.estado) = 'tocantins' THEN 'Tocantins'
        WHEN LOWER(l.estado) = 'distrito federal' THEN 'Distrito Federal'
        ELSE INITCAP(LOWER(l.estado))
    END AS local_estado, -- Alteração aqui para padronizar o nome do estado e tratar acentos
    l.municipio AS local_municipio,
    CAST(ro.atualizado_em AS DATE) AS data_atualizacao,
    CAST(ro.atualizado_em AS TIME) AS hora_atualizacao,
    ro.ocupacao_suspeito_cli,
    ro.ocupacao_suspeito_uti,
    ro.ocupacao_confirmado_cli,
    ro.ocupacao_confirmado_uti,
    ro.ocupacao_covid_cli,
    ro.ocupacao_covid_uti,
    ro.ocupacao_hospitalar_cli,
    ro.ocupacao_hospitalar_uti,
    ro.saida_suspeita_obitos,
    ro.saida_suspeita_altas,
    ro.saida_confirmada_obitos,
    ro.saida_confirmada_altas,
    se.origem AS status_origem,
    se.usuario_sistema AS status_usuario_sistema,
    se.validado AS status_validado
FROM
    RankedOccupacao ro
JOIN
    status_envio se ON ro.id_status = se.id_status
JOIN
    hospital h ON ro.id_hospital = h.id_hospital
JOIN
    localidade l ON ro.id_local = l.id_local
WHERE
    ro.rn = 1;

-- Exemplo de uso da View:
select distinct(local_estado) from vw_ocupacao_diaria_detalhada vodd 

SELECT * FROM vw_ocupacao_diaria_detalhada WHERE id_hospital = 1 ORDER BY data_atualizacao DESC, hora_atualizacao DESC;

---

-- 1. CTE: CTE_OcupacaoMediaDiaria
-- Esta CTE calcula a média diária das ocupações de leitos (clínica e UTI) para casos confirmados e suspeitos,
-- por hospital, para entender a tendência geral de ocupação.
WITH CTE_OcupacaoMediaDiaria AS (
    SELECT
        ro.id_hospital,
        h.cnes AS hospital_cnes,
        CAST(ro.atualizado_em AS DATE) AS data_registro,
        AVG(ro.ocupacao_suspeito_cli) AS avg_suspeito_cli,
        AVG(ro.ocupacao_suspeito_uti) AS avg_suspeito_uti,
        AVG(ro.ocupacao_confirmado_cli) AS avg_confirmado_cli,
        AVG(ro.ocupacao_confirmado_uti) AS avg_confirmado_uti,
        AVG(ro.ocupacao_covid_cli) AS avg_covid_cli,
        AVG(ro.ocupacao_covid_uti) AS avg_covid_uti,
        AVG(ro.ocupacao_hospitalar_cli) AS avg_hospitalar_cli,
        AVG(ro.ocupacao_hospitalar_uti) AS avg_hospitalar_uti
    FROM
        registro_ocupacao ro
    JOIN
        hospital h ON ro.id_hospital = h.id_hospital
    JOIN
        status_envio se ON ro.id_status = se.id_status
    WHERE
        se.excluido = FALSE
    GROUP BY
        ro.id_hospital,
        h.cnes,
        CAST(ro.atualizado_em AS DATE)
)
SELECT
    id_hospital,
    hospital_cnes,
    data_registro,
    avg_suspeito_cli,
    avg_suspeito_uti,
    avg_confirmado_cli,
    avg_confirmado_uti,
    avg_covid_cli,
    avg_covid_uti,
    avg_hospitalar_cli,
    avg_hospitalar_uti
FROM
    CTE_OcupacaoMediaDiaria
WHERE
    avg_confirmado_cli IS NOT NULL OR avg_suspeito_cli IS NOT NULL
ORDER BY
    data_registro, id_hospital;

---

-- 2. Subconsulta: Hospitais com Ocupação Média de UTI Acima da Média Geral
-- Esta consulta identifica hospitais que tiveram uma ocupação média de UTI (confirmados COVID)
-- em um determinado dia superior à média geral de todos os hospitais para o mesmo dia.
SELECT
    ro.id_hospital,
    h.cnes,
    CAST(ro.atualizado_em AS DATE) AS data_registro,
    AVG(ro.ocupacao_covid_uti) AS media_ocupacao_covid_uti
FROM
    registro_ocupacao ro
JOIN
    hospital h ON ro.id_hospital = h.id_hospital
JOIN
    status_envio se ON ro.id_status = se.id_status
WHERE
    se.excluido = FALSE
    AND ro.ocupacao_covid_uti IS NOT NULL
GROUP BY
    ro.id_hospital,
    h.cnes,
    CAST(ro.atualizado_em AS DATE)
HAVING
    AVG(ro.ocupacao_covid_uti) > (
        SELECT
            AVG(ro2.ocupacao_covid_uti)
        FROM
            registro_ocupacao ro2
        JOIN
            status_envio se2 ON ro2.id_status = se2.id_status
        WHERE
            se2.excluido = FALSE
            AND CAST(ro2.atualizado_em AS DATE) = CAST(ro.atualizado_em AS DATE)
            AND ro2.ocupacao_covid_uti IS NOT NULL
    )
ORDER BY
    data_registro, media_ocupacao_covid_uti DESC;

---

-- 3. Detecção de Anomalias: Registros com Picos Isolados (Ocupação de UTI Confirmada)
-- Esta consulta busca identificar "picos" ou "quedas" abruptas e isoladas na ocupação de UTI
-- para casos confirmados. Um pico é definido como um valor significativamente maior (mais que o dobro)
-- do que o valor do dia anterior e posterior. Pode ser ajustado conforme a necessidade.
WITH DailyOccupation AS (
    SELECT
        id_hospital,
        CAST(atualizado_em AS DATE) AS data_registro,
        AVG(ocupacao_confirmado_uti) AS avg_ocupacao_confirmado_uti
    FROM
        registro_ocupacao
    JOIN
        status_envio se ON registro_ocupacao.id_status = se.id_status
    WHERE
        se.excluido = FALSE
        AND ocupacao_confirmado_uti IS NOT NULL
    GROUP BY
        id_hospital,
        CAST(atualizado_em AS DATE)
),
LagLeadOccupation AS (
    SELECT
        id_hospital,
        data_registro,
        avg_ocupacao_confirmado_uti,
        LAG(avg_ocupacao_confirmado_uti, 1, 0) OVER (PARTITION BY id_hospital ORDER BY data_registro) AS prev_day_avg,
        LEAD(avg_ocupacao_confirmado_uti, 1, 0) OVER (PARTITION BY id_hospital ORDER BY data_registro) AS next_day_avg
    FROM
        DailyOccupation
)
SELECT
    llo.id_hospital,
    h.cnes,
    llo.data_registro,
    llo.avg_ocupacao_confirmado_uti AS valor_no_pico,
    llo.prev_day_avg,
    llo.next_day_avg
FROM
    LagLeadOccupation llo
JOIN
    hospital h ON llo.id_hospital = h.id_hospital
WHERE
    llo.avg_ocupacao_confirmado_uti > (llo.prev_day_avg * 2) -- Ocupação mais que o dobro do dia anterior
    AND llo.avg_ocupacao_confirmado_uti > (llo.next_day_avg * 2) -- E mais que o dobro do dia posterior
    AND llo.avg_ocupacao_confirmado_uti > 0 -- Ignorar picos de zero para algo pequeno
ORDER BY
    data_registro, id_hospital;

---

-- 4. Análise de Texto: Origem dos Dados e Padrões de Usuários
-- Esta consulta analisa a distribuição da origem dos dados (coluna 'origem' da tabela status_envio)
-- e os usuários de sistema ('usuario_sistema'), para identificar padrões ou anomalias nos envios.
-- Isso pode ajudar a entender de onde vêm a maioria dos dados e se há usuários específicos
-- com muitos ou poucos registros.
SELECT
    se.origem,
    se.usuario_sistema,
    COUNT(ro._id) AS total_registros,
    MIN(CAST(ro.data_notificacao AS DATE)) AS primeira_notificacao,
    MAX(CAST(ro.data_notificacao AS DATE)) AS ultima_notificacao,
    COUNT(DISTINCT ro.id_hospital) AS hospitais_distintos_afetados
FROM
    status_envio se
JOIN
    registro_ocupacao ro ON se.id_status = ro.id_status
WHERE
    se.excluido = FALSE
GROUP BY
    se.origem,
    se.usuario_sistema
ORDER BY
    total_registros DESC, origem, usuario_sistema;

---

-- 5. Evolução das Altas vs. Óbitos (Mensal)
-- Esta consulta calcula a proporção de altas versus óbitos para casos suspeitos e confirmados
-- agregados mensalmente, permitindo uma análise temporal da gravidade dos casos e da eficácia do tratamento.
SELECT
    TO_CHAR(ro.data_notificacao, 'YYYY-MM') AS mes_ano,
    SUM(COALESCE(ro.saida_suspeita_altas, 0)) AS total_altas_suspeitas,
    SUM(COALESCE(ro.saida_suspeita_obitos, 0)) AS total_obitos_suspeitos,
    CASE
        WHEN SUM(COALESCE(ro.saida_suspeita_obitos, 0)) = 0 THEN NULL
        ELSE SUM(COALESCE(ro.saida_suspeita_altas, 0))::NUMERIC / SUM(COALESCE(ro.saida_suspeita_obitos, 0))
    END AS proporcao_altas_obitos_suspeitos,
    SUM(COALESCE(ro.saida_confirmada_altas, 0)) AS total_altas_confirmadas,
    SUM(COALESCE(ro.saida_confirmada_obitos, 0)) AS total_obitos_confirmados,
    CASE
        WHEN SUM(COALESCE(ro.saida_confirmada_obitos, 0)) = 0 THEN NULL
        ELSE SUM(COALESCE(ro.saida_confirmada_altas, 0))::NUMERIC / SUM(COALESCE(ro.saida_confirmada_obitos, 0))
    END AS proporcao_altas_obitos_confirmados
FROM
    registro_ocupacao ro
JOIN
    status_envio se ON ro.id_status = se.id_status
WHERE
    se.excluido = FALSE
GROUP BY
    mes_ano
ORDER BY
    mes_ano;

---

-- Análise Livre: Hospitais com Maior Ocupação Total de Leitos (Clínica + UTI)
-- Esta consulta identifica os hospitais com a maior ocupação total de leitos (clínica e UTI somados,
-- para todos os tipos de casos: suspeitos, confirmados, COVID e hospitalar geral) em um determinado período,
-- fornecendo uma visão consolidada da demanda.
SELECT
    h.id_hospital,
    h.cnes,
    l.municipio,
    CAST(ro.atualizado_em AS DATE) AS data_ocupacao,
    SUM(COALESCE(ro.ocupacao_suspeito_cli, 0) + COALESCE(ro.ocupacao_suspeito_uti, 0) +
        COALESCE(ro.ocupacao_confirmado_cli, 0) + COALESCE(ro.ocupacao_confirmado_uti, 0) +
        COALESCE(ro.ocupacao_covid_cli, 0) + COALESCE(ro.ocupacao_covid_uti, 0) +
        COALESCE(ro.ocupacao_hospitalar_cli, 0) + COALESCE(ro.ocupacao_hospitalar_uti, 0)) AS ocupacao_total_leitos
FROM
    registro_ocupacao ro
JOIN
    hospital h ON ro.id_hospital = h.id_hospital
JOIN
    localidade l ON ro.id_local = l.id_local
JOIN
    status_envio se ON ro.id_status = se.id_status
WHERE
    se.excluido = FALSE
    -- Opcional: Filtrar por um período específico, por exemplo, o último mês
    -- AND ro.atualizado_em >= NOW() - INTERVAL '1 month'
GROUP BY
    h.id_hospital,
    h.cnes,
    l.municipio,
    CAST(ro.atualizado_em AS DATE)
ORDER BY
    ocupacao_total_leitos DESC, data_ocupacao DESC
LIMIT 10; -- Limita aos 10 hospitais com maior ocupação total
