--Primeira atividade a criação de uma VIEW que facilita o nosso trabalho de análises no decorrer do processo.
CREATE OR REPLACE VIEW vw_analises AS
SELECT    
	sub.id_hospital,
	sub.data_notificacao::date AS data_notificacao,
    sub.criado_em::date AS criado_em,
    sub.atualizado_em::date AS atualizado_em,
    l.estado,
    l.municipio,
    sub.ocupacao_suspeito_cli,
    sub.ocupacao_suspeito_uti,
    sub.ocupacao_confirmado_cli,
    sub.ocupacao_confirmado_uti,
    sub.ocupacao_covid_cli,
    sub.ocupacao_covid_uti,
    sub.ocupacao_hospitalar_cli,
    sub.ocupacao_hospitalar_uti,
    sub.saida_suspeita_obitos,
    sub.saida_suspeita_altas,
    sub.saida_confirmada_obitos,
    sub.saida_confirmada_altas
FROM (
    SELECT
        ro.id_hospital,
        ro.data_notificacao,
        ro.criado_em,
        ro.atualizado_em,
        ro.id_local,
        ro.id_status,
        COALESCE(ro.ocupacao_suspeito_cli, 0) AS ocupacao_suspeito_cli,
        COALESCE(ro.ocupacao_suspeito_uti, 0) AS ocupacao_suspeito_uti,
        COALESCE(ro.ocupacao_confirmado_cli, 0) AS ocupacao_confirmado_cli,
        COALESCE(ro.ocupacao_confirmado_uti, 0) AS ocupacao_confirmado_uti,
        COALESCE(ro.ocupacao_covid_cli, 0) AS ocupacao_covid_cli,
        COALESCE(ro.ocupacao_covid_uti, 0) AS ocupacao_covid_uti,
        COALESCE(ro.ocupacao_hospitalar_cli, 0) AS ocupacao_hospitalar_cli,
        COALESCE(ro.ocupacao_hospitalar_uti, 0) AS ocupacao_hospitalar_uti,
        COALESCE(ro.saida_suspeita_obitos, 0) AS saida_suspeita_obitos,
        COALESCE(ro.saida_suspeita_altas, 0) AS saida_suspeita_altas,
        COALESCE(ro.saida_confirmada_obitos, 0) AS saida_confirmada_obitos,
        COALESCE(ro.saida_confirmada_altas, 0) AS saida_confirmada_altas,
        se.excluido,
        ROW_NUMBER() OVER (
            PARTITION BY ro.id_hospital, ro.atualizado_em::date
            ORDER BY ro.atualizado_em DESC, ro._id DESC
        ) AS rn
    FROM
        registro_ocupacao ro
    JOIN 
        status_envio se ON ro.id_status = se.id_status
    WHERE
        se.excluido = FALSE
) AS sub
JOIN 
    localidade l ON sub.id_local = l.id_local
WHERE
    sub.rn = 1;


--- 1. CTEs
---
-- CTE para calcular a média móvel de 7 dias e a variação da ocupação de leitos de UTI
WITH ocupacao_uti_diaria AS (
    SELECT
        data_notificacao,
        -- Soma as ocupações de UTI para casos suspeitos, confirmados e COVID
        SUM(ocupacao_suspeito_uti + ocupacao_confirmado_uti + ocupacao_covid_uti) AS total_leitos_uti_dia
    FROM
        vw_analises
    GROUP BY
        data_notificacao
),
media_movel_uti AS (
    SELECT
        data_notificacao,
        total_leitos_uti_dia,
        -- Calcula a média móvel de 7 dias da ocupação total de leitos de UTI
        ROUND(AVG(total_leitos_uti_dia) OVER (ORDER BY data_notificacao ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS media_movel_7d_uti
    FROM
        ocupacao_uti_diaria
)
SELECT
    data_notificacao,
    total_leitos_uti_dia,
    media_movel_7d_uti,
    -- Calcula a variação percentual do dia atual em relação à média móvel
    ROUND(((total_leitos_uti_dia - media_movel_7d_uti) * 100.0 / NULLIF(media_movel_7d_uti, 0)), 2) AS variacao_pct_vs_media_movel
FROM
    media_movel_uti
ORDER BY
    data_notificacao;

-- CTE para análise de ocupação hospitalar por estado
WITH ocupacao_por_estado AS (
    SELECT
        estado,
        DATE_TRUNC('month', data_notificacao) AS mes,
        EXTRACT(YEAR FROM data_notificacao) AS ano,
        EXTRACT(MONTH FROM data_notificacao) AS mes_numero,
        SUM(ocupacao_hospitalar_cli + ocupacao_hospitalar_uti) AS total_ocupacao,
        SUM(saida_confirmada_altas + saida_suspeita_altas) AS total_altas,
        SUM(saida_confirmada_obitos + saida_suspeita_obitos) AS total_obitos
    FROM
        vw_analises
    GROUP BY
        estado, DATE_TRUNC('month', data_notificacao), ano, mes_numero
)
SELECT
    estado,
    ano,
    CASE mes_numero
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS mes,
    total_ocupacao,
    total_altas,
    total_obitos,
    ROUND((total_obitos::DECIMAL / NULLIF(total_altas + total_obitos, 0)) * 100, 2) AS taxa_mortalidade
FROM
    ocupacao_por_estado
ORDER BY
    estado, ano, mes_numero;

-- 2. Subconsulta (Análise de Performance de Municípios com Subconsulta)
-- Análise de municípios com maior taxa de mortalidade (usando subconsulta)
SELECT 
    municipio,
    estado,
    total_obitos,
    total_altas,
    ROUND((total_obitos::DECIMAL / NULLIF(total_altas + total_obitos, 0)) * 100, 2) AS taxa_mortalidade
FROM (
    SELECT
        municipio,
        estado,
        SUM(saida_confirmada_obitos + saida_suspeita_obitos) AS total_obitos,
        SUM(saida_confirmada_altas + saida_suspeita_altas) AS total_altas
    FROM
        vw_analises
    WHERE
        data_notificacao BETWEEN '2022-01-01' AND '2023-03-31'
    GROUP BY
        municipio, estado
    HAVING
        SUM(saida_confirmada_altas + saida_suspeita_altas) > 0
) AS subquery
ORDER BY
    taxa_mortalidade DESC;

-- 3. Detecção de Anomalias Temporais (Picos Isolados)
-- Detecção de anomalias (picos isolados) na ocupação de UTI
WITH ocupacao_diaria AS (
    SELECT
        data_notificacao AS dia,
        SUM(ocupacao_confirmado_uti) AS ocupacao_uti
    FROM
        vw_analises
    GROUP BY
        data_notificacao
),
estatisticas AS (
    SELECT
        dia,
        ocupacao_uti,
        ROUND(AVG(ocupacao_uti) OVER (ORDER BY dia ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING), 2) AS media_movel,
        ROUND(STDDEV(ocupacao_uti) OVER (ORDER BY dia ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING), 2) AS desvio_padrao
    FROM
        ocupacao_diaria
)
SELECT
    dia,
    ocupacao_uti,
    media_movel,
    desvio_padrao,
    ROUND((ocupacao_uti - media_movel), 2) AS diferenca_media,
    ROUND(((ocupacao_uti - media_movel) / NULLIF(desvio_padrao, 0)), 2) AS z_score
FROM
    estatisticas
WHERE
    ABS((ocupacao_uti - media_movel) / NULLIF(desvio_padrao, 0)) > 3  -- Consideramos anomalia quando Z-score > 3
ORDER BY
    ABS((ocupacao_uti - media_movel) / NULLIF(desvio_padrao, 0)) DESC;


-- 4. Análise de Texto (Origem dos Dados)
-- Análise de texto aprimorada com todos os campos textuais relevantes
WITH dados_textuais AS (
    SELECT
        -- Dados de origem/envio
        se.origem,
        se.usuario_sistema,        
        -- Dados de localidade
        l.estado,
        l.municipio,
        l.estado_notificacao,
        l.municipio_notificacao,        
        -- Dados do hospital
        h.cnes,        
        -- Dados de ocupação (para métricas)
        ro.saida_confirmada_obitos,
        ro.saida_suspeita_obitos,
        ro.saida_confirmada_altas,
        ro.saida_suspeita_altas,        
        -- Classificação do tipo de usuário
        CASE 
            WHEN se.usuario_sistema ~* '[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}' THEN 'Email'
            WHEN se.usuario_sistema ~* '^[A-Z]{2,}$' THEN 'Sigla'
            WHEN se.usuario_sistema ~* '^[0-9]+$' THEN 'Numérico'
            WHEN se.usuario_sistema IS NULL OR se.usuario_sistema = '' THEN 'Vazio'
            ELSE 'Outro'
        END AS tipo_usuario,        
        -- Padrão do CNES
        CASE
            WHEN h.cnes ~ '^[0-9]{7}$' THEN 'CNES Válido'
            WHEN h.cnes IS NULL OR h.cnes = '' THEN 'CNES Vazio'
            ELSE 'CNES Inválido'
        END AS tipo_cnes,        
        -- Verifica se há diferença entre local de atendimento e notificação
        CASE
            WHEN l.estado = l.estado_notificacao AND l.municipio = l.municipio_notificacao THEN 'Igual'
            WHEN l.estado_notificacao IS NULL OR l.municipio_notificacao IS NULL THEN 'Não informado'
            ELSE 'Diferente'
        END AS local_vs_notificacao
    FROM
        registro_ocupacao ro
    JOIN
        status_envio se ON ro.id_status = se.id_status
    JOIN
        localidade l ON ro.id_local = l.id_local
    JOIN
        hospital h ON ro.id_hospital = h.id_hospital
)
SELECT
    -- Agrupamento por características textuais
    origem,
    tipo_usuario,
    tipo_cnes,
    local_vs_notificacao,    
    -- Contagens
    COUNT(*) AS total_registros,
    COUNT(DISTINCT estado) AS estados_afetados,
    COUNT(DISTINCT municipio) AS municipios_afetados,
    COUNT(DISTINCT cnes) AS hospitais_afetados,    
    -- Métricas de saúde
    SUM(COALESCE(saida_confirmada_obitos, 0)) + SUM(COALESCE(saida_suspeita_obitos, 0)) AS total_obitos,
    SUM(COALESCE(saida_confirmada_altas, 0)) + SUM(COALESCE(saida_suspeita_altas, 0)) AS total_altas,    
    -- Médias
    ROUND(AVG(COALESCE(saida_confirmada_obitos, 0) + COALESCE(saida_suspeita_obitos, 0)), 2) AS media_obitos,
    ROUND(AVG(COALESCE(saida_confirmada_altas, 0) + COALESCE(saida_suspeita_altas, 0)), 2) AS media_altas,    
    -- Proporções
    ROUND(
        (SUM(COALESCE(saida_confirmada_obitos, 0)) + SUM(COALESCE(saida_suspeita_obitos, 0)))::DECIMAL / 
        NULLIF(SUM(COALESCE(saida_confirmada_altas, 0)) + SUM(COALESCE(saida_suspeita_altas, 0)) + 
               SUM(COALESCE(saida_confirmada_obitos, 0)) + SUM(COALESCE(saida_suspeita_obitos, 0)), 0) * 100, 
    2) AS taxa_mortalidade
FROM
    dados_textuais
GROUP BY
    origem,
    tipo_usuario,
    tipo_cnes,
    local_vs_notificacao
ORDER BY
    total_registros DESC;
 

-- 5. Evolução das Altas vs. Óbitos (Análise Temporal)
-- Evolução temporal da proporção entre altas e óbitos
WITH evolucao_semanal AS (
    SELECT
        DATE_TRUNC('week', data_notificacao) AS semana,
        SUM(saida_confirmada_altas + saida_suspeita_altas) AS total_altas,
        SUM(saida_confirmada_obitos + saida_suspeita_obitos) AS total_obitos,
        SUM(ocupacao_confirmado_cli + ocupacao_confirmado_uti) AS ocupacao_confirmados,
        SUM(ocupacao_suspeito_cli + ocupacao_suspeito_uti) AS ocupacao_suspeitos
    FROM
        vw_analises
    GROUP BY
        DATE_TRUNC('week', data_notificacao)
)
SELECT
    semana,
    total_altas,
    total_obitos,
    ocupacao_confirmados,
    ocupacao_suspeitos,
    ROUND((total_obitos::DECIMAL / NULLIF(total_altas + total_obitos, 0)) * 100, 2) AS taxa_mortalidade,
    ROUND((total_altas::DECIMAL / NULLIF(ocupacao_confirmados + ocupacao_suspeitos, 0)) * 100, 2) AS taxa_alta_ocupacao,
    -- Diferença em relação à semana anterior
    ROUND((
        (total_obitos::DECIMAL / NULLIF(total_altas + total_obitos, 0)) - 
        LAG(total_obitos::DECIMAL / NULLIF(total_altas + total_obitos, 0)) OVER (ORDER BY semana)
    ) * 100, 2) AS variacao_taxa_mortalidade
FROM
    evolucao_semanal
ORDER BY
    semana;

-- Análise Adicional: Comparação entre Estados
-- Comparação entre estados usando a view
WITH dados_estados AS (
    SELECT
        estado,
        DATE_TRUNC('month', data_notificacao) AS mes,
        SUM(ocupacao_confirmado_uti) AS uti_confirmados,
        SUM(ocupacao_suspeito_uti) AS uti_suspeitos,
        SUM(saida_confirmada_obitos) AS obitos_confirmados,
        SUM(saida_suspeita_obitos) AS obitos_suspeitos
    FROM
        vw_analises
    GROUP BY
        estado, DATE_TRUNC('month', data_notificacao)  -- Corrigi para DATE_TRUNC aqui
)
SELECT
    estado,
    TO_CHAR(mes, 'YYYY-MM') AS ano_mes,  -- Formata como '2022-01'
    uti_confirmados,
    uti_suspeitos,
    obitos_confirmados,
    obitos_suspeitos,
    ROUND((obitos_confirmados + obitos_suspeitos)::DECIMAL / NULLIF(uti_confirmados + uti_suspeitos, 0) * 100, 2) AS mortalidade_uti,
    ROUND(
        ((obitos_confirmados + obitos_suspeitos)::DECIMAL / NULLIF(uti_confirmados + uti_suspeitos, 0)) / 
        NULLIF(
            AVG((obitos_confirmados + obitos_suspeitos)::DECIMAL / NULLIF(uti_confirmados + uti_suspeitos, 0)) OVER (PARTITION BY mes),
            0
        ),
    2) AS razao_vs_media_nacional
FROM
    dados_estados
ORDER BY
    mes, mortalidade_uti DESC;
