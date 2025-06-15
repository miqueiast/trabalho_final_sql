 ===================================================================================
 SCRIPT DE ANÁLISES AVANÇADAS DE DADOS DE OCUPAÇÃO HOSPITALAR
 Autor: Miqueias

 -
 DESCRIÇÃO DAS ANÁLISES
 Este bloco de comentários serve como README para as consultas abaixo.
 Cada seção descreve o objetivo, funcionamento e insights de uma análise,
 referenciada pelo seu número.
 -
 ===================================================================================


 ===================================================================================
 ANÁLISE 1: Média Móvel e Volatilidade da Ocupação de UTI
 ===================================================================================

 Objetivo:
   Suavizar as flutuações diárias na ocupação de UTI para identificar a tendência
   real e medir a volatilidade de cada dia em relação a essa tendência.

 Como Funciona:
   1. Uma CTE (Common Table Expression) agrega a ocupação total de UTI por dia.
   2. A consulta final usa uma função de janela para calcular a média móvel de 7 dias.
   3. Em seguida, calcula a variação percentual do dia atual em relação à média.

 Métricas e Insights:
   - media_movel_7d_uti: Revela a tendência de crescimento, estabilidade ou queda.
   - variacao_vs_media_movel_pct: Identifica picos ou quedas anormais que
     merecem investigação.


 ===================================================================================
 ANÁLISE 2: Variação e Aceleração de Óbitos
 ===================================================================================

 Objetivo:
   Medir a variação diária de óbitos e, mais importante, analisar se essa
   variação está acelerando ou desacelerando, servindo como um indicador preditivo.

 Como Funciona:
   1. A primeira CTE calcula o total de óbitos por dia.
   2. A segunda CTE usa a função LAG() para obter o dado do dia anterior e calcular
      a variação percentual diária.
   3. A consulta final calcula uma média móvel sobre essa variação percentual.

 Métricas e Insights:
   - media_movel_7d_variacao_pct: É o "termômetro da crise". Se o valor é positivo
     e crescente, a situação está piorando em ritmo acelerado. Se é negativo
     e decrescente, a melhora está se intensificando.


 ===================================================================================
 ANÁLISE 3: Análise de Performance de Municípios com Percentil
 ===================================================================================

 Objetivo:
   Rankear os municípios pela média de altas médicas e classificá-los em grupos
   (percentis) para entender sua performance relativa no cenário nacional.

 Como Funciona:
   1. Uma CTE calcula as métricas de performance (média e total de altas) por município.
   2. A consulta final aplica as funções de janela RANK() e NTILE(100).

 Métricas e Insights:
   - ranking_nacional: A posição absoluta do município, ideal para "Top 10s".
   - percentil: A posição relativa. Um município no percentil 1 está no 1% com
     melhor performance. Excelente para benchmarking e identificação de outliers.


 ===================================================================================
 ANÁLISE 4: Análise de Qualidade dos Dados por Origem de Envio
 ===================================================================================

 Objetivo:
   Auditar a conformidade e a qualidade dos dados, segmentando pela origem do
   envio (ex: 'API', 'Formulário Web').

 Como Funciona:
   - A consulta agrupa os registros por origem e usa a cláusula FILTER do Postgres
     para fazer contagens condicionais de registros validados, excluídos e pendentes.

 Métricas e Insights:
   - percentual_validado, percentual_excluido: Métricas diretas de qualidade que
     respondem qual sistema de entrada de dados é mais confiável e gera menos erros.


 ===================================================================================
 ANÁLISE 5: Análise de Contribuição Relativa de Óbitos por Hospital
 ===================================================================================

 Objetivo:
   Identificar hospitais críticos não pelo número absoluto de óbitos, mas por sua
   participação percentual no total, dando um contexto mais preciso para ação.

 Como Funciona:
   1. Uma CTE calcula o total de óbitos por hospital em um período.
   2. A consulta final divide o total de cada hospital pelo total geral (calculado
      em uma subconsulta) para obter a participação percentual.

 Métricas e Insights:
   - participacao_no_total_pct: Revela o peso real de um hospital na crise. Um
     hospital pode ser responsável por uma fatia desproporcional dos óbitos,
     mesmo sem estar no topo do ranking absoluto.


 ===================================================================================
 ANÁLISE 6: Análise de Latência de Atualização (Notification Lag)
 ===================================================================================

 Objetivo:
   Medir a eficiência do pipeline de dados, analisando o tempo entre o evento
   (data_notificacao) e sua última atualização no sistema (atualizado_em).

 Como Funciona:
   1. A CTE calcula a diferença de tempo (INTERVAL) para cada registro e extrai a hora.
   2. A consulta final agrega os dados por origem e hora, calculando métricas
      estatísticas robustas sobre essa latência.

 Métricas e Insights:
   - mediana_de_lag: Mostra o tempo de atraso "típico", sendo mais resistente a
     outliers que a média.
   - p95_lag: Indica o tempo de atraso para 95% dos registros. É um ótimo SLA
     (Acordo de Nível de Serviço) para entender o "pior cenário" realista.
   - Permite identificar gargalos por origem de dados ou por hora do dia.


 ===================================================================================
 CONSIDERAÇÕES DE PERFORMANCE
 ===================================================================================

 Para garantir a execução rápida destas queries, considere criar ÍNDICES nas
 colunas usadas em JOINs, WHERE e ORDER BY, como:

   - registro_ocupacao(data_notificacao)
   - registro_ocupacao(id_hospital)
   - registro_ocupacao(id_local)
   - registro_ocupacao(id_status)

 ============================ FIM DAS EXPLICAÇÕES ==================================
