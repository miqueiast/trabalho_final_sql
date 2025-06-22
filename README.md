# Análises de Dados Hospitalares - Ocupação e Desfechos

Este repositório contém consultas SQL desenvolvidas para analisar dados hospitalares relacionados à ocupação de leitos e desfechos de pacientes (altas e óbitos). As análises visam extrair insights sobre a evolução temporal, detecção de anomalias, padrões de dados e a performance de diferentes localidades.

## Estrutura do Projeto

O arquivo principal `analises_hospitalares.sql` contém todas as consultas SQL organizadas de acordo com os requisitos do projeto.

## Consultas SQL Desenvolvidas

A seguir, uma descrição detalhada de cada consulta SQL, sua lógica e os aprendizados que podem ser extraídos.

---

### 1. `vw_analises` (View Base para Análises)

Esta View foi criada como um passo inicial fundamental para facilitar todas as análises subsequentes. Ela consolida e padroniza os dados de ocupação e desfechos, limpando valores `NULL` e selecionando apenas os registros mais recentes e válidos de cada hospital por data de atualização.

* **Lógica Aplicada:**
    * Realiza um `JOIN` entre `registro_ocupacao`, `status_envio` e `localidade` para enriquecer os dados.
    * Filtra registros onde `se.excluido = FALSE` para garantir que apenas dados válidos sejam considerados.
    * Utiliza `COALESCE(campo, 0)` para substituir valores `NULL` por zero nas colunas numéricas de ocupação e saída, garantindo cálculos corretos.
    * Emprega `ROW_NUMBER()` com `PARTITION BY id_hospital, atualizado_em::date` e `ORDER BY atualizado_em DESC, _id DESC` para selecionar o registro mais recente para cada hospital em um dado dia, evitando duplicidades e garantindo a atualização dos dados.
* **Aprendizado sobre os Dados:**
    * Fornece uma base de dados limpa e pronta para consumo, eliminando a necessidade de repetir lógicas de tratamento em cada consulta.
    * Garante que as análises sejam feitas sobre os dados mais atualizados e consistentes.

---

### 2. Análise de Ocupação de UTI e Variação (CTE)

Esta consulta utiliza Common Table Expressions (CTEs) para calcular a média móvel da ocupação de leitos de UTI e a variação percentual em relação a essa média. É útil para identificar tendências e mudanças significativas na ocupação ao longo do tempo.

* **Lógica Aplicada:**
    * **`ocupacao_uti_diaria`**: Calcula a soma total de leitos de UTI ocupados por dia, agrupando as ocupações de casos suspeitos, confirmados e COVID.
    * **`media_movel_uti`**: Calcula a média móvel de 7 dias da `total_leitos_uti_dia` usando uma janela de agregação (`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`), suavizando as flutuações diárias.
    * A consulta final calcula a variação percentual do dia atual em relação à média móvel, destacando desvios.
* **Aprendizado sobre os Dados:**
    * Permite observar a **tendência** da ocupação de UTI, identificando se há um aumento ou queda sustentada.
    * A **variação percentual** ajuda a quantificar o quão acima ou abaixo da tendência a ocupação está em um determinado dia, o que pode indicar picos ou quedas abruptas.

---

### 3. Análise de Ocupação Hospitalar por Estado (CTE)

Esta CTE oferece uma visão consolidada da ocupação hospitalar, altas e óbitos por estado e por mês. É ideal para comparar a performance e a carga hospitalar entre diferentes regiões.

* **Lógica Aplicada:**
    * **`ocupacao_por_estado`**: Agrupa os dados da `vw_analises` por estado e mês (`DATE_TRUNC('month', data_notificacao)`), somando as ocupações totais (clínica + UTI), altas e óbitos.
    * A consulta final calcula a `taxa_mortalidade` por estado e mês.
* **Aprendizado sobre os Dados:**
    * Permite comparar a **carga hospitalar** e a **taxa de mortalidade** entre diferentes estados ao longo do tempo.
    * Identifica estados com maior ou menor sucesso na gestão de casos e recuperação de pacientes.

---

### 4. Análise de Municípios com Maior Taxa de Mortalidade (Subconsulta)

Esta consulta utiliza uma subconsulta para identificar os municípios com as maiores taxas de mortalidade em um período específico.

* **Lógica Aplicada:**
    * A **subconsulta** agrega os totais de óbitos e altas por município e estado para um período específico (`BETWEEN '2022-01-01' AND '2023-03-31'`), garantindo que apenas municípios com pelo menos uma alta sejam considerados.
    * A consulta externa calcula a `taxa_mortalidade` e ordena os resultados para mostrar os 20 municípios com as maiores taxas.
* **Aprendizado sobre os Dados:**
    * Ajuda a **direcionar esforços** e recursos para municípios que apresentam as maiores taxas de mortalidade, indicando possíveis problemas na infraestrutura de saúde ou no manejo de casos.
    * Permite um foco geográfico nas áreas mais críticas.

---

### 5. Detecção de Anomalias Temporais (Picos Isolados)

Esta consulta foi projetada para detectar "picos isolados" na ocupação de UTI, que podem indicar eventos incomuns ou anomalias nos dados.

* **Lógica Aplicada:**
    * **`ocupacao_diaria`**: Soma a ocupação confirmada de UTI por dia.
    * **`estatisticas`**: Calcula a média móvel e o desvio padrão da `ocupacao_uti` em uma janela de 15 dias (7 dias antes, o dia atual, e 7 dias depois).
    * A consulta final identifica anomalias comparando a `ocupacao_uti` do dia com sua `media_movel` e `desvio_padrao` através do **Z-score**. Um `ABS(Z-score) > 3` é usado como critério para anomalia, indicando que o ponto está a mais de 3 desvios padrão da média.
* **Aprendizado sobre os Dados:**
    * Permite identificar dias específicos com **aumentos ou quedas incomuns** na ocupação de UTI que merecem investigação.
    * Essas anomalias podem ser causadas por erros de dados, eventos específicos (ex: surtos localizados), ou mudanças na metodologia de registro.

---

### 6. Análise de Texto (Origem dos Dados e Padrões de Usuários)

Esta consulta oferece uma análise profunda dos campos textuais disponíveis nos dados, buscando entender a origem dos registros e padrões de preenchimento dos usuários e hospitais.

* **Lógica Aplicada:**
    * **`dados_textuais` (CTE)**: Realiza `JOINs` para consolidar todos os campos textuais relevantes de `status_envio`, `localidade` e `hospital`.
    * **Classificação de Padrões:** Utiliza `CASE` statements e expressões regulares (`~*`) para categorizar:
        * `tipo_usuario`: Se o `usuario_sistema` é um email, sigla, numérico, vazio ou outro.
        * `tipo_cnes`: Se o CNES é válido (7 dígitos), vazio ou inválido.
        * `local_vs_notificacao`: Se o local de atendimento é igual ou diferente do local de notificação.
    * A consulta final agrupa por essas características textuais e calcula métricas como `total_registros`, `estados_afetados`, `municipios_afetados`, `hospitais_afetados`, `total_obitos`, `total_altas`, `media_obitos`, `media_altas` e `taxa_mortalidade`.
* **Aprendizado sobre os Dados:**
    * Revela a **qualidade e a padronização** dos dados de entrada (ex: consistência do CNES, forma de preenchimento do usuário).
    * Ajuda a entender **de onde vêm os dados** (`origem`) e como diferentes fontes podem impactar as métricas de saúde.
    * Identifica potenciais **discrepâncias** entre o local de atendimento e o local de notificação, que podem indicar fluxos de pacientes ou problemas de registro.

---

### 7. Evolução das Altas vs. Óbitos (Análise Temporal)

Esta consulta se concentra na análise temporal da proporção entre altas e óbitos, tanto para casos suspeitos quanto confirmados, agrupados semanalmente.

* **Lógica Aplicada:**
    * **`evolucao_semanal` (CTE)**: Agrupa os dados da `vw_analises` por semana (`DATE_TRUNC('week', data_notificacao)`), somando `total_altas`, `total_obitos`, e ocupações de casos confirmados e suspeitos.
    * A consulta final calcula a `taxa_mortalidade` semanal, a `taxa_alta_ocupacao` (proporção de altas em relação à ocupação total) e a `variacao_taxa_mortalidade` em relação à semana anterior usando `LAG()`.
* **Aprendizado sobre os Dados:**
    * Permite monitorar a **evolução da gravidade** dos casos ao longo do tempo, indicando se as taxas de mortalidade estão aumentando ou diminuindo.
    * A `taxa_alta_ocupacao` pode indicar a eficiência do sistema de saúde em liberar leitos.
    * A **variação semanal** ajuda a identificar tendências de curto prazo e a reagir a mudanças rápidas no cenário da doença.

---

### 8. Análise Adicional: Comparação entre Estados (CTE)

Esta consulta adicional compara o desempenho dos estados em relação à mortalidade em UTI e sua relação com a média nacional.

* **Lógica Aplicada:**
    * **`dados_estados` (CTE)**: Agrega a ocupação de UTI e óbitos por estado e mês.
    * A consulta final calcula a `mortalidade_uti` para cada estado/mês.
    * A `razao_vs_media_nacional` compara a mortalidade de um estado com a média nacional para aquele mês, usando uma janela de agregação com `PARTITION BY mes`.
* **Aprendizado sobre os Dados:**
    * Oferece um **benchmark** para os estados, permitindo identificar quais estão com taxas de mortalidade em UTI acima ou abaixo da média.
    * Essas comparações podem subsidiar discussões sobre melhores práticas, alocação de recursos e políticas de saúde regionais.
