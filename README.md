# Desafio Data Engineer Pleno - Pier

Este repositório contém a solução para o desafio de Data Engineer Pleno da Pier. O objetivo é construir um pipeline ETL (Extração, Transformação e Carga) para processar dados de sorteios da Loteria Federal, armazenados em um arquivo JSON, e carregá-los em um banco de dados SQLite.

## Sumário
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Como Executar](#como-executar)
- [Dependências](#dependencias)
- [Design do Projeto e Decisões de Modelagem](#design-do-projeto-e-decisoes-de-modelagem)
- [Documentação do Processo ETL](#documentacao-do-processo-etl)

## Estrutura do Projeto

```
.
├── dataset.json
├── etl_pipeline.py
├── README.md
├── requirements.txt
└── lottery_data.db
```

## Como Executar

Para executar o pipeline ETL, siga os passos abaixo:

1. Clone este repositório:
   ```bash
   git clone <URL_DO_SEU_REPOSITORIO>
   cd case-pierdigital
   ```

2. Crie e ative um ambiente virtual (recomendado):
   ```bash
   python -m venv venv
   # No Windows
   .\venv\Scripts\activate
   # No macOS/Linux
   source venv/bin/activate
   ```

3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

4. Execute o pipeline ETL:
   ```bash
   python etl_pipeline.py
   ```

   Após a execução, um arquivo `lottery_data.db` será criado na raiz do projeto, contendo os dados transformados.

## Dependências

As dependências do projeto estão listadas no arquivo `requirements.txt`.

```
# requirements.txt
pandas
sqlalchemy
```

## Design do Projeto e Decisões de Modelagem

A modelagem do banco de dados SQLite foi projetada para ser clara, normalizada e facilitar a análise dos dados de sorteios da Loteria Federal. Foram criadas três tabelas principais:

1.  **`sorteios`**: Contém as informações gerais de cada sorteio.
    -   `concurso` (INTEGER): Número do concurso. Chave primária composta com `loteria`.
    -   `loteria` (TEXT): Nome da loteria (ex: 'megasena'). Chave primária composta com `concurso`.
    -   `data` (DATETIME): Data do sorteio.
    -   `local` (TEXT): Local onde o sorteio foi realizado.
    -   `observacao` (TEXT): Observações sobre o sorteio.
    -   `acumulou` (BOOLEAN): Indica se o prêmio acumulou.
    -   `proximo_concurso` (INTEGER): Número do próximo concurso.
    -   `data_proximo_concurso` (DATETIME): Data do próximo concurso.

2.  **`dezenas_sorteadas`**: Armazena as dezenas sorteadas para cada concurso, permitindo múltiplos números por sorteio.
    -   `id` (INTEGER): Chave primária auto-incrementável.
    -   `concurso` (INTEGER): Referência ao `concurso` na tabela `sorteios`.
    -   `loteria` (TEXT): Referência à `loteria` na tabela `sorteios`.
    -   `dezena` (TEXT): A dezena sorteada (mantido como TEXT para preservar zeros à esquerda).

3.  **`premiacoes`**: Detalha as diferentes faixas de premiação para cada sorteio.
    -   `id` (INTEGER): Chave primária auto-incrementável.
    -   `concurso` (INTEGER): Referência ao `concurso` na tabela `sorteios`.
    -   `loteria` (TEXT): Referência à `loteria` na tabela `sorteios`.
    -   `descricao` (TEXT): Descrição da faixa de premiação (ex: '6 acertos').
    -   `faixa` (INTEGER): Número da faixa de premiação.
    -   `ganhadores` (INTEGER): Número de ganhadores nesta faixa.
    -   `valor_premio` (REAL): Valor do prêmio para esta faixa.

### Justificativas das Decisões de Modelagem:

-   **Normalização**: As informações foram divididas em tabelas separadas (`sorteios`, `dezenas_sorteadas`, `premiacoes`) para evitar redundância e melhorar a integridade dos dados. Isso facilita a manutenção e garante que cada dado esteja armazenado em apenas um local, minimizando anomalias.
-   **Chaves Primárias e Estrangeiras**: A combinação `(concurso, loteria)` atua como uma chave primária composta lógica para a tabela `sorteios`, garantindo a unicidade de cada sorteio. As tabelas `dezenas_sorteadas` e `premiacoes` referenciam essa chave composta, estabelecendo relacionamentos claros entre as entidades. Embora o SQLite não suporte `ALTER TABLE ADD PRIMARY KEY` ou `ADD FOREIGN KEY` de forma simples após a criação via `to_sql`, foram criados índices únicos e comuns para otimizar as consultas e simular os relacionamentos.
-   **Tipos de Dados**: Foram escolhidos tipos de dados adequados para cada campo (INTEGER, TEXT, DATETIME, BOOLEAN, REAL) para otimizar o armazenamento e o desempenho das consultas. Dezenas foram mantidas como TEXT para preservar formatos como "02".

## Documentação do Processo ETL

O pipeline ETL é implementado no script `etl_pipeline.py` e consiste em três fases principais:

### 1. Extração

A fase de extração é responsável por ler os dados brutos do arquivo `dataset.json`. O arquivo é carregado integralmente na memória usando a biblioteca `json` do Python. Esta abordagem é adequada para arquivos de tamanho moderado, garantindo acesso rápido a todos os registros.

### 2. Transformação

Na fase de transformação, os dados brutos são processados para limpar, padronizar e estruturar as informações em DataFrames do Pandas, que correspondem às tabelas do banco de dados SQLite. As principais transformações realizadas incluem:

-   **Estruturação dos Dados**: Os dados aninhados (como `dezenas` e `premiacoes`) são "achatados" e organizados em DataFrames separados, prontos para serem carregados em suas respectivas tabelas (`dezenas_sorteadas` e `premiacoes`).
-   **Tratamento de Valores Ausentes e Inconsistentes**: Valores ausentes (`null` ou não presentes) são tratados implicitamente pelo `get()` ao extrair os dados, resultando em `None` ou valores padrão. O Pandas e o SQLAlchemy lidam com `None` de forma adequada ao carregar para o SQLite.
-   **Conversão e Padronização de Tipos de Dados**: As colunas de data (`data` e `data_proximo_concurso`) são convertidas para o tipo `datetime` usando `pd.to_datetime`. Foi utilizado `format='mixed'` e `dayfirst=True` para lidar com a inconsistência de formatos de data presentes no arquivo JSON (ex: 'DD/MM/YYYY' e 'YYYY-MM-DD').
-   **Criação de Colunas Derivadas/Chaves Compostas**: Para facilitar os relacionamentos entre as tabelas e garantir a integridade dos dados, as colunas `concurso` e `loteria` são passadas para as tabelas `dezenas_sorteadas` e `premiacoes`, atuando como chaves estrangeiras lógicas que se referem à chave composta da tabela `sorteios`.
-   **Padronização de Nomes de Colunas**: Os nomes das colunas foram padronizados para `snake_case` (ex: `proximoConcurso` para `proximo_concurso`, `valorPremio` para `valor_premio`) para maior consistência e legibilidade no banco de dados.

### 3. Carregamento

A fase de carregamento é responsável por persistir os DataFrames transformados no banco de dados SQLite. As seguintes etapas são realizadas:

-   **Conexão com o Banco de Dados**: É estabelecida uma conexão com o banco de dados `lottery_data.db` usando `SQLAlchemy`.
-   **Carga dos DataFrames**: Os DataFrames são carregados nas tabelas correspondentes (`sorteios`, `dezenas_sorteadas`, `premiacoes`) usando o método `to_sql()` do Pandas. `if_exists='replace'` é utilizado para garantir que o banco de dados seja recriado a cada execução do pipeline, facilitando o desenvolvimento e a reprodutibilidade. `index=False` evita que o índice do DataFrame seja escrito como uma coluna no banco de dados.
-   **Criação de Índices**: Índices são criados nas colunas `(concurso, loteria)` na tabela `sorteios`, e nas colunas de referência `(concurso, loteria)` nas tabelas `dezenas_sorteadas` e `premiacoes`. Isso otimiza a performance das consultas, especialmente aquelas que envolvem junções entre as tabelas. Um índice único é criado na tabela `sorteios` para a chave composta `(concurso, loteria)` para garantir a unicidade de cada registro de sorteio.
-   **Tratamento de Chaves Primárias e Estrangeiras**: Devido às limitações do SQLite para `ALTER TABLE ADD PRIMARY KEY` e `ADD FOREIGN KEY` diretamente após a criação da tabela via `to_sql`, a unicidade é garantida pelo índice único na tabela `sorteios`. Os relacionamentos entre as tabelas são inferidos pelas colunas `concurso` e `loteria` nas tabelas `dezenas_sorteadas` e `premiacoes`, que são indexadas para facilitar as junções.
