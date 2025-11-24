import json
import pandas as pd
from sqlalchemy import create_engine, text

JSON_FILE = 'dataset.json'
DB_FILE = 'lottery_data.db'
DATABASE_URL = f'sqlite:///{DB_FILE}'

def extract_data(file_path: str) -> list:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def transform_data(data: list) -> dict:
    all_sorteios = []
    all_dezenas = []
    all_premiacoes = []

    for record in data:
        concurso = record.get('concurso')
        loteria = record.get('loteria')

        # sorteios
        sorteio_data = {
            'concurso': concurso,
            'loteria': loteria,
            'data': record.get('data'),
            'local': record.get('local'),
            'observacao': record.get('observacao'),
            'acumulou': record.get('acumulou'),
            'proximo_concurso': record.get('proximoConcurso'),
            'data_proximo_concurso': record.get('dataProximoConcurso')
        }
        all_sorteios.append(sorteio_data)

        # dezenas _sorteadas
        dezenas = record.get('dezenas')
        if dezenas:
            for dezena in dezenas:
                all_dezenas.append({
                    'concurso': concurso,
                    'loteria': loteria,
                    'dezena': dezena
                })

        # premiacoes
        premiacoes = record.get('premiacoes')
        if premiacoes:
            for premio in premiacoes:
                all_premiacoes.append({
                    'concurso': concurso,
                    'loteria': loteria,
                    'descricao': premio.get('descricao'),
                    'faixa': premio.get('faixa'),
                    'ganhadores': premio.get('ganhadores'),
                    'valor_premio': premio.get('valorPremio')
                })

    # DataFrames
    df_sorteios = pd.DataFrame(all_sorteios)
    df_dezenas = pd.DataFrame(all_dezenas)
    df_premiacoes = pd.DataFrame(all_premiacoes)

    # padronização
    df_sorteios['data'] = pd.to_datetime(df_sorteios['data'], format='mixed', dayfirst=True)
    df_sorteios['data_proximo_concurso'] = pd.to_datetime(df_sorteios['data_proximo_concurso'], format='mixed', dayfirst=True)

    return {
        'sorteios': df_sorteios,
        'dezenas_sorteadas': df_dezenas,
        'premiacoes': df_premiacoes
    }

def load_data(dataframes: dict, db_url: str):
    engine = create_engine(db_url)

    with engine.connect() as connection:
        dataframes['sorteios'].to_sql('sorteios', connection, if_exists='replace', index=False)
        dataframes['dezenas_sorteadas'].to_sql('dezenas_sorteadas', connection, if_exists='replace', index=False)
        dataframes['premiacoes'].to_sql('premiacoes', connection, if_exists='replace', index=False)

        connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_sorteios_concurso_loteria ON sorteios (concurso, loteria);"))

        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_dezenas_sorteadas_concurso_loteria ON dezenas_sorteadas (concurso, loteria);"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_dezenas_sorteadas_dezena ON dezenas_sorteadas (dezena);"))

        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_premiacoes_concurso_loteria ON premiacoes (concurso, loteria);"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_premiacoes_descricao ON premiacoes (descricao);"))

        connection.commit()
    print(f"Dados carregados com sucesso em {db_url}")

def main():
    print("Iniciando pipeline ETL...")
    raw_data = extract_data(JSON_FILE)
    print("Dados extraídos.")

    transformed_dfs = transform_data(raw_data)
    print("Dados transformados.")

    load_data(transformed_dfs, DATABASE_URL)
    print("Pipeline ETL concluído.")

if __name__ == "__main__":
    main()
