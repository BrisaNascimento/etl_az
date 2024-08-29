from etl_az.utils import web_data_extractor


class Extraction():
    def __init__(self) -> None:
        ...

    @staticmethod
    def extract_erva_mate() -> list():
        '''
        List of URLs from source for Erva Mate
        area colhida - https://dados.dee.planejamento.rs.gov.br/download/dee-977.csv
        area destinada a colheita - https://dados.dee.planejamento.rs.gov.br/download/dee-1690.csv
        quantidade produzida - https://dados.dee.planejamento.rs.gov.br/download/dee-978.csv
        rendimento medio - https://dados.dee.planejamento.rs.gov.br/download/dee-979.csv
        valor da producao - https://dados.dee.planejamento.rs.gov.br/download/dee-980.csv
        '''
        urls = [
            'https://dados.dee.planejamento.rs.gov.br/download/dee-977.csv',
            'https://dados.dee.planejamento.rs.gov.br/download/dee-1690.csv',
            'https://dados.dee.planejamento.rs.gov.br/download/dee-978.csv',
            'https://dados.dee.planejamento.rs.gov.br/download/dee-979.csv',
            'https://dados.dee.planejamento.rs.gov.br/download/dee-980.csv'
        ]
        filesextracted = []

        for url in urls:
            f_extracted = web_data_extractor(url)
            filesextracted.append(f_extracted)
        print(filesextracted)
        return filesextracted
