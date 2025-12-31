import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json
import requests
from datetime import datetime
import logging
import sys
import math

def get_total_pages(uf="AL"):
    url = "https://revendedoresapi.anp.gov.br/v1/combustivel"
    params = {"numeropagina": 1, "uf": uf}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://revendedoresapi.anp.gov.br"
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        # Acessando o objeto que você identificou no log
        filter_info = data.get("searchPageFilter", {})
        
        # Capturando os campos exatos: 'totalPagina' ou 'totalRegistro'
        total_pages = filter_info.get("totalPagina")
        total_items = filter_info.get("totalRegistro")
        
        if total_pages:
            logging.info(f"Sucesso! Total de páginas identificado: {total_pages}")
            return int(total_pages)
        
        if total_items:
            # Se por algum motivo 'totalPagina' falhar, calculamos pelo 'totalRegistro'
            items_per_page = filter_info.get("tamanhoPagina") or len(data.get("data", [])) or 100
            calculated_pages = math.ceil(total_items / items_per_page)
            logging.info(f"Total de registros: {total_items}. Páginas calculadas: {calculated_pages}")
            return calculated_pages

        logging.warning("Não foi possível extrair o total dos campos conhecidos. Usando padrão 500.")
        return 500 
        
    except Exception as e:
        logging.error(f"Erro ao calcular total de páginas: {e}")
        return 100

class FetchANPPage(beam.DoFn):
    def process(self, page):
        import requests
        from datetime import datetime
        
        url = "https://revendedoresapi.anp.gov.br/v1/combustivel"
        params = {"numeropagina": page, "uf": "AL"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Origin": "https://revendedoresapi.anp.gov.br"
        }

        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                items = r.json().get("data", [])
                if not items: return
                for item in items:
                    yield {
                        **item,
                        "ingestion_timestamp": datetime.utcnow().isoformat(),
                        "source_page": page
                    }
        except Exception as e:
            logging.error(f"Erro na pagina {page}: {e}")

def run():
    total_pages = get_total_pages(uf="AL")

    pipeline_args = {
        "project": "fuel-data-project-482021",
        "region": "us-east1",
        "runner": "DataflowRunner",
        "job_name": f"anp-ingestion-al-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "staging_location": "gs://bk_anp_raw-dataflow-staging/staging",
        "temp_location": "gs://bk_anp_raw-dataflow-temp/temp",
        "service_account_email": "fuel-data-dev@fuel-data-project-482021.iam.gserviceaccount.com",
        "save_main_session": True,
    }

    options = PipelineOptions(flags=[], **pipeline_args)
    today_str = datetime.utcnow().date().isoformat()
    output_path = f"gs://bk_anp_raw/bronze/anp/combustivel/ingestion_date={today_str}/dados"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Gerar Paginas" >> beam.Create(range(1, total_pages + 1))
            | "Reshuffle" >> beam.Reshuffle()
            | "Fetch API" >> beam.ParDo(FetchANPPage())
            | "JSON" >> beam.Map(lambda x: json.dumps(x, ensure_ascii=False))
            | "Write" >> beam.io.WriteToText(output_path, file_name_suffix=".json")
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    print("Iniciando o processo de submissão para o Dataflow...")
    try:
        run()
        print("Job enviado! Verifique o console do Google Cloud.")
    except Exception as e:
        print(f"ERRO AO EXECUTAR: {e}")


