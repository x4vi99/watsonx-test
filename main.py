from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import concurrent.futures
import requests
import pandas as pd
import time

app = FastAPI()

YOUR_API_KEY = "eyJraWQiOiIyMDIzMDcxMDA4MzMiLCJhbGciOiJSUzI1NiJ9.eyJpYW1faWQiOiJJQk1pZC0yNzAwMDY5R0o4IiwiaWQiOiJJQk1pZC0yNzAwMDY5R0o4IiwicmVhbG1pZCI6IklCTWlkIiwianRpIjoiNzI4NzI4MTQtMmQwNS00YjkxLWFkMWItNjQ0ZGQwYTJlZTExIiwiaWRlbnRpZmllciI6IjI3MDAwNjlHSjgiLCJnaXZlbl9uYW1lIjoiQU5HRUwiLCJmYW1pbHlfbmFtZSI6IlNFVklMTEFOTyBDQUJSRVJBIiwibmFtZSI6IkFOR0VMIFNFVklMTEFOTyBDQUJSRVJBIiwiZW1haWwiOiJhbmdlbHNldmlsbGFub0Blcy5pYm0uY29tIiwic3ViIjoiYW5nZWxzZXZpbGxhbm9AZXMuaWJtLmNvbSIsImF1dGhuIjp7InN1YiI6ImFuZ2Vsc2V2aWxsYW5vQGVzLmlibS5jb20iLCJpYW1faWQiOiJJQk1pZC0yNzAwMDY5R0o4IiwibmFtZSI6IkFOR0VMIFNFVklMTEFOTyBDQUJSRVJBIiwiZ2l2ZW5fbmFtZSI6IkFOR0VMIiwiZmFtaWx5X25hbWUiOiJTRVZJTExBTk8gQ0FCUkVSQSIsImVtYWlsIjoiYW5nZWxzZXZpbGxhbm9AZXMuaWJtLmNvbSJ9LCJhY2NvdW50Ijp7InZhbGlkIjp0cnVlLCJic3MiOiIyZTcxOGYzY2VkNWM0MjM1OWIyMjZjZTliZDM4ZjljNiIsImltc191c2VyX2lkIjoiODM0MTEwNCIsImZyb3plbiI6dHJ1ZSwiaW1zIjoiMjA2MzYyOCJ9LCJpYXQiOjE2OTE2MTkzMzYsImV4cCI6MTY5MTYyMjkzNiwiaXNzIjoiaHR0cHM6Ly9pYW0uY2xvdWQuaWJtLmNvbS9pZGVudGl0eSIsImdyYW50X3R5cGUiOiJ1cm46aWJtOnBhcmFtczpvYXV0aDpncmFudC10eXBlOmFwaWtleSIsInNjb3BlIjoiaWJtIG9wZW5pZCIsImNsaWVudF9pZCI6ImRlZmF1bHQiLCJhY3IiOjEsImFtciI6WyJwd2QiXX0.btrbUq4m_r5REbw0VFIE-AVRVXz2QtUJAh1QRbGLBZXvGjzG-K9J959sFYLXnSwM9KDrAfIoaSTIkZ9CWxCwaaFfu3yzjcIgrdlY9-yq-hOi0gILB32ObthOpkxmiJ76Va7RYKit6ZrCJXJmgrZRmUAxv6jAarPysZWo2UMezeRQgrNKvcD3a1Hc-NktuErbK8yyUc5S9e1L01gUoJYfvM6el4v46i3HLMhpaFZSSkrRrtigMM6od7EvyOAGI9TvnM5wwnjtXzIzMr3v5fliKIyxGyUQzHHK3O_-PnaeZfym66Xcbwn2I072IDhfTfGNvhU4Z5Cj4msb2bfDFV852A"
model = ["google/flan-ul2","google/flan-ul2"][1]#

class JobPostData(BaseModel):
    jobposts: list

def request_llm(url, headers, data):
    # Definimos el número máximo de intentos de reintentos
    max_retries = 2

    retry_count = 0
    while retry_count < max_retries:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            results = response.json()
            return results  # Devolvemos los resultados si la solicitud fue exitosa
        else:
            print(f"Error: {response.status_code} - {response.text}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying... Attempt {retry_count}")
                time.sleep(15)
            else:
                print("Max retries reached. Exiting.")
                return None  # Devolvemos None si se agotan los reintentos
            
def define_request(input_prompt):
    # Definimos la URL de la API
    url = 'https://us-south.ml.cloud.ibm.com/ml/v1-beta/generation/text?version=2023-05-29'

    # Definimos el encabezado con las credenciales de autenticación
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {YOUR_API_KEY}'  # Reemplaza YOUR_API_KEY con tu clave de API
    }

    # Definimos el cuerpo del mensaje en formato JSON
    data = {
        "model_id": model,
        "input": 
            f"{str(input_prompt)}"
        ,
        "parameters": {
            "decoding_method": "greedy",
            "min_new_tokens": 1,
            "max_new_tokens": 1024,
            "moderations": {
                "hap": {
                    "input": True,
                    "threshold": 0.75,
                    "output": True
                }
            }
        },
         "project_id": "c80d2ced-876e-4162-97bd-4bf4a238e45c"
    }
    return url, headers, data

def define_input_prompt(jobpost):
    input_prompt = f"""A job offer is shown below. Your objective is to identify the job and the most important requirements (years of experience, expertise, degrees, skills, languages) for the position in brief ordered. They should at most 10 requirements.

    The format to follow is:
    @job_position: job position
    @requirements:
    1 requirement 1
    2 requirement 2
    3 ...

    Job offer:
    '''
    {jobpost}
    '''
    """
    return input_prompt

def process_jobpost(jobpost, n):
    # ... El mismo código que antes ...
    start_time = time.time()
    print(f"Iteración {n + 1}")

    input_prompt = define_input_prompt(jobpost)
    #print("//////" + input_prompt + "//////")
    url, headers, data = define_request(str(input_prompt))
    results = request_llm(url, headers, data)
    #print("//////" + results['results'][0]['generated_text'] + "//////")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo transcurrido: {elapsed_time:.4f} segundos")
    print("-" * 20)
    return results['results'][0]['generated_text']

@app.post("/process_jobposts")
async def process_jobposts(jobpost_data: JobPostData):
    jobposts = jobpost_data.jobposts[:3]
    combined_results = ""

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_jobpost, jobpost, n): n for n, jobpost in enumerate(jobposts)}

        for future in concurrent.futures.as_completed(futures):
            n = futures[future]
            try:
                result = future.result()
                combined_results += result
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Iteración {n + 1} generó una excepción: {e}")

    return {"combined_results": combined_results}

@app.get("/")
def read_root():
    return {"Hello": "World"}
