from fastapi import FastAPI, Request
from mainmerger import main
from utils.commons import Response


app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello, World!"}

@app.post("/merge_data_sources/")
async def merge_data_sources(request: Request):
    try:
        body = await request.json()
        dataframe_res = main(body)
        records = main(body).to_dict(orient='records')
        records_size = len(dataframe_res.index)
        response = Response(None, records_size, records_size, records)
        return {"Response": response}
    except Exception as e:
        return {"message" : str(e)}