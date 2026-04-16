FROM apify/actor-python:3.12

COPY requirements.txt ./
RUN pip install -r requirements.txt --quiet

COPY . ./

CMD ["python", "dtc_scraper.py"]
