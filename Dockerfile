FROM apify/actor-python:3.12

COPY requirements.txt ./
RUN pip install -r requirements.txt --quiet

# Install Chromium + system deps for Playwright
RUN playwright install --with-deps chromium

COPY . ./

CMD ["python", "main.py"]
