FROM python:latest

WORKDIR /quandoo_webscraper

VOLUME ["/quandoo_webscraper"]

COPY . /quandoo_webscraper

RUN pip install pandas requests beautifulsoup4

CMD ["python", "./quandoo_webscraper_app.py"]

