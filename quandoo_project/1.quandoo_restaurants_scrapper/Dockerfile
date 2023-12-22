FROM python:latest

WORKDIR /quandoo_webscraper_dir

COPY . /quandoo_webscraper_dir

RUN pip install pandas requests beautifulsoup4

CMD ["python", "./quandoo_webscraper_app.py"]

