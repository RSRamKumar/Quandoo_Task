FROM python:latest

WORKDIR /quandoo_viz

COPY . /quandoo_viz

RUN pip install pandas plotly dash

CMD ["python", "./berlin_restaurants_viz.py"]
