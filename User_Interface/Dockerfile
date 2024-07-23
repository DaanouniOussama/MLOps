FROM python:3.11-slim-bullseye

WORKDIR /UserInterface

ADD streamlit_ui/ .

RUN pip install -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "ui.py"]