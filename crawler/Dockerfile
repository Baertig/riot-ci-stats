FROM python:3.12-slim

RUN pip install pipenv

WORKDIR /app

COPY ../Pipfile Pipfile.lock ./

RUN pipenv install --deploy --system

# crawler included in path because build context is the whole
# repo -> because all python packages are contained in the 
# one top level Pipfile
COPY crawler/fetch_stats.py .

CMD ["python", "fetch_stats.py"]