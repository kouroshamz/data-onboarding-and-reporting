.PHONY: install test run docker-build docker-run clean

install:
pip install -r requirements.txt

test:
pytest tests/ -v --cov=app

run:
python -m app.cli run --config config.yaml

docker-build:
docker build -f docker/Dockerfile -t data-onboarding:latest .

docker-run:
docker run -v $(PWD)/config.yaml:/app/config.yaml \
           -v $(PWD)/reports:/app/reports \
           data-onboarding:latest

clean:
find . -type d -name __pycache__ -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
rm -rf build/ dist/ *.egg-info/

format:
black app/ tests/
isort app/ tests/
