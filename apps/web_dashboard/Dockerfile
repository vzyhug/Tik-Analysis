FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY apps/web_dashboard/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN addgroup --system --gid 10001 app && adduser --system --uid 10001 --gid 10001 app
COPY --chown=10001:10001 apps/web_dashboard/ ./
USER 10001:10001

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
