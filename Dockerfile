FROM python:3.12.10-alpine
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apk add --no-cache git

COPY . ./CROUStillantData

WORKDIR /CROUStillantData

RUN uv sync --frozen

RUN crontab crontab

CMD ["crond", "-f"]
