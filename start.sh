#!/bin/bash

# Docker Compose 파일 경로 설정
COMPOSE_FILE=docker/compose/docker-compose.yml

# 사용자가 입력한 모든 인자를 docker-compose 명령어에 전달
docker-compose -f $COMPOSE_FILE "$@"