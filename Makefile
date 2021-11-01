all: docker

docker: stop env
	@ echo "Starting app in docker..."
	@ docker-compose build --no-cache && docker-compose up -d

env:
	@ echo "Check .env file exist..."
	@ ([ -f .env ] && echo ".env file found") || (echo "Make sure the .env file exists" && exit 1)
	@ echo "Sourcing .env..."
	@ . ./.env && echo "ok"

stop:
	@ echo "Stopping app..."
	@ docker-compose down -v --remove-orphans # --rmi=all