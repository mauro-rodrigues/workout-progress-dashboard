# copy the dbt profiles from your home directory to the container dbt folder
copy-dbt-profiles: 
	cp ~/.dbt/profiles.yml etls/workout_journal_dbt/

# stop and remove the containers and volumes (with image removal)
local-down: 
	docker compose down --rmi all --volumes

# build and run the container locally without cache
local-up: copy-dbt-profiles
	docker compose build --no-cache
	docker compose up --force-recreate -d > /dev/null 2>&1

# build and run the container locally without cache, with logs
local-up-with-logs: copy-dbt-profiles
	docker compose build --no-cache
	docker compose up --force-recreate

# stops the container without removing it, preserving images and volumes
local-stop:
	docker compose stop

# starts the container and ensures environment variables are reapplied without rebuilding the image
local-start:
	docker compose up -d --force-recreate

# restart the container by bringing it down and then up
local-restart: local-down local-up

# to enter the container
local-exec:
	docker exec -ti workout-progress-analysis bash

# run the entire restart and execute process
local-run: local-restart local-exec
