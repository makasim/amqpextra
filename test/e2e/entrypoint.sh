#!/usr/bin/env bash

FORCE_EXIT=false

function waitForService()
{
    ATTEMPTS=0
    until nc -z $1 $2; do
        printf "wait for service %s:%s\n" $1 $2
        ((ATTEMPTS++))
        if [ $ATTEMPTS -ge $3 ]; then
            printf "service is not running %s:%s\n" $1 $2
            exit 1
        fi
        if [ "$FORCE_EXIT" = true ]; then
            exit;
        fi

        sleep 1
    done

    printf "service is online %s:%s\n" $1 $2
}

trap "FORCE_EXIT=true" SIGTERM SIGINT

waitForService rabbitmq 5672 30
waitForService rabbitmq2 5672 30

exec "$@"
