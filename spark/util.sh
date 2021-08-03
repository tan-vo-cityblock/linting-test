#!/bin/bash

NORMAL="\0033[0m"
GREEN="\0033[32m"
RED="\0033[31m"
YELLOW="\0033[33m"

function info {
    echo -e $GREEN"["$0"] [INFO] $@"$NORMAL
}

function warn {
    echo -e $YELLOW"["$0"] [WARN] $@"$NORMAL
}

function error {
    echo -e $RED"["$0"] [ERROR] $@"$NORMAL 1>&2
}

function install_util_verify {
    if [ `id -u` -eq 0 ]; then
        error "Do not run this script with sudo. Make sure `whoami` returns your username."
        exit 1
    fi

    if [ $(echo `git rev-parse --show-toplevel`/spark) != `pwd` ]; then
        error "gcloud_setup.sh must be run the 'spark' directory of the 'mixer' repository."
        exit 1
    fi

    if [[ ! -f .env ]]; then
        info "Copying sample.env to .env"
        cp sample.env .env
    else
        warn ".env already exists"
    fi

    source .env # All .env variables are prefixed with CBH_SPARK

    if [[ $CBH_SPARK_USER_EMAIL == {{email}} ]]; then
        echo -n "Enter your cityblock e-mail: "
        read EMAIL
        sed -i '' -e "s/{{email}}/$EMAIL/" .env
        source .env
    else
        EMAIL=$CBH_SPARK_USER_EMAIL
        warn "Using email $EMAIL"
    fi

    if [[ $CBH_SPARK_PROJECT_ID == {{project}} ]]; then
        echo -n "Enter your personal project id: "
        read PROJECT_ID
        sed -i '' -e "s/{{project}}/$PROJECT_ID/" .env
        source .env
    else
        PROJECT_ID=$CBH_SPARK_PROJECT_ID
        warn "Using project $PROJECT_ID"
    fi

    if [[ ! -d $CBH_SPARK_CONFIG_DIR ]]; then
        info "Creating $CBH_SPARK_CONFIG_DIR directory"
        mkdir ~/.spark
    else
        warn "$CBH_SPARK_CONFIG_DIR directory already exists"
    fi
}

function shell_name {
    echo $SHELL | rev | cut -d/ -f1 | rev
}

function shell_config {
    if [[ `shell_name` == bash ]]; then
        echo ~/.bash_profile
    elif [[ `shell_name` == zsh ]]; then
        echo ~/.zshrc
    elif [[ `shell_name` == fish ]]; then
        echo ~/.config/fish/config.fish
    else
        echo
    fi
}
