#!/bin/bash

# Normally we would also `set -u` to catch use of unbound variables, but our
# version of pyenv-virtualenv does not play nice with that setting. See
# https://github.com/pypa/virtualenv/issues/1029 for context.
set -eo pipefail

source ./util.sh
install_util_verify
source .env

MIXER_SPARK_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

info "Begining installation..."

if ! brew cask list adoptopenjdk8 &> /dev/null; then
    info "Installing java 8"
    warn "This command may require you to enter your laptop's login password"
    brew install --cask homebrew/cask-versions/adoptopenjdk8
else
    warn "java 8 is already installed"
fi
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

function brew_install_if_missing {
    local package=$1
    if ! brew list $package &> /dev/null; then
        info "Installing $package"
        brew install $package
    else
        warn "$package is already installed"
    fi
}

brew_install_if_missing gpg
brew_install_if_missing pyenv
brew_install_if_missing pyenv-virtualenv
brew_install_if_missing zlib
brew_install_if_missing pandoc

info "Entering $CBH_SPARK_CONFIG_DIR"
pushd $CBH_SPARK_CONFIG_DIR

function download_if_missing {
    local file=$2
    local url=$1
    if [[ ! -f $file ]]; then
        info "Downloading $file"
        curl -fSL -o $file $url 
    else
        warn "$file already downloaded"
    fi
}

if [[ ! -d $SPARK_HOME ]]; then
    MIRROR=http://mirrors.ibiblio.org/apache/spark/
    download_if_missing \
        http://mirrors.ibiblio.org/apache/spark/$CBH_SPARK_VERSION/$CBH_SPARK_PACKAGE.tgz \
        $CBH_SPARK_PACKAGE.tgz
    download_if_missing \
        https://downloads.apache.org/spark/$CBH_SPARK_VERSION/$CBH_SPARK_PACKAGE.tgz.asc \
        $CBH_SPARK_PACKAGE.tgz.asc
    download_if_missing \
        https://downloads.apache.org/spark/KEYS \
        spark.KEYS
    info "Verifying spark binaries"
    gpg --import ~/.spark/spark.KEYS
    gpg --verify ~/.spark/$CBH_SPARK_PACKAGE.tgz.asc
    info "Unpacking spark binaries"
    gunzip -c $CBH_SPARK_PACKAGE.tgz | tar xf -
else
    warn "spark $CBH_SPARK_VERSION (scala $CBH_SCALA_VERSION) already installed"
fi

SPARK_DEFAULTS_CONF_PATH=$SPARK_HOME/conf
SPARK_DEFAULTS_CONF_FILENAME=spark-defaults.conf
if [[ ! -L $SPARK_DEFAULTS_CONF_PATH/$SPARK_DEFAULTS_CONF_FILENAME ]]; then
    info "Creating a symbolic link to $MIXER_SPARK_PATH/$SPARK_DEFAULTS_CONF_FILENAME in directory $SPARK_DEFAULTS_CONF_PATH"
    ln -s $MIXER_SPARK_PATH/$SPARK_DEFAULTS_CONF_FILENAME $SPARK_DEFAULTS_CONF_PATH
else
    info "$MIXER_SPARK_PATH/$SPARK_DEFAULTS_CONF_FILENAME already symbolic linked in $SPARK_DEFAULTS_CONF_PATH/$SPARK_DEFAULTS_CONF_FILENAME."
fi

if [[ ! -d $HADOOP_HOME ]]; then
    download_if_missing \
        https://downloads.apache.org/hadoop/common/hadoop-3.2.1/$CBH_HADOOP_PACKAGE.tar.gz \
        $CBH_HADOOP_PACKAGE.tar.gz
    download_if_missing \
        https://downloads.apache.org/hadoop/common/hadoop-3.2.1/$CBH_HADOOP_PACKAGE.tar.gz.asc \
        $CBH_HADOOP_PACKAGE.tar.gz.asc
    download_if_missing \
        https://downloads.apache.org/hadoop/common/KEYS \
        hadoop.KEYS
    info "Verifying hadoop binaries"
    gpg --import ~/.spark/hadoop.KEYS
    gpg --verify ~/.spark/$CBH_HADOOP_PACKAGE.tar.gz.asc
    info "Unpacking hadoop binaries"
    tar xf $CBH_HADOOP_PACKAGE.tar.gz
else
    warn "hadoop $CBH_HADOOP_VERSION already installed"
fi

unset MIRROR

info "Leaving $CBH_SPARK_CONFIG_DIR"
popd

export LDFLAGS="-L/usr/local/opt/zlib/lib"
export CPPFLAGS="-I/usr/local/opt/zlib/include"
eval "$(pyenv init -)"
# When using the 'virtualenv-init' script's default shell detection, it detects
# the version of the shell running this script and not bash itself. Specifying
# bash directly makes sure that the 'virtualenv-init' script only uses features
# that bash supports.
eval "$(pyenv virtualenv-init - bash)"

if [[ -z $(pyenv versions | grep 3.6.5) ]]; then
    info "Installing python 3.6.5"
    pyenv install 3.6.5
else
    warn "python 3.6.5 already installed"
fi

if [[ -z $(pyenv virtualenvs | grep env-spark) ]]; then
    info "Creating 'env-spark' virtualenv"
    pyenv virtualenv 3.6.5 env-spark
else
    warn "env-spark virtualenv with python 3.6.5 already exists"
fi

info "Installing dependencies in 'env-spark' virtualenv"
VIRTUAL_ENV_DISABLE_PROMPT=1 pyenv activate env-spark
pip install --upgrade pip
pip install --upgrade setuptools
pip install -r requirements.txt
pip install -r requirements-dev.txt
pyenv deactivate

info "Installation is done!\n"

function print_shell_init_message {
        info "Run the following to auto-initialize google-cloud-sdk in your shell"

}

info "Run the following to auto-initialize pyspark dependencies in your shell"
info "(ignore this message if you've already done so)"

if [[ `shell_name` == fish ]]; then
    warn "Fish users, remove the \\\$'s :)."
fi

SHELL_CONFIG=`shell_config`
cat <<END
cat <<EOF >> $SHELL_CONFIG
export JAVA_HOME=\$(/usr/libexec/java_home -v 1.8)
export LDFLAGS="-L/usr/local/opt/zlib/lib"
export CPPFLAGS="-I/usr/local/opt/zlib/include"
if ! which pyenv &> /dev/null; then
    eval "\\\$(pyenv init -)"
fi
    eval "\\\$(pyenv virtualenv-init -)"
EOF
source $SHELL_CONFIG
END
