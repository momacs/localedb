#!/bin/bash

url=https://raw.githubusercontent.com/momacs/localedb/master/localedb

if [ -f $HOME/bin/localedb ]; then
    echo "The file $HOME/bin/localedb already exists. The installation will not continue. To update an already installed LocaleDB command line management tool run:"
    echo "localedb update"
    exit 1
fi

case "$(uname)" in
    "Darwin") cmd="curl -fsSL $url -O -";;
    "Linux")  cmd="wget       $url -O -";;
    *) echo "Unsupported operating system." && exit 1;;
esac

mkdir -p $HOME/bin
cd $HOME/bin
$cmd
chmod a+x ./localedb

cd $HOME
! command -v localedb > /dev/null 2>&1 && echo 'export PATH=$PATH:$HOME\bin' >> .bashrc

echo "Command line interface installed. To see list of available commands, run: localedb"
