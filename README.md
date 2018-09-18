# μNEST Explorer Api Build Guide

Author:μNEST dev team, http://iotee.io


## Ubuntu 16.04 LTS (64-bit)

Get the explorer code:
```
cd ~
git clone https://github.com/NESTchain/explorer.git
```

## Setup pyenv
1. Get sourcecode:
```
cd ~
git clone https://github.com/yyuu/pyenv.git ~/.pyenv
```

2. Config
```
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bash_profile
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bash_profile
```

3. Add pyenv to shell
```
echo 'eval "$(pyenv init -)"' >> ~/.bash_profile
```

4. Reboot shell
```
exec $SHELL
source ~/.bash_profile
```

## Install python
Using command "pyenv  install --list", you will get valid python, we choice v3.6.5, and input command:
```
pyenv install -v 3.6.5
```

when finished, using command "pyenv versions", you will get python installed.

## Choose global python
```
pyenv global 3.6.5
```

## Setup virtualenv 

1. Get pyenv-virtualenv plugin

```
git clone https://github.com/yyuu/pyenv-virtualenv.git ~/.pyenv/plugins/pyenv-virtualenv   
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bash_profile
source ~/.bash_profile
```

2. Create virtualenv

```
pyenv virtualenv 3.6.5 env365
```

3. Active env

```
pyenv activate env365
```
If you want to exit env , input "pyenv deactivate"

## Setup mongodb(ubuntu 16.04 LTS)

Prerequisites

MongoDB .tar.gz tarballs require installing the following dependencies
```
sudo apt-get install libcurl3 openssl
```

start to install

```
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/4.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org
```

Start MongoDB

```
sudo service mongod start
```

## api access

Before start the explorer api, add api access in "genesis.json". First, Create an json file named "api-access.json", and input
```
{
   "permission_map" :
   [
      [
         "*",
         {
            "password_hash_b64" : "*",
            "password_salt_b64" : "*",
            "allowed_apis" : ["database_api", "history_api", "asset_api", "orders_api"]
         }
      ]
   ]
}  
```
put this file in the same folder with "genesis.json", and restart witness_node, and open "data/config.ini", enable "JSON file specifying API permissions", input:
```
api-access = api-access.json
```

## Install necessary python package

```
pip install pymongo, Flask
```

## start explorer api
1. Config
In the project, there is a file named "host_info.json", 
```
{
    "url": "ws://127.0.0.1:10110",  //witness_node url
    "listen_ip": "0.0.0.0",	    //Flask listen ip
    "listen_port": 5000             //Flask linsten port
}

2. start

```
python explorer_api.py
```

## Reference
https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/
https://github.com/pyenv/pyenv
https://github.com/pyenv/pyenv-virtualenv
http://docs.bitshares.org/api/access.html



