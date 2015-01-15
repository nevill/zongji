# MySQL is going to be run under this user
sudo rm -rf /var/ramfs/mysql/
sudo mkdir /var/ramfs/mysql/
sudo chown $USER /var/ramfs/mysql/
# --------------------------------------------

# Download and extract MySQL binaries
wget http://dev.mysql.com/get/Downloads/MySQL-5.1/mysql-5.1.73-linux-x86_64-glibc23.tar.gz
tar -zxf mysql-5.1.73-linux-x86_64-glibc23.tar.gz
wget http://dev.mysql.com/get/Downloads/MySQL-5.5/mysql-5.5.41-linux2.6-x86_64.tar.gz
tar -zxf mysql-5.5.41-linux2.6-x86_64.tar.gz
wget http://dev.mysql.com/get/Downloads/MySQL-5.6/mysql-5.6.22-linux-glibc2.5-x86_64.tar.gz
tar -zxf mysql-5.6.22-linux-glibc2.5-x86_64.tar.gz
# --------------------------------------------

# Prepare 5.1.73
cd mysql-5.1.73-linux-x86_64-glibc23/

mkdir -p data/mysql/data/tmp
# Initialize information database
./scripts/mysql_install_db --datadir=./data/mysql --user=$USER

# Copy configuration
cp ../test/travis/my.5.1.73.cnf ./my.cnf
mkdir binlog
touch binlog/mysql-bin.index

# Start server
./bin/mysqld --defaults-file=my.cnf &
sleep 4

cd ..
# --------------------------------------------

# Prepare 5.5.41
cd mysql-5.5.41-linux2.6-x86_64/

mkdir -p data/mysql/data/tmp
# Initialize information database
./scripts/mysql_install_db --datadir=./data/mysql --user=$USER
# Duplicate initial data for use with 5.6.22
cp -P -r data/mysql ../mysql-5.6.22-linux-glibc2.5-x86_64/data

# Copy configuration
cp ../test/travis/my.5.5.41.cnf ./my.cnf
mkdir binlog
touch binlog/mysql-bin.index

# Start server
./bin/mysqld --defaults-file=my.cnf &
sleep 4

cd ..

# --------------------------------------------

# Prepare 5.6.22
cd mysql-5.6.22-linux-glibc2.5-x86_64/

mkdir -p data/mysql/data/tmp

# Copy configuration
cp ../test/travis/my.5.6.22.cnf ./my.cnf
mkdir binlog
touch binlog/mysql-bin.index

# Start server, do not display errors as there will be `mysql` schema diffs
nohup ./bin/mysqld --defaults-file=my.cnf > /dev/null 2>&1 &
sleep 4

cd ..

