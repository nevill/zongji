echo -e "\033[1;35m Run test on 5.1.73\033[0m"
TEST_MYSQL_PORT=3351 npm test || exit $?
echo -e "\033[1;35m Run test on 5.5.41\033[0m"
TEST_MYSQL_PORT=3355 npm test || exit $?
# echo -e "\033[1;35m Run test on 5.6.13\033[0m"
# TEST_MYSQL_PORT=3456 npm test || exit $?
echo -e "\033[1;35m Run test on 5.6.22\033[0m"
TEST_MYSQL_PORT=3356 npm test || exit $?
