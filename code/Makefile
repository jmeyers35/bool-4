CFLAGS  =
LFLAGS  =
CC      = g++ -std=c++11
RM      = /bin/rm -rf

TESTS = test_app manager storage unit_tests experiment_tests
SRC = test_app.cpp client.cpp
UNIT_SRC = test_app.cpp client.cpp manager.cpp storage.cpp

all: $(TESTS)

manager: manager.cpp
	$(CC) -Wall -pthread manager.cpp client.cpp -o manager

storage: storage.cpp
	$(CC) -Wall -pthread storage.cpp client.cpp -o storage

test_app : manager storage $(SRC)
	$(CC) -Wall -pthread $(SRC) -o test_app

unit_tests : $(UNIT_SRC)
	$(CC) -Wall -pthread -DUNIT_TESTING -DPRINT_TRACE $(UNIT_SRC) -o unit_tests

experiment_tests : $(UNIT_SRC)
	$(CC) -Wall -pthread -DUNIT_TESTING $(UNIT_SRC) -o experiment_tests

clean:
	$(RM) *.o $(TESTS)
